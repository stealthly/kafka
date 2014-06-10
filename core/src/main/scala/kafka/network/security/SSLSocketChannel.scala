/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network.security

import java.io.IOException
import java.net._
import java.nio.ByteBuffer
import java.nio.channels._
import javax.net.ssl._
import javax.net.ssl.SSLEngineResult._
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import kafka.utils.Logging
import java.util

object SSLSocketChannel {

  def makeSecureClientConnection(sch: SocketChannel, host: String, port: Int) = {
    val engine = SecureAuth.sslContext.createSSLEngine(host, port)
    engine.setEnabledProtocols(Array("SSLv3"))
    engine.setUseClientMode(true)
    new SSLSocketChannel(sch, engine)
  }

  def makeSecureServerConnection(socketChannel: SocketChannel,
                                 wantClientAuth: Boolean = true,
                                 needClientAuth: Boolean = true) = {
    val engine = socketChannel.socket.getRemoteSocketAddress match {
      case ise: InetSocketAddress =>
        SecureAuth.sslContext.createSSLEngine(ise.getHostName, ise.getPort)
      case _ =>
        SecureAuth.sslContext.createSSLEngine()
    }
    engine.setEnabledProtocols(Array("SSLv3"))
    engine.setUseClientMode(false)
    if (wantClientAuth) {
      engine.setWantClientAuth(true)
    }
    if (needClientAuth) {
      engine.setNeedClientAuth(true)
    }
    new SSLSocketChannel(socketChannel, engine)
  }

  val simulateSlowNetwork = false

  val runningTasks = -2
  private[this] lazy val counter = new AtomicInteger(0)
  private[kafka] lazy val executor = new ThreadPoolExecutor(2, 10,
                                                            60L, TimeUnit.SECONDS,
                                                            new SynchronousQueue[Runnable](),
                                                            new ThreadFactory() {
                                                              override def newThread(r: Runnable): Thread = {
                                                                val thread = new Thread(r, "SSLSession-Task-Thread-%d".format(counter.incrementAndGet()))
                                                                thread.setDaemon(true)
                                                                thread
                                                              }
                                                            })
}

class SSLSocketChannel(val underlying: SocketChannel, val sslEngine: SSLEngine)
  extends SocketChannel(underlying.provider) with Logging {

  import SSLSocketChannel.executor

  private[this] class SSLTasker(val runnable: Runnable) extends Runnable {
    selectionKey.interestOps(0)

    override def run(): Unit = {
      try {
        runnable.run()
        outer.synchronized {
                             handshakeStatus = sslEngine.getHandshakeStatus
                             handshakeStatus match {
                               case HandshakeStatus.NEED_WRAP =>
                                 debug("sslTasker setting up to write for %s".format(underlying.socket.getRemoteSocketAddress))
                                 selectionKey.interestOps(SelectionKey.OP_WRITE)
                               case HandshakeStatus.NEED_UNWRAP =>
                                 if (peerNetData.position > 0) {
                                   debug("sslTasker found existing data %s. running hanshake for %s".format(peerNetData,
                                                                                                            underlying.socket.getRemoteSocketAddress))
                                   val init = outer.handshake(SelectionKey.OP_READ, selectionKey)
                                   if (init == 0) {
                                     debug("sslTasker setting up to read after hanshake")
                                     selectionKey.interestOps(SelectionKey.OP_READ)
                                   } else if (init != SSLSocketChannel.runningTasks) {
                                     debug("sslTasker setting up for operation %d after hanshake for %s".format(init,
                                                                                                                underlying.socket.getRemoteSocketAddress))
                                     selectionKey.interestOps(init)
                                   }
                                 } else {
                                   debug("sslTasker setting up to read for %s".format(underlying.socket.getRemoteSocketAddress))
                                   selectionKey.interestOps(SelectionKey.OP_READ)
                                 }
                               case HandshakeStatus.NEED_TASK =>
                                 val runnable = sslEngine.getDelegatedTask
                                 if (runnable != null) {
                                   debug("sslTasker running next task for %s".format(underlying.socket.getRemoteSocketAddress))
                                   executor.execute(new SSLTasker(runnable))
                                   handshakeStatus = null
                                 }
                                 return
                               case _ =>
                                 throw new SSLException("unexpected handshakeStatus: " + handshakeStatus)
                             }
                             selectionKey.selector.wakeup()
                           }
      } catch {
        case t: Throwable =>
          error("Unexpected exception", t)
      }
    }
  }

  private[this] val outer = this

  /**
   * The engine handshake status.
   */
  private[this] var handshakeStatus: HandshakeStatus = HandshakeStatus.NOT_HANDSHAKING

  /**
   * The initial handshake ops.
   */
  @volatile private[this] var initialized = -1

  /**
   * Marker for shutdown status
   */
  private[this] var shutdown = false

  private[this] var peerAppData = ByteBuffer.allocate(sslEngine.getSession.getApplicationBufferSize)
  private[this] var myNetData = ByteBuffer.allocate(sslEngine.getSession.getPacketBufferSize)
  private[this] var peerNetData = ByteBuffer.allocate(sslEngine.getSession.getPacketBufferSize)
  private[this] val emptyBuffer = ByteBuffer.allocate(0)

  myNetData.limit(0)

  underlying.configureBlocking(false)

  private[this] var blocking = false
  private[this] lazy val blockingSelector = Selector.open()
  private[this] var blockingKey: SelectionKey = null

  @volatile private[this] var selectionKey: SelectionKey = null

  def simulateBlocking(b: Boolean) = {
    blocking = b
  }

  def socket(): Socket = underlying.socket

  def isConnected: Boolean = underlying.isConnected

  def isConnectionPending: Boolean = underlying.isConnectionPending

  def connect(remote: SocketAddress): Boolean = {
    debug("SSLSocketChannel Connecting to Remote : " + remote)
    val ret = underlying.connect(remote)
    if (blocking) {
      while (!finishConnect()) {
        try {
          Thread.sleep(10)
        } catch {
          case _: InterruptedException =>
        }
      }
      blockingKey = underlying.register(blockingSelector, SelectionKey.OP_READ)
      handshakeInBlockMode(SelectionKey.OP_WRITE)
      true
    } else ret
  }

  def finishConnect(): Boolean = underlying.finishConnect()

  def isReadable = finished && (peerAppData.position > 0 || peerNetData.position > 0)

  def read(dst: ByteBuffer): Int = {
    this.synchronized {
                        if (peerAppData.position >= dst.remaining) {
                          return readFromPeerData(dst)
                        } else if (underlying.socket.isInputShutdown) {
                          throw new ClosedChannelException
                        } else if (initialized != 0) {
                          handshake(SelectionKey.OP_READ, selectionKey)
                          return 0
                        } else if (shutdown) {
                          shutdown()
                          return -1
                        } else if (sslEngine.isInboundDone) {
                          return -1
                        } else {
                          val count = readRaw()
                          if (count <= 0 && peerNetData.position == 0) return count.asInstanceOf[Int]
                        }

                        if (unwrap(false) < 0) return -1

                        readFromPeerData(dst)
                      }
  }

  def read(destination: Array[ByteBuffer], offset: Int, length: Int): Long = {
    var n = 0
    var i = offset
    def localReadLoop() {
      while (i < length) {
        if (destination(i).hasRemaining) {
          val x = read(destination(i))
          if (x > 0) {
            n += x
            if (!destination(i).hasRemaining) {
              return
            }
          } else {
            if ((x < 0) && (n == 0)) {
              n = -1
            }
            return
          }
        }
        i = i + 1
      }
    }
    localReadLoop()
    n
  }

  def write(source: ByteBuffer): Int = {
    this.synchronized {
                        if (myNetData.hasRemaining) {
                          writeRaw(myNetData)
                          return 0
                        } else if (underlying.socket.isOutputShutdown) {
                          throw new ClosedChannelException
                        } else if (initialized != 0) {
                          handshake(SelectionKey.OP_WRITE, selectionKey)
                          return 0
                        } else if (shutdown) {
                          shutdown()
                          return -1
                        }

                        val written = wrap(source)

                        while (myNetData.hasRemaining)
                          writeRaw(myNetData)
                        written
                      }
  }

  def write(sources: Array[ByteBuffer], offset: Int, length: Int): Long = {
    var n = 0
    var i = offset
    def localWriteLoop {
      while (i < length) {
        if (sources(i).hasRemaining) {
          var x = write(sources(i))
          if (x > 0) {
            n += x
            if (!sources(i).hasRemaining) {
              return
            }
          } else {
            return
          }
        }
        i = i + 1
      }
    }
    localWriteLoop
    n
  }

  def finished(): Boolean = initialized == 0

  override def toString = "SSLSocketChannel[" + underlying.toString + "]"

  protected def implCloseSelectableChannel(): Unit = {
    try {
      _shutdown()
    } catch {
      case x: Exception =>
    }
    underlying.close()
  }

  protected def implConfigureBlocking(block: Boolean): Unit = {
    simulateBlocking(block)
    if (!block) underlying.configureBlocking(block)
  }

  def handshake(o: Int, key: SelectionKey): Int = {
    def writeIfReadyAndNeeded(mustWrite: Boolean): Boolean = {
      if ((o & SelectionKey.OP_WRITE) != 0) {
        writeRaw(myNetData)
        myNetData.remaining > 0
      } else mustWrite
    }    
    def readIfReadyAndNeeded(mustRead: Boolean): Boolean = {
      if ((o & SelectionKey.OP_READ) != 0) {
        if (readRaw() < 0) {
          shutdown = true
          underlying.close()
          return true
        }
        val oldPos = peerNetData.position
        unwrap(true)
        oldPos == peerNetData.position
      } else mustRead
    }

    def localHandshake(): Int = {
      while (true) {
        handshakeStatus match {
          case HandshakeStatus.NOT_HANDSHAKING =>
            info("begin ssl handshake for %s/%s".format(underlying.socket.getRemoteSocketAddress,
                                                        underlying.socket.getLocalSocketAddress))
            sslEngine.beginHandshake()
            handshakeStatus = sslEngine.getHandshakeStatus
          case HandshakeStatus.NEED_UNWRAP =>
            debug("need unwrap in ssl handshake for %s/%s".format(underlying.socket.getRemoteSocketAddress,
                                                                  underlying.socket.getLocalSocketAddress))
            if (readIfReadyAndNeeded(true) && handshakeStatus != HandshakeStatus.FINISHED) {
              debug("select to read more for %s/%s".format(underlying.socket.getRemoteSocketAddress,
                                                           underlying.socket.getLocalSocketAddress))
              return SelectionKey.OP_READ
            }
          case HandshakeStatus.NEED_WRAP =>
            debug("need wrap in ssl handshake for %s/%s".format(underlying.socket.getRemoteSocketAddress,
                                                                underlying.socket.getLocalSocketAddress))
            if (myNetData.remaining == 0) {
              wrap(emptyBuffer)
            }
            if (writeIfReadyAndNeeded(true)) {
              debug("select to write more for %s/%s".format(underlying.socket.getRemoteSocketAddress,
                                                            underlying.socket.getLocalSocketAddress))
              return SelectionKey.OP_WRITE
            }
          case HandshakeStatus.NEED_TASK =>
            handshakeStatus = runTasks()
          case HandshakeStatus.FINISHED =>
            info("finished ssl handshake for %s/%s".format(underlying.socket.getRemoteSocketAddress,
                                                           underlying.socket.getLocalSocketAddress))
            return 0
          case null =>
            return SSLSocketChannel.runningTasks
        }
      }
      o
    }

    this.synchronized {
                        if (initialized == 0) return initialized

                        if (selectionKey == null) selectionKey = key

                        if (initialized != -1) {
                          if (writeIfReadyAndNeeded(false)) return o
                        }
                        val init = localHandshake()
                        if (init != SSLSocketChannel.runningTasks) {
                          initialized = init
                        }
                        init
                      }
  }

  def shutdown() {
    debug("SSLSocketChannel shutting down with locking")
    this.synchronized(_shutdown())
    underlying.close()
  }

  private def _shutdown() {
    debug("SSLSocketChannel shutting down with out locking")
    shutdown = true

    try {
      if (!sslEngine.isOutboundDone) sslEngine.closeOutbound()

      myNetData.compact()
      while (!sslEngine.isOutboundDone) {
        val res = sslEngine.wrap(emptyBuffer, myNetData)
        if (res.getStatus != Status.CLOSED) {
          throw new SSLException("Unexpected shutdown status '%s'".format(res.getStatus))
        }

        myNetData.flip()
        try {
          while (myNetData.hasRemaining)
            writeRaw(myNetData)
        } catch {
          case ignore: IOException =>
        }
      }
    } finally {
      if (blockingKey != null) {
        try {
          blockingKey.cancel()
        } finally {
          blockingKey = null
          blockingSelector.close()
        }
      }
    }
  }

  private def handshakeInBlockMode(ops: Int) = {
    var o = ops
    while (o != 0) {
      val tops = handshake(o, null)
      if (tops == o) {
        try {
          Thread.sleep(10)
        } catch {
          case _: InterruptedException =>
        }
      } else {
        o = tops
      }
    }
    o
  }

  private[this] def readRaw(): Long = {
    def blockIfNeeded() {
      if (blockingKey != null) {
        try {
          blockingSelector.select(5000)
        } catch {
          case t: Throwable => error("Unexpected error in blocking select", t)
        }
      }
    }
    this.synchronized {
                        blockIfNeeded()
                        try {
                          val n = underlying.read(peerNetData)
                          if (n < 0) {
                            sslEngine.closeInbound()
                          }
                          n
                        } catch {
                          case x: IOException =>
                            sslEngine.closeInbound()
                            throw x
                        }
                      }
  }

  private[this] def unwrap(isHandshaking: Boolean): Int = {
    val pos = peerAppData.position
    peerNetData.flip()
    trace("unwrap: flipped peerNetData %s for %s/%s".format(peerNetData,
                                                            underlying.socket.getRemoteSocketAddress,
                                                            underlying.socket.getLocalSocketAddress))
    try {
      while (peerNetData.hasRemaining) {
        val result = sslEngine.unwrap(peerNetData, peerAppData)
        handshakeStatus = result.getHandshakeStatus
        result.getStatus match {
          case SSLEngineResult.Status.OK =>
            if (handshakeStatus == HandshakeStatus.NEED_TASK) {
              handshakeStatus = runTasks()
              if (handshakeStatus == null) return 0
            }
            if (isHandshaking && handshakeStatus == HandshakeStatus.FINISHED) {
              return peerAppData.position - pos
            }
          case SSLEngineResult.Status.BUFFER_OVERFLOW =>
            peerAppData = expand(peerAppData, sslEngine.getSession.getApplicationBufferSize)
          case SSLEngineResult.Status.BUFFER_UNDERFLOW =>
            return 0
          case SSLEngineResult.Status.CLOSED =>
            if (peerAppData.position == 0) {
              trace("uwrap: shutdown for %s/%s".format(peerAppData,
                                                       underlying.socket.getRemoteSocketAddress,
                                                       underlying.socket.getLocalSocketAddress))
              shutdown()
              return -1
            } else {
              trace("uwrap: shutdown with non-empty peerAppData %s for %s/%s".format(peerAppData,
                                                                                     underlying.socket.getRemoteSocketAddress,
                                                                                     underlying.socket.getLocalSocketAddress))
              shutdown = true
              return 0
            }
          case _ =>
            throw new SSLException("Unexpected state!")
        }
      }
    } finally {
      peerNetData.compact()
      trace("unwrap: compacted peerNetData %s for %s/%s".format(peerNetData,
                                                                underlying.socket.getRemoteSocketAddress,
                                                                underlying.socket.getLocalSocketAddress))
    }
    peerAppData.position - pos
  }

  private[this] def wrap(src: ByteBuffer): Int = {
    val written = src.remaining
    myNetData.compact()
    trace("wrap: compacted myNetData %s for %s/%s".format(myNetData,
                                                          underlying.socket.getRemoteSocketAddress,
                                                          underlying.socket.getLocalSocketAddress))
    try {
      do {
        val result = sslEngine.wrap(src, myNetData)
        handshakeStatus = result.getHandshakeStatus
        result.getStatus match {
          case SSLEngineResult.Status.OK =>
            if (handshakeStatus == HandshakeStatus.NEED_TASK) {
              handshakeStatus = runTasks()
              if (handshakeStatus == null) return 0
            }
          case SSLEngineResult.Status.BUFFER_OVERFLOW =>
            val size = if (src.remaining * 2 > sslEngine.getSession.getApplicationBufferSize) src.remaining * 2
            else sslEngine.getSession.getApplicationBufferSize
            myNetData = expand(myNetData, size)
          case SSLEngineResult.Status.CLOSED =>
            shutdown()
            throw new IOException("Write error received Status.CLOSED")
          case _ =>
            throw new SSLException("Unexpected state!")
        }
      } while (src.hasRemaining)
    } finally {
      myNetData.flip()
      trace("wrap: flipped myNetData %s for %s/%s".format(myNetData,
                                                          underlying.socket.getRemoteSocketAddress,
                                                          underlying.socket.getLocalSocketAddress))
    }
    written
  }

  private[this] def writeRaw(out: ByteBuffer): Long = {
    def writeTwo(i: ByteBuffer): ByteBuffer = {
      val o = ByteBuffer.allocate(2)
      var rem = i.limit - i.position
      if (rem > o.capacity) rem = o.capacity
      var c = 0
      while (c < rem) {
        o.put(i.get)
        c += 1
      }
      o.flip()
      o
    }
    try {
      if (out.hasRemaining) {
        underlying.write(if (SSLSocketChannel.simulateSlowNetwork) writeTwo(out) else out)
      } else 0
    } catch {
      case x: IOException =>
        sslEngine.closeOutbound()
        shutdown = true
        throw x
    }
  }

  private[this] def runTasks(ops: Int = SelectionKey.OP_READ): HandshakeStatus = {
    val reInitialize = initialized match {
      case 0 =>
        initialized = ops
        info("runTasks running renegotiation for %s/%s".format(underlying.socket.getRemoteSocketAddress,
                                                               underlying.socket.getLocalSocketAddress))
        true
      case _ => false
    }
    var runnable: Runnable = sslEngine.getDelegatedTask
    if (!blocking && selectionKey != null) {
      debug("runTasks asynchronously in ssl handshake for %s/%s".format(underlying.socket.getRemoteSocketAddress,
                                                                        underlying.socket.getLocalSocketAddress))
      if (runnable != null) {
        executor.execute(new SSLTasker(runnable))
      }
      null
    } else {
      debug("runTasks synchronously in ssl handshake for %s/%s".format(underlying.socket.getRemoteSocketAddress,
                                                                       underlying.socket.getLocalSocketAddress))
      while (runnable != null) {
        runnable.run()
        runnable = sslEngine.getDelegatedTask
      }
      if (reInitialize) {
        handshakeInBlockMode(ops)
      }
      sslEngine.getHandshakeStatus
    }
  }

  private[this] def expand(src: ByteBuffer, ensureSize: Int): ByteBuffer = {
    if (src.remaining < ensureSize) {
      val newBuffer = ByteBuffer.allocate(src.capacity + ensureSize)
      if (src.position > 0) {
        src.flip()
        newBuffer.put(src)
      }
      newBuffer
    } else {
      src
    }
  }

  private[this] def readFromPeerData(dest: ByteBuffer): Int = {
    peerAppData.flip()
    try {
      var remaining = peerAppData.remaining
      if (remaining > 0) {
        if (remaining > dest.remaining) {
          remaining = dest.remaining
        }
        var i = 0
        while (i < remaining) {
          dest.put(peerAppData.get)
          i = i + 1
        }
      }
      remaining
    } finally {
      peerAppData.compact()
    }
  }

  def bind(local: SocketAddress): SocketChannel = underlying.bind(local)

  def shutdownInput(): SocketChannel = shutdownInput()

  def setOption[T](name: SocketOption[T], value: T): SocketChannel = underlying.setOption(name, value)

  def getRemoteAddress: SocketAddress = underlying.getRemoteAddress

  def shutdownOutput(): SocketChannel = underlying.shutdownOutput()

  def getLocalAddress: SocketAddress = underlying.getLocalAddress

  def getOption[T](name: SocketOption[T]): T = underlying.getOption(name)

  def supportedOptions(): util.Set[SocketOption[_]] = underlying.supportedOptions
}
