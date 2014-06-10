/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.util.concurrent._
import java.util.concurrent.atomic._
import java.net._
import java.io._
import java.nio.channels._

import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.network.security._
import kafka.utils._
import com.yammer.metrics.core.Meter
import javax.net.ssl.SSLException

/**
 * An NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
class SocketServer(val brokerId: Int,
                   val host: String,
                   val port: Int,
                   val secure: Boolean,
                   val securityConfig: AuthConfig,
                   val numProcessorThreads: Int,
                   val maxQueuedRequests: Int,
                   val sendBufferSize: Int,
                   val receiveBufferSize: Int,
                   val maxRequestSize: Int = Int.MaxValue) extends Logging with KafkaMetricsGroup {
  this.logIdent = "[Socket Server on Broker " + brokerId + "], "
  private val time = SystemTime
  private val processors = new Array[Processor](numProcessorThreads)
  @volatile private var acceptor: Acceptor = null
  val requestChannel = new RequestChannel(numProcessorThreads, maxQueuedRequests)

  /* a meter to track the average free capacity of the network processors */
  private val aggregateIdleMeter = newMeter("NetworkProcessorAvgIdlePercent", "percent", TimeUnit.NANOSECONDS)

  /**
   * Start the socket server
   */
  def startup() {
    // If secure setup SSLContext
    if (secure) SecureAuth.initialize(securityConfig)

    for (i <- 0 until numProcessorThreads) {
      processors(i) = new Processor(i, time, maxRequestSize, aggregateIdleMeter,
                                    newMeter("NetworkProcessor-" + i + "-IdlePercent", "percent", TimeUnit.NANOSECONDS),
                                    numProcessorThreads, requestChannel, secure)
      Utils.newThread("kafka-network-thread-%d-%d".format(port, i), processors(i), false).start()
    }
    // register the processor threads for notification of responses
    requestChannel.addResponseListener((id: Int) => processors(id).wakeup())

    // start accepting connections
    this.acceptor = new Acceptor(host, port, secure, securityConfig, processors, sendBufferSize, receiveBufferSize)
    Utils.newThread("kafka-socket-acceptor", acceptor, false).start()
    acceptor.awaitStartup
    info("Started")
  }

  /**
   * Shutdown the socket server
   */
  def shutdown() = {
    info("Shutting down")
    if (acceptor != null)
      acceptor.shutdown()
    for (processor <- processors)
      processor.shutdown()
    info("Shutdown completed")
  }
}

/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread extends Runnable with Logging {
  protected val selector = Selector.open()
  private val startupLatch = new CountDownLatch(1)
  private val shutdownLatch = new CountDownLatch(1)
  private val alive = new AtomicBoolean(false)

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  def shutdown(): Unit = {
    alive.set(false)
    selector.wakeup()
    shutdownLatch.await
  }

  /**
   * Wait for the thread to completely start up
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
   */
  protected def startupComplete() = {
    alive.set(true)
    startupLatch.countDown
  }

  /**
   * Record that the thread shutdown is complete
   */
  protected def shutdownComplete() = shutdownLatch.countDown

  /**
   * Is the server still running?
   */
  protected def isRunning = alive.get

  /**
   * Wakeup the thread for selection.
   */
  def wakeup() = selector.wakeup()

}

/**
 * Thread that accepts and configures new connections. There is only need for one of these
 */
private[kafka] class Acceptor(val host: String,
                              val port: Int,
                              val secure: Boolean,
                              val securityConfig: AuthConfig,
                              private val processors: Array[Processor],
                              val sendBufferSize: Int,
                              val receiveBufferSize: Int) extends AbstractServerThread {
  val serverChannel = openServerSocket(host, port)

  /**
   * Accept loop that checks for new connection attempts
   */
  def run() {
    serverChannel.register(selector, SelectionKey.OP_ACCEPT)
    startupComplete()
    var currentProcessor = 0
    while (isRunning) {
      val ready = selector.select(500)
      if (ready > 0) {
        val keys = selector.selectedKeys()
        val iter = keys.iterator()
        while (iter.hasNext && isRunning) {
          var key: SelectionKey = null
          try {
            key = iter.next
            iter.remove()
            if (key.isAcceptable)
              accept(key, processors(currentProcessor))
            else
              throw new IllegalStateException("Unrecognized key state for acceptor thread.")

            // round robin to the next processor thread
            currentProcessor = (currentProcessor + 1) % processors.length
          } catch {
            case e: Throwable => error("Error in acceptor", e)
          }
        }
      }
    }
    debug("Closing server socket and selector.")
    swallowError(serverChannel.close())
    swallowError(selector.close())
    shutdownComplete()
  }

  /*
   * Create a server socket to listen for connections on.
   */
  def openServerSocket(host: String, port: Int): ServerSocketChannel = {
    val socketAddress =
      if (host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)
    try {
      serverChannel.socket.bind(socketAddress)
      info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostName, port))
    } catch {
      case e: SocketException =>
        throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostName, port, e.getMessage), e)
    }
    serverChannel
  }

  /*
   * Accept a new connection
   */
  def accept(key: SelectionKey, processor: Processor) {
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    serverSocketChannel.socket().setReceiveBufferSize(receiveBufferSize)

    val sch = serverSocketChannel.accept()
    val socketChannel = if (secure) SSLSocketChannel.makeSecureServerConnection(sch, securityConfig.wantClientAuth, securityConfig.needClientAuth) else sch
    socketChannel.configureBlocking(false)
    socketChannel.socket().setTcpNoDelay(true)
    socketChannel.socket().setSendBufferSize(sendBufferSize)

    debug("Accepted connection from %s on %s. sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
          .format(socketChannel.socket.getInetAddress, socketChannel.socket.getLocalSocketAddress,
                  socketChannel.socket.getSendBufferSize, sendBufferSize,
                  socketChannel.socket.getReceiveBufferSize, receiveBufferSize))

    processor.accept(socketChannel)
  }

}

private case class ChannelTuple(value: Any, sslChannel: SSLSocketChannel)

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selectors
 */
private[kafka] class Processor(val id: Int,
                               val time: Time,
                               val maxRequestSize: Int,
                               val aggregateIdleMeter: Meter,
                               val idleMeter: Meter,
                               val totalProcessorThreads: Int,
                               val requestChannel: RequestChannel,
                               val secure: Boolean) extends AbstractServerThread {

  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()

  override def run() {
    startupComplete()
    while (isRunning) {
      try {
        // setup any new connections that have been queued up
        configureNewConnections()
        // register any new responses for writing
        processNewResponses()
        val startSelectTime = SystemTime.nanoseconds
        val ready = selector.select(300)
        val idleTime = SystemTime.nanoseconds - startSelectTime
        idleMeter.mark(idleTime)
        // We use a single meter for aggregate idle percentage for the thread pool.
        // Since meter is calculated as total_recorded_value / time_window and
        // time_window is independent of the number of threads, each recorded idle
        // time should be discounted by # threads.
        aggregateIdleMeter.mark(idleTime / totalProcessorThreads)

        trace("Processor id " + id + " selection time = " + idleTime + " ns")
        if (ready > 0) {
          val keys = selector.selectedKeys()
          val iter = keys.iterator()
          while (iter.hasNext && isRunning) {
            var key: SelectionKey = null
            try {
              key = iter.next
              iter.remove()
              if (key.isReadable)
                read(key)
              else if (key.isWritable)
                write(key)
              else if (!key.isValid)
                close(key)
              else
                throw new IllegalStateException("Unrecognized key state for processor thread.")
            } catch {
              case e: EOFException => {
                info("Closing socket connection to %s.".format(channelFor(key).socket.getInetAddress))
                close(key)
              }
              case e: InvalidRequestException => {
                info("Closing socket connection to %s due to invalid request: %s".format(channelFor(key).socket.getInetAddress, e.getMessage))
                close(key)
              }
              case e: Throwable => {
                error("Closing socket for " + channelFor(key).socket.getInetAddress + " because of error", e)
                close(key)
              }
            }
          }
        }
      } catch {
        case e: Throwable => {
          error("Unexpected error", e)
        }
      }
    }
    debug("Closing selector.")
    closeAll()
    swallowError(selector.close())
    shutdownComplete()
  }

  private def processNewResponses() {
    var curr = requestChannel.receiveResponse(id)
    while (curr != null) {
      val key = curr.request.requestKey.asInstanceOf[SelectionKey]
      val channelTuple = key.attachment.asInstanceOf[ChannelTuple]
      try {
        curr.responseAction match {
          case RequestChannel.NoOpAction => {
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            curr.request.updateRequestMetrics()
            trace("Socket server received empty response to send, registering for read: " + curr)
            key.interestOps(SelectionKey.OP_READ)
            key.attach(ChannelTuple(null, channelTuple.sslChannel))
            readBufferedSSLDataIfNeeded(key, channelTuple)
          }
          case RequestChannel.SendAction => {
            trace("Socket server received response to send, registering for write: " + curr)
            key.interestOps(SelectionKey.OP_WRITE)
            key.attach(ChannelTuple(curr, channelTuple.sslChannel))
          }
          case RequestChannel.CloseConnectionAction => {
            curr.request.updateRequestMetrics()
            trace("Closing socket connection actively according to the response code.")
            close(key)
          }
          case responseCode => throw new KafkaException("No mapping found for response code " + responseCode)
        }
      } catch {
        case e: CancelledKeyException => {
          debug("Ignoring response for closed socket.")
          close(key)
        }
      } finally {
        curr = requestChannel.receiveResponse(id)
      }
    }
  }

  private def close(key: SelectionKey) {
    try {
      val channel = channelFor(key)
      debug("Closing connection from " + channel.socket.getRemoteSocketAddress)
      swallowError(channel.close())
      swallowError(channel.socket().close())
    } finally {
      key.attach(null)
      swallowError(key.cancel())
    }
  }

  /*
   * Close all open connections
   */
  private def closeAll() {
    val iter = this.selector.keys().iterator()
    while (iter.hasNext) {
      val key = iter.next()
      close(key)
    }
  }

  /**
   * Queue up a new connection for reading
   */
  def accept(socketChannel: SocketChannel) {
    newConnections.add(socketChannel)
    wakeup()
  }

  /**
   * Register any new connections that have been queued up
   */
  private def configureNewConnections() {
    while (newConnections.size() > 0) {
      val channel = newConnections.poll()
      debug("Processor %s listening to new connection from %s".format(id, channel.socket.getRemoteSocketAddress))
      val (regChannel, sslsch) = channel match {
        case sslsch: SSLSocketChannel =>
          val rch = sslsch.underlying.asInstanceOf[SocketChannel]
          (rch, sslsch)
        case _ => (channel, null)
      }
      val key = regChannel.register(selector, SelectionKey.OP_READ)
      key.attach(ChannelTuple(null, sslsch))
    }
  }

  /*
   * Process reads from ready sockets
   */
  def read(key: SelectionKey) {
    val channelTuple = key.attachment.asInstanceOf[ChannelTuple]
    val socketChannel = channelFor(key, SelectionKey.OP_READ)
    if (socketChannel == null) return
    var receive = channelTuple.value.asInstanceOf[Receive]
    if (receive == null) {
      receive = new BoundedByteBufferReceive(maxRequestSize)
      key.attach(ChannelTuple(receive, channelTuple.sslChannel))
    }
    val read = receive.readFrom(socketChannel)
    val address = socketChannel.socket.getRemoteSocketAddress
    trace(read + " bytes read from " + address)
    if (read < 0) {
      close(key)
    } else if (receive.complete) {
      val req = RequestChannel.Request(processor = id, requestKey = key, buffer = receive.buffer, startTimeMs = time.milliseconds, remoteAddress = address)
      requestChannel.sendRequest(req)
      key.attach(ChannelTuple(null, channelTuple.sslChannel))
      // explicitly reset interest ops to not READ, no need to wake up the selector just yet
      key.interestOps(key.interestOps & (~SelectionKey.OP_READ))
    } else {
      // more reading to be done
      trace("Did not finish reading, registering for read again on connection " + socketChannel.socket.getRemoteSocketAddress)
      key.interestOps(SelectionKey.OP_READ)
      wakeup()
    }
  }

  /*
   * Process writes to ready sockets
   */
  def write(key: SelectionKey) {
    val channelTuple = key.attachment.asInstanceOf[ChannelTuple]
    val socketChannel = channelFor(key, SelectionKey.OP_WRITE)
    if (socketChannel == null) return
    val response = channelTuple.value.asInstanceOf[RequestChannel.Response]
    val responseSend = response.responseSend
    if (responseSend == null)
      throw new IllegalStateException("Registered for write interest but no response attached to key.")
    val written = responseSend.writeTo(socketChannel)
    trace(written + " bytes written to " + socketChannel.socket.getRemoteSocketAddress + " using key " + key)
    if (responseSend.complete) {
      response.request.updateRequestMetrics()
      key.attach(ChannelTuple(null, channelTuple.sslChannel))
      trace("Finished writing, registering for read on connection " + socketChannel.socket.getRemoteSocketAddress)
      key.interestOps(SelectionKey.OP_READ)
      readBufferedSSLDataIfNeeded(key, channelTuple)
    } else {
      trace("Did not finish writing, registering for write again on connection " + socketChannel.socket.getRemoteSocketAddress)
      key.interestOps(SelectionKey.OP_WRITE)
      wakeup()
    }
  }

  private def channelFor(key: SelectionKey, ops: Int = -1) = {
    val sch = key.channel.asInstanceOf[SocketChannel]
    if (secure) {
      val secureSocketChannel = key.attachment.asInstanceOf[ChannelTuple].sslChannel
      if (ops >= 0 && !secureSocketChannel.finished()) {
        var done = false
        try {
          val next = secureSocketChannel.handshake(key.interestOps(), key)
          if (next == 0) {
            // when handshake is complete and we are doing a read so ahead with the read
            // otherwise go back to read mode
            if (ops == SelectionKey.OP_READ) {
              done = true
            } else {
              key.interestOps(SelectionKey.OP_READ)
            }
          } else if (next != SSLSocketChannel.runningTasks) {
            key.interestOps(next)
          }
        } catch {
          case e: SSLException => // just ignore SSL disconnect errors
            debug("SSLException: " + e)
            close(key)
        }
        if (done) secureSocketChannel else null
      } else secureSocketChannel
    } else sch
  }

  private[this] def readBufferedSSLDataIfNeeded(key: SelectionKey, channelTuple: ChannelTuple) {
    try {
      if (channelTuple.sslChannel != null && channelTuple.sslChannel.isReadable) {
        read(key)
      }
    } catch {
      case e: EOFException => {
        info("Closing socket connection to %s.".format(channelFor(key).socket.getInetAddress))
        close(key)
      }
      case e: InvalidRequestException => {
        info("Closing socket connection to %s due to invalid request: %s".format(channelFor(key).socket.getInetAddress,
                                                                                 e.getMessage))
        close(key)
      }
      case e: Throwable => {
        error("Closing socket for %s because of error".format(channelFor(key).socket.getInetAddress), e)
        close(key)
      }
    }
  }
}
