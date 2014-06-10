package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.network.security.SecureAuth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static javax.net.ssl.SSLEngineResult.HandshakeStatus;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.*;
import static javax.net.ssl.SSLEngineResult.Status.CLOSED;

public class SSLSocketChannel<T> extends SocketChannel {
    private static final Logger log = LoggerFactory.getLogger(SSLSocketChannel.class);

    private final SSLSocketChannel outer = this;
    private final SSLEngine sslEngine;
    private final boolean simulateSlowNetwork = false;
    private final int runningTasks = -2;
    private final AtomicInteger counter = new AtomicInteger(0);
    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 10,
            60L, TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>(),
            new ThreadFactory() {
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r, String.format("SSLSession-Task-Thread-%d",
                            counter.incrementAndGet()));
                    thread.setDaemon(true);
                    return thread;
                }
            }
    );

    private HandshakeStatus handshakeStatus = NOT_HANDSHAKING;

    private SocketChannel underlying;

    private volatile int initialized = -1;
    private boolean shutdown = false;

    private ByteBuffer peerAppData;
    private ByteBuffer myNetData;
    private ByteBuffer peerNetData;
    private ByteBuffer emptyBuffer;

    private boolean blocking = false;
    private Selector blockingSelector;
    private SelectionKey blockingKey = null;
    private volatile SelectionKey selectionKey = null;

    public SSLSocketChannel(SocketChannel underlying, SSLEngine sslEngine) throws IOException {
        super(underlying.provider());
        this.underlying = underlying;
        this.sslEngine = sslEngine;
        this.peerAppData = ByteBuffer.allocate(sslEngine.getSession().getApplicationBufferSize());
        this.myNetData = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
        this.peerNetData = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
        this.emptyBuffer = ByteBuffer.allocate(0);

        myNetData.limit(0);
        underlying.configureBlocking(false);

        this.blockingSelector = Selector.open();
    }

    public static SSLSocketChannel create(SocketChannel sch, String host, int port) throws IOException {
        SSLEngine engine = SecureAuth.getSSLContext().createSSLEngine(host, port);
        engine.setEnabledProtocols(new String[]{"SSLv3"});
        engine.setUseClientMode(true);
        return new SSLSocketChannel(sch, engine);
    }

    public void simulateBlocking(Boolean b) {
        blocking = b;
    }

    public Socket socket() {
        return underlying.socket();
    }

    public boolean isConnected() {
        return underlying.isConnected();
    }

    public boolean isConnectionPending() {
        return underlying.isConnectionPending();
    }

    public boolean connect(SocketAddress remote) throws IOException {
        boolean ret = underlying.connect(remote);
        if (blocking) {
            while (!finishConnect()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ignore) {
                }
            }
            blockingKey = underlying.register(blockingSelector, SelectionKey.OP_READ);
            try {
                handshakeInBlockMode(SelectionKey.OP_WRITE);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return true;
        } else {
            return ret;
        }
    }

    public boolean finishConnect() throws IOException {
        return underlying.finishConnect();
    }

    public boolean finished() {
        return initialized == 0;
    }

    public boolean isReadable() {
        return finished() && (peerAppData.position() > 0 || peerNetData.position() > 0);
    }

    public synchronized int read(ByteBuffer dst) throws IOException {
        if (peerAppData.position() >= dst.remaining()) {
            return readFromPeerData(dst);
        } else if (underlying.socket().isInputShutdown()) {
            throw new ClosedChannelException();
        } else if (initialized != 0) {
            try {
                handshake(SelectionKey.OP_READ, selectionKey);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return 0;
        } else if (shutdown) {
            shutdown();
            return -1;
        } else if (sslEngine.isInboundDone()) {
            return -1;
        } else {
            int count = (int) readRaw();
            if (count <= 0 && peerNetData.position() == 0) return count;
        }

        try {
            if (unwrap(false) < 0) return -1;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return readFromPeerData(dst);
    }

    public long read(ByteBuffer[] destination, int offset, int length) throws IOException {
        int n = 0;
        int i = offset;

        while (i < length) {
            if (destination[i].hasRemaining()) {
                int x = read(destination[i]);
                if (x > 0) {
                    n += x;
                    if (!destination[i].hasRemaining()) {
                        break;
                    }
                } else {
                    if ((x < 0) && (n == 0)) {
                        n = -1;
                    }
                    break;
                }
            }
            i = i + 1;
        }

        return n;
    }

    public synchronized int write(ByteBuffer source) throws IOException {
        if (myNetData.hasRemaining()) {
            writeRaw(myNetData);
            return 0;
        } else if (underlying.socket().isOutputShutdown()) {
            throw new ClosedChannelException();
        } else if (initialized != 0) {
            try {
                handshake(SelectionKey.OP_WRITE, selectionKey);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return 0;
        } else if (shutdown) {
            shutdown();
            return -1;
        }

        int written;
        try {
            written = wrap(source);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        while (myNetData.hasRemaining())
            writeRaw(myNetData);

        return written;
    }

    public long write(ByteBuffer[] sources, int offset, int length) throws IOException {
        int n = 0;
        int i = offset;
        while (i < length) {
            if (sources[i].hasRemaining()) {
                int x = write(sources[i]);
                if (x > 0) {
                    n += x;
                    if (!sources[i].hasRemaining()) {
                        return 0;
                    }
                } else {
                    return 0;
                }
            }
            i = i + 1;
        }
        return n;
    }

    @Override
    public String toString() {
        return "SSLSocketChannel[" + underlying.toString() + "]";
    }

    protected void implCloseSelectableChannel() throws IOException {
        try {
            _shutdown();
        } catch (Exception ignore) {
        }
        underlying.close();
    }

    protected void implConfigureBlocking(boolean block) throws IOException {
        simulateBlocking(block);
        if (!block) underlying.configureBlocking(block);
    }

    public synchronized int handshake(int o, SelectionKey key) throws IOException, InterruptedException {
        if (initialized == 0) return initialized;

        if (selectionKey == null) selectionKey = key;

        if (initialized != -1) {
            if (writeIfReadyAndNeeded(o, false)) return o;
        }
        int init = localHandshake(o);
        if (init != runningTasks) {
            initialized = init;
        }
        return init;
    }

    private boolean writeIfReadyAndNeeded(int o, boolean mustWrite) throws IOException {
        if ((o & SelectionKey.OP_WRITE) != 0) {
            writeRaw(myNetData);
            return myNetData.remaining() > 0;
        } else {
            return mustWrite;
        }
    }

    private boolean readIfReadyAndNeeded(int o, boolean mustRead) throws IOException, InterruptedException {
        if ((o & SelectionKey.OP_READ) != 0) {
            if (readRaw() < 0) {
                shutdown = true;
                underlying.close();
                return true;
            }
            int oldPos = peerNetData.position();
            unwrap(true);
            return oldPos == peerNetData.position();
        } else {
            return mustRead;
        }
    }

    private int localHandshake(int o) throws IOException, InterruptedException {
        while (!Thread.currentThread().isInterrupted()) {
            switch (handshakeStatus) {
                case NOT_HANDSHAKING:
                    sslEngine.beginHandshake();
                    handshakeStatus = sslEngine.getHandshakeStatus();
                    break;
                case NEED_UNWRAP:
                    if (readIfReadyAndNeeded(o, true) && handshakeStatus != FINISHED) {
                        return SelectionKey.OP_READ;
                    }
                    break;
                case NEED_WRAP:
                    if (myNetData.remaining() == 0) {
                        wrap(emptyBuffer);
                    }
                    if (writeIfReadyAndNeeded(o, true)) {
                        return SelectionKey.OP_WRITE;
                    }
                    break;
                case NEED_TASK:
                    handshakeStatus = runTasks(SelectionKey.OP_READ);
                    break;
                case FINISHED:
                    underlying.socket().getLocalSocketAddress();
                    return 0;
                default:
                    if (handshakeStatus == null) return runningTasks;
            }
        }

        return o;
    }

    public void shutdown() throws IOException {
        _shutdown();
        underlying.close();
    }

    synchronized private void _shutdown() throws IOException {
        shutdown = true;

        try {
            if (!sslEngine.isOutboundDone()) sslEngine.closeOutbound();

            myNetData.compact();
            while (!sslEngine.isOutboundDone()) {
                SSLEngineResult res = sslEngine.wrap(emptyBuffer, myNetData);
                if (res.getStatus() != CLOSED) {
                    throw new SSLException(String.format("Unexpected shutdown status '%s'", res.getStatus()));
                }

                myNetData.flip();
                try {
                    while (myNetData.hasRemaining())
                        writeRaw(myNetData);
                } catch (IOException ignore) {
                }
            }
        } finally {
            if (blockingKey != null) {
                try {
                    blockingKey.cancel();
                } finally {
                    blockingKey = null;
                    blockingSelector.close();
                }
            }
        }
    }

    private int handshakeInBlockMode(int ops) throws InterruptedException, IOException {
        int o = ops;
        while (o != 0) {
            int tops = handshake(o, null);
            if (tops == o) {
                try {
                    Thread.sleep(10);
                } catch (Throwable e) {
                    //case _:  => InterruptedException
                }
            } else {
                o = tops;
            }
        }
        return o;
    }

    private void blockIfNeeded() {
        if (blockingKey != null) {
            try {
                blockingSelector.select(5000);
            } catch (Throwable e) {
                //error("Unexpected error in blocking select", t)
            }
        }
    }

    private synchronized long readRaw() throws IOException {
        blockIfNeeded();
        try {
            int n = underlying.read(peerNetData);
            if (n < 0) {
                sslEngine.closeInbound();
            }

            return n;
        } catch (IOException e) {
            sslEngine.closeInbound();
            throw e;
        }
    }

    private int unwrap(boolean isHandshaking) throws IOException, InterruptedException {
        int pos = peerAppData.position();
        peerNetData.flip();
        try {
            while (peerNetData.hasRemaining()) {
                SSLEngineResult result = sslEngine.unwrap(peerNetData, peerAppData);
                handshakeStatus = result.getHandshakeStatus();
                switch(result.getStatus()) {
                    case OK:
                        if (handshakeStatus == NEED_TASK) {
                            handshakeStatus = runTasks(SelectionKey.OP_READ);
                            if (handshakeStatus == null) return 0;
                        }
                        if (isHandshaking && handshakeStatus == HandshakeStatus.FINISHED) {
                            return peerAppData.position() - pos;
                        }
                    case BUFFER_OVERFLOW:
                        peerAppData = expand(peerAppData, sslEngine.getSession().getApplicationBufferSize());
                        break;
                    case BUFFER_UNDERFLOW:
                        return 0;
                    case CLOSED:
                        if (peerAppData.position() == 0) {
                            shutdown();
                            return -1;
                        } else {
                            shutdown = true;
                            return 0;
                        }
                    default: throw new SSLException("Unexpected state!");
                }
            }
        } finally {
            peerNetData.compact();
        }

        return peerAppData.position() - pos;
    }

    private int wrap(ByteBuffer src) throws IOException, InterruptedException {
        int written = src.remaining();
        myNetData.compact();
        try {
            do {
                SSLEngineResult result = sslEngine.wrap(src, myNetData);
                handshakeStatus = result.getHandshakeStatus();
                switch(result.getStatus()) {
                    case OK:
                        if (handshakeStatus == NEED_TASK) {
                            handshakeStatus = runTasks(SelectionKey.OP_READ);
                            if (handshakeStatus == null) return 0;
                        }
                        break;
                    case BUFFER_OVERFLOW:
                        int size = (src.remaining() * 2 > sslEngine.getSession().getApplicationBufferSize()) ?
                                                                    src.remaining() * 2 :
                                                                    sslEngine.getSession().getApplicationBufferSize();
                        myNetData = expand(myNetData, size);
                        break;
                    case CLOSED:
                        shutdown();
                        throw new IOException("Write error received Status.CLOSED");
                    default: throw new SSLException("Unexpected state!");
                }
            } while (src.hasRemaining());
        } finally {
            myNetData.flip();
        }
        return written;
    }

    private long writeRaw(ByteBuffer out) throws IOException {
        try {
            if (out.hasRemaining()) {
                return underlying.write( (simulateSlowNetwork) ? writeTwo(out) : out );
            } else {
                return 0;
            }
        } catch (IOException e) {
            sslEngine.closeOutbound();
            shutdown = true;
            throw e;
        }
    }

    private ByteBuffer writeTwo(ByteBuffer i) {
        ByteBuffer o = ByteBuffer.allocate(2);
        int rem = i.limit() - i.position();
        if (rem > o.capacity()) rem = o.capacity();
        int c = 0;
        while (c < rem) {
            o.put(i.get());
            c += 1;
        }
        o.flip();
        return o;
    }

    private HandshakeStatus runTasks(int ops) throws IOException, InterruptedException {
        boolean reInitialize;
        switch (initialized) {
            case 0:
                initialized = ops;
                reInitialize = true;
                break;
            default:
                reInitialize = false;
        }

        Runnable runnable = sslEngine.getDelegatedTask();
        if (!blocking && selectionKey != null) {
            if (runnable != null) {
                executor.execute(new SSLTasker(runnable));
            }
            return null;
        } else {
            while (runnable != null) {
                runnable.run();
                runnable = sslEngine.getDelegatedTask();
            }
            if (reInitialize) {
                handshakeInBlockMode(ops);
            }
            return sslEngine.getHandshakeStatus();
        }
    }

    private ByteBuffer expand(ByteBuffer src, int ensureSize) {
        if (src.remaining() < ensureSize) {
            ByteBuffer newBuffer = ByteBuffer.allocate(src.capacity() + ensureSize);
            if (src.position() > 0) {
                src.flip();
                newBuffer.put(src);
            }
            return newBuffer;
        } else {
            return src;
        }
    }

    private int readFromPeerData(ByteBuffer dest) {
        peerAppData.flip();
        try {
            int remaining = peerAppData.remaining();
            if (remaining > 0) {
                if (remaining > dest.remaining()) {
                    remaining = dest.remaining();
                }
                int i = 0;
                while (i < remaining) {
                    dest.put(peerAppData.get());
                    i = i + 1;
                }
            }
            return remaining;
        } finally {
            peerAppData.compact();
        }
    }

    public SocketChannel bind(SocketAddress local) throws IOException {
        return underlying.bind(local);
    }

    public SocketChannel shutdownInput() {
        return shutdownInput();
    }

    public <T> SocketChannel setOption(SocketOption<T> name, T value) throws IOException {
        return underlying.setOption(name, value);
    }

    public <T> T getOption(SocketOption<T> name) throws IOException {
        return underlying.getOption(name);
    }

    public SocketAddress getRemoteAddress() throws IOException {
        return underlying.getRemoteAddress();
    }

    public SocketChannel shutdownOutput() throws IOException {
        return underlying.shutdownOutput();
    }

    public SocketAddress getLocalAddress() throws IOException {
        return underlying.getLocalAddress();
    }

    public Set<SocketOption<?>> supportedOptions() {
        return underlying.supportedOptions();
    }

    private class SSLTasker implements Runnable {
        private Runnable runnable;

        private SSLTasker(Runnable runnable) {
            this.runnable = runnable;
            selectionKey.interestOps(0);
        }

        public void run() {
            try {
                runnable.run();
                synchronized(outer) {
                    handshakeStatus = sslEngine.getHandshakeStatus();
                    switch(handshakeStatus) {
                        case NEED_WRAP:
                            selectionKey.interestOps(SelectionKey.OP_WRITE);
                            break;
                        case NEED_UNWRAP:
                            if (peerNetData.position() > 0) {
                                int init = outer.handshake(SelectionKey.OP_READ, selectionKey);
                                if (init == 0) {
                                    selectionKey.interestOps(SelectionKey.OP_READ);
                                } else if (init != runningTasks) {
                                    selectionKey.interestOps(init);
                                }
                            } else {
                                selectionKey.interestOps(SelectionKey.OP_READ);
                            }
                        case NEED_TASK:
                            Runnable runnable = sslEngine.getDelegatedTask();
                            if (runnable != null) {
                                executor.execute(new SSLTasker(runnable));
                                handshakeStatus = null;
                            }
                            return;
                        default: throw new SSLException("unexpected handshakeStatus: " + handshakeStatus);
                    }
                    selectionKey.selector().wakeup();
                }
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            } catch (SSLException e) {
                log.error(e.getMessage(), e);
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
