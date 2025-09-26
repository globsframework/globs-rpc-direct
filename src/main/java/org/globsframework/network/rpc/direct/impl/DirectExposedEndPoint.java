package org.globsframework.network.rpc.direct.impl;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.cache.DefaultGlobsCache;
import org.globsframework.core.model.cache.GlobsCache;
import org.globsframework.core.utils.serialization.*;
import org.globsframework.network.rpc.direct.ExposedEndPoint;
import org.globsframework.serialisation.BinReader;
import org.globsframework.serialisation.BinReaderFactory;
import org.globsframework.serialisation.BinWriter;
import org.globsframework.serialisation.BinWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

public class DirectExposedEndPoint implements ExposedEndPoint {
    private static final Logger log = LoggerFactory.getLogger(DirectExposedEndPoint.class);
    private final BinWriterFactory binWriterFactory;
        private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
//    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final ExecutorService connectionExecutorService = Executors.newSingleThreadExecutor();
    private final String host;
    private int port;
    private ServerSocket serverSocket;
    private volatile boolean running = false;
    private final Map<String, ReceivedWithType> receiver = new ConcurrentHashMap<>();
    private final BinReaderFactory binReaderFactory;
    private final List<MessageReader> messageReaders = new ArrayList<>();

    record ReceivedWithType(Receiver receiver, GlobType type) {

    }

    public DirectExposedEndPoint(String host, int port) {
        this.host = host;
        this.port = port;
        binReaderFactory = BinReaderFactory.create();
        binWriterFactory = BinWriterFactory.create();
        init();
    }

    public void init() {
        try {
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(host, port));
            if (port == 0) {
                port = serverSocket.getLocalPort();
            }
            running = true;
            connectionExecutorService.submit(this::processConnections);
        } catch (IOException e) {
            final String msg = "Failed to initialize server";
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    public void addReceiver(String path, Receiver receiver, GlobType receivedType) {
        this.receiver.put(path, new ReceivedWithType(receiver, receivedType));
    }

    private void processConnections() {
        try {
            while (running) {
                final Socket socket = serverSocket.accept();
                MessageReader messageReader =
                        new MessageReader(socket, binReaderFactory, binWriterFactory, receiver);
                executorService.submit(messageReader::run);
                messageReaders.add(messageReader);
            }
        } catch (IOException e) {
            final String msg = "Error processing connections";
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        } finally {
            log.info("Closing server socket.");
        }
    }

    public void shutdown() {
        running = false;
        for (MessageReader messageReader : messageReaders) {
            messageReader.shutdown();
        }
    }

    static class MessageReader {
        private final Map<String, ReceivedWithType> receivers;
        private final Socket socket;
        private final BinReader globBinReader;
        private final BinWriter globBinWriter;
        private final GlobsCache globsCache;
        private final SerializedInput serializationInput;
        private final ByteBufferSerializationOutput serializationOutput;
        private int requestId = 0;
        private volatile boolean shutdown = false;

        public MessageReader(Socket socket, BinReaderFactory binReader, BinWriterFactory binWriter, Map<String, ReceivedWithType> receivers) throws IOException {
            this.socket = socket;
            InputStream inputStream = socket.getInputStream();
            OutputStream outputStream = socket.getOutputStream();
            this.receivers = receivers;
            serializationInput = new DefaultSerializationInput(new BufferedInputStream(inputStream));
            serializationOutput = new ByteBufferSerializationOutput(outputStream);
            globsCache = new DefaultGlobsCache(100);
            globBinReader = binReader.createGlobBinReader(globType -> globsCache.newGlob(globType, requestId), serializationInput);
            this.globBinWriter = binWriter.create(serializationOutput);
        }

        void run() {
            try {
                while (!shutdown) {
                    requestId++;
                    if (requestId == Integer.MAX_VALUE) {
                        requestId = 1;
                    }
                    int callRequestId = requestId;
                    final long order = serializationInput.readNotNullLong();
                    final String path = serializationInput.readUtf8String();
                    final ReceivedWithType receiver = this.receivers.get(path);
                    if (receiver == null) {
                        log.warn("No receiver found for path {}", path);
                        globBinReader.read(null); // read the buffer content
                        synchronized (this) {
                            serializationOutput.write(order);
                            globBinWriter.write(((Glob) null));
                            serializationOutput.flush();
                        }
                    } else {
                        final Glob receivedMessage = globBinReader.read(receiver.type).orElse(null);
                        // put GlobInstantiator in Scoped Value ??
                        final CompletableFuture<Glob> receive = receiver.receiver.receive(receivedMessage,
                                globType -> globsCache.newGlob(globType, callRequestId));
                        receive.whenComplete(new ResponseHandler(order, receivedMessage, callRequestId));
                    }
                }
            } catch (Throwable e) {
                log.error("Error reading of writing glob. Closing connection", e);
                try {
                    socket.close();
                } catch (IOException ex) {
                    log.error("Error closing socket", ex);
                }
                throw new RuntimeException(e);
            }
            log.info("Closing connection.");
        }

        public void shutdown() {
            shutdown = true;
            try {
                socket.close();
            } catch (IOException e) {
            }
        }

        private class ResponseHandler implements BiConsumer<Glob, Throwable> {
            private final long order;
            private final Glob receivedMessage;
            private final int callRequestId;

            public ResponseHandler(long order, Glob receivedMessage, int callRequestId) {
                this.order = order;
                this.receivedMessage = receivedMessage;
                this.callRequestId = callRequestId;
            }

            @Override
            public void accept(Glob glob, Throwable throwable) {
                synchronized (MessageReader.this) {
                    try {
                        if (throwable != null || glob == null) {
                            serializationOutput.write(order);
                            globBinWriter.write(((Glob) null));
                            serializationOutput.flush();
                        } else {
                            serializationOutput.write(order);
                            globBinWriter.write(glob);
                            serializationOutput.flush();
                            if (receivedMessage != null) {
                                globsCache.release(receivedMessage, callRequestId);
                            }
                            globsCache.release(glob, callRequestId);
                        }
                    } catch (Exception e) {
                        // ignored : the input is closed so will already leave the loop
                    }
                }
            }
        }
    }
}
