package org.globsframework.network.exchange.impl;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.model.Glob;
import org.globsframework.core.utils.serialization.BufferInputStreamWithLimit;
import org.globsframework.core.utils.serialization.ByteBufferSerializationOutput;
import org.globsframework.core.utils.serialization.DefaultSerializationInput;
import org.globsframework.core.utils.serialization.SerializedInput;
import org.globsframework.network.exchange.GlobSingleClient;
import org.globsframework.network.exchange.GlobsServer;
import org.globsframework.serialisation.BinReader;
import org.globsframework.serialisation.BinReaderFactory;
import org.globsframework.serialisation.BinWriter;
import org.globsframework.serialisation.BinWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExchangeGlobsServer implements GlobsServer {
    private static final Logger log = LoggerFactory.getLogger(ExchangeGlobsServer.class);
    private final String host;
    private final BinReaderFactory binReaderFactory;
    private final BinWriterFactory binWriterFactory;
    private int port;
    private final Map<String, ClientInfo> clients = new ConcurrentHashMap<>();
    private final ExecutorService connectionExecutorService = Executors.newSingleThreadExecutor();
    private final Executor executorService;
    private final List<MessageReader> readers = new ArrayList<>();
    private ServerSocket serverSocket;
    private boolean running;

    public ExchangeGlobsServer(String host, int port, Executor executor) {
        this.host = host;
        this.port = port;
        binReaderFactory = BinReaderFactory.create();
        binWriterFactory = BinWriterFactory.create();
        executorService = executor;
    }

    public static class Builder {
        private final String host;
        private final int port;
        private Executor executor;

        private Builder(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public static Builder create(String host, int port) {
            return new Builder(host, port);
        }

        public Builder with(Executor executor) {
            this.executor = executor;
            return this;
        }

        public ExchangeGlobsServer build() {
            final ExchangeGlobsServer exchangeGlobsServer =
                    new ExchangeGlobsServer(host, port, executor == null ? Executors.newSingleThreadExecutor() : executor);
            exchangeGlobsServer.init();
            return exchangeGlobsServer;
        }
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
            throw new RuntimeException(msg, e);
        }
    }

    private void processConnections() {
        try {
            while (running) {
                final Socket socket = serverSocket.accept();
                MessageReader messageReader =
                        new MessageReader(socket, binReaderFactory, binWriterFactory, clients, reader -> {
                            synchronized (this) {
                                readers.add(reader);
                            }
                        });
                executorService.execute(messageReader::run);
                readers.add(messageReader);
            }
        } catch (IOException e) {
            final String msg = "Error processing connections";
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        } finally {
            log.info("Closing server socket.");
        }
    }

    record ClientInfo(OnClient onClient, GlobType receiveType) {
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public void shutdown() {
        try {
            serverSocket.close();
        } catch (IOException e) {
        }
        connectionExecutorService.shutdown();
        synchronized (this) {
            for (MessageReader reader : readers) {
                reader.shutdown();
            }
        }
    }

    @Override
    synchronized public void onPath(String path, OnClient onClient, GlobType receiveType) {
        if (clients.containsKey(path)) {
            throw new GlobSingleClient.AlreadyRegisteredException(path);
        }
        clients.put(path, new ClientInfo(onClient, receiveType));
    }

    record OnReceiverWithType(long l, Receiver onClient, MessageReader.OnDataServer onData, GlobType receiveType,
                              GlobSingleClient.Option opt) {
    }


    static class MessageReader {
        private final Map<String, ClientInfo> clientInfoMap;
        private final Map<Long, OnReceiverWithType> clients = new HashMap<>();
        private final Socket socket;
        private final OnClose onClose;
        private final BinReader globBinReader;
        private final BinWriter globBinWriter;
        //        private final GlobsCache globsCache;
        private final SerializedInput serializationInput;
        private final ByteBufferSerializationOutput serializationOutput;
        private final OutputStream socketOutputStream;
        private final BufferInputStreamWithLimit bufferedInputStream;
        private final int maxMessageSize;
        private int requestId = 0;
        private volatile boolean shutdown = false;

        public MessageReader(Socket socket, BinReaderFactory binReader, BinWriterFactory binWriter,
                             Map<String, ClientInfo> pathToClient, OnClose onClose) throws IOException {
            maxMessageSize = 10 * 1024;
            this.socket = socket;
            this.onClose = onClose;
            socket.setTcpNoDelay(true);
            InputStream inputStream = socket.getInputStream();
            socketOutputStream = socket.getOutputStream();
            this.clientInfoMap = pathToClient;
            this.bufferedInputStream = new BufferInputStreamWithLimit(inputStream);
            serializationInput = new DefaultSerializationInput(this.bufferedInputStream);
            serializationOutput = new ByteBufferSerializationOutput(new byte[maxMessageSize]);
//            globsCache = new DefaultGlobsCache(100);
//            globBinReader = binReader.createGlobBinReader(globType -> globsCache.newGlob(globType, requestId), serializationInput);
            globBinReader = binReader.createGlobBinReader(GlobType::instantiate, serializationInput);
            this.globBinWriter = binWriter.create(serializationOutput);
        }

        interface OnClose {
            void closed(MessageReader reader);
        }

        void run() {
            try {
                serializationOutput.reset();
                serializationOutput.write(1); // version
                socketOutputStream.write(serializationOutput.getBuffer(), 0, serializationOutput.position());
                int version = serializationInput.readNotNullInt();
                while (!shutdown) {
                    final long streamId = serializationInput.readNotNullLong();
                    if (streamId < 0) {
                        manageCommand(streamId);
                    } else {
                        final OnReceiverWithType onClientWithType = clients.get(streamId);
                        requestId = serializationInput.readNotNullInt();
                        int dataSize = serializationInput.readNotNullInt();
                        bufferedInputStream.limit(dataSize);
                        if (onClientWithType != null) {
                            if (onClientWithType.opt == GlobSingleClient.Option.WITH_ACK_BEFORE_READ_DATA) {
                                synchronized (serializationOutput) {
                                    serializationOutput.reset();
                                    serializationOutput.write(-streamId);
                                    serializationOutput.write(CommandId.ACK.id);
                                    serializationOutput.write(requestId);
                                    socketOutputStream.write(serializationOutput.getBuffer(), 0, serializationOutput.position());
                                }
                            }
                            Optional<Glob> read = null;
                            try {
                                read = globBinReader.read(onClientWithType.receiveType());
                            } catch (
                                    Exception e) { // it can be an IOException that was lost. But that exception will be rethrown when reading the next data.
                                log.error("Error reading data " +
                                          onClientWithType.receiveType().getName() + " => check if the GlobType match.", e);
                                bufferedInputStream.readToLimit();
                            }
                            if (read != null) {
                                try {
                                    onClientWithType.onClient().receive(read.orElse(null));
                                    if (onClientWithType.opt == GlobSingleClient.Option.WITH_ACK_AFTER_CLIENT_CALL) {
                                        synchronized (serializationOutput) {
                                            serializationOutput.reset();
                                            serializationOutput.write(-streamId);
                                            serializationOutput.write(CommandId.ACK.id);
                                            serializationOutput.write(requestId);
                                            socketOutputStream.write(serializationOutput.getBuffer(), 0, serializationOutput.position());
                                        }
                                    }
                                } catch (Exception e) {
                                    log.error("Error in receiver.", e);
                                }
                            }
                        } else {
                            // closed to soon
                            globBinReader.read(null);
                        }
                        bufferedInputStream.resetLimit();
                    }
                }
            } catch (Throwable e) {
                try {
                    socket.close();
                } catch (IOException ex) {
                }
                throw new RuntimeException(e);
            } finally {
                onClose.closed(this);
            }
        }

        private void manageCommand(long streamId) {
            int code = serializationInput.readNotNullInt();
            if (code == CommandId.CLOSE.id) {
                final OnReceiverWithType remove = clients.remove(-streamId);
                if (remove != null) {
                    remove.onClient().closed();
                }
            } else if (code == CommandId.NEW.id) {
                String path = serializationInput.readUtf8String();
                final GlobSingleClient.Option opt = switch (serializationInput.readNotNullInt()) {
                    case 0 -> GlobSingleClient.Option.NO_ACK;
                    case 1 -> GlobSingleClient.Option.WITH_ACK_BEFORE_READ_DATA;
                    case 2 -> GlobSingleClient.Option.WITH_ACK_BEFORE_CLIENT_CALL;
                    case 3 -> GlobSingleClient.Option.WITH_ACK_AFTER_CLIENT_CALL;
                    default ->
                            throw new IllegalStateException("Unexpected value: " + serializationInput.readNotNullInt());
                };
                final ClientInfo clientInfo = this.clientInfoMap.get(path);
                if (clientInfo != null) {
                    final OnDataServer onData = new OnDataServer(-streamId);
                    final Receiver receiver = clientInfo.onClient.onNewClient(onData);
                    clients.put(-streamId, new OnReceiverWithType(-streamId, receiver, onData, clientInfo.receiveType, opt));
                }
            }
        }

        public void shutdown() {
            shutdown = true;
            try {
                socket.close();
            } catch (IOException e) {
            }
        }

        private class OnDataServer implements OnData {
            private final long streamId;
            private int requestId;

            public OnDataServer(long streamId) {
                this.streamId = streamId;
            }

            @Override
            public void onData(Glob data) {
                requestId++;
                synchronized (serializationOutput) {
                    DataSerialisationUtils.serializeMessageData(data, streamId, requestId, serializationOutput, globBinWriter);
                    try {
                        socketOutputStream.write(serializationOutput.getBuffer(), 0, serializationOutput.position());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            @Override
            public void close() {
                final OnReceiverWithType remove = clients.remove(streamId);
                if (remove == null) {
                    log.warn("Client " + streamId + " has already been closed");
                }
                synchronized (serializationOutput) {
                    serializationOutput.reset();
                    serializationOutput.write(-streamId);
                    serializationOutput.write(CommandId.CLOSE.id);
                    try {
                        socketOutputStream.write(serializationOutput.getBuffer(), 0, serializationOutput.position());
                    } catch (IOException e) {
                    }
                }
            }
        }
    }
}
