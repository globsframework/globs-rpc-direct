package org.globsframework.rpc.direct.impl;

import org.globsframework.core.model.Glob;
import org.globsframework.core.utils.serialization.*;
import org.globsframework.rpc.direct.ExposedEndPoint;
import org.globsframework.serialisation.BinReader;
import org.globsframework.serialisation.BinReaderFactory;
import org.globsframework.serialisation.BinWriter;
import org.globsframework.serialisation.BinWriterFactory;
import org.globsframework.serialisation.field.reader.GlobTypeIndexResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DirectExposedEndPoint implements ExposedEndPoint {
    private static final Logger log = LoggerFactory.getLogger(DirectExposedEndPoint.class);
    private final BinWriterFactory binWriterFactory;
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final String host;
    private int port;
    private ServerSocket serverSocket;
    private volatile boolean running = false;
    private final Map<String, Receiver> receiver = new ConcurrentHashMap<>();
    private final BinReaderFactory binReaderFactory;
    private final List<MessageReader> messageReaders = new ArrayList<>();

    public DirectExposedEndPoint(String host, int port, GlobTypeIndexResolver globTypeResolver) {
        this.host = host;
        this.port = port;
        binReaderFactory = BinReaderFactory.create(globTypeResolver);
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
            executorService.submit(this::processConnections);
        } catch (IOException e) {
            final String msg = "Failed to initialize server";
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    public void addReceiver(String path, Receiver receiver) {
        this.receiver.put(path, receiver);
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
        private final Map<String, Receiver> receivers;
        private final Socket socket;
        private final BufferedOutputStream bufferedOutputStream;
        private final BinReader globBinReader;
        private final BinWriter globBinWriter;
        private final GlobsCache globsCache;
        private final SerializedInput serializationInput;
        private final SerializedOutput serializationOutput;
        private volatile boolean shutdown = false;

        public MessageReader(Socket socket, BinReaderFactory binReader, BinWriterFactory binWriter, Map<String, Receiver> receivers) throws IOException {
            this.socket = socket;
            InputStream inputStream = socket.getInputStream();
            OutputStream outputStream = socket.getOutputStream();
            this.receivers = receivers;
            serializationInput = new DefaultSerializationInput(new BufferedInputStream(inputStream));
            bufferedOutputStream = new BufferedOutputStream(outputStream);
            serializationOutput = new DefaultSerializationOutput(bufferedOutputStream);
            globsCache = new GlobsCache();
            globBinReader = binReader.createGlobBinReader(globsCache, serializationInput);
            this.globBinWriter = binWriter.create(serializationOutput);
        }

        void run() {
            try {
                while (!shutdown) {
                    final long order = serializationInput.readNotNullLong();
                    final String path = serializationInput.readUtf8String();
                    Glob receivedMessage = globBinReader.read().orElse(null);
                    final Receiver receiver = this.receivers.get(path);
                    final Glob receive;
                    if (receiver != null) {
                        receive = receiver.receive(receivedMessage);
                    }else {
                        receive = null;
                    }
                    serializationOutput.write(order);
                    globBinWriter.write(receive);
                    bufferedOutputStream.flush();
                    if (receivedMessage != null) {
                        globsCache.release(receivedMessage);
                    }
//                    if (receive != null) {
//                        globsCache.release(receive);
//                    }
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
    }
}
