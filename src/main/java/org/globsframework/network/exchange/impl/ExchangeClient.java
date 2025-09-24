package org.globsframework.network.exchange.impl;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.model.Glob;
import org.globsframework.core.utils.serialization.DefaultSerializationInput;
import org.globsframework.core.utils.serialization.DefaultSerializationOutput;
import org.globsframework.core.utils.serialization.SerializedInput;
import org.globsframework.core.utils.serialization.SerializedOutput;
import org.globsframework.serialisation.BinReader;
import org.globsframework.serialisation.BinReaderFactory;
import org.globsframework.serialisation.BinWriter;
import org.globsframework.serialisation.BinWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class ExchangeClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ExchangeClient.class);
    private final Map<Long, ResponseInfo> requests = new ConcurrentHashMap<>();
    private final String host;
    private final int port;
    private final Socket socket;
    private final BinReader globBinReader;
    private final BinWriter binWriter;
    private final BufferedOutputStream bufferedOutputStream;
    private final SerializedOutput serializedOutput;
    private final SerializedInput serializedInput;
    private final ExecutorService executorService;
    private AtomicLong writeOrder = new AtomicLong(0);
    private volatile boolean shutdown;

    public ExchangeClient(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        BinReaderFactory binReaderFactory = BinReaderFactory.create();
        BinWriterFactory binWriterFactory = BinWriterFactory.create();
        socket = new Socket();
        socket.setTcpNoDelay(true);
        socket.connect(new InetSocketAddress(host, port));
        final InputStream inputStream = socket.getInputStream();
        final OutputStream outputStream = socket.getOutputStream();
        serializedInput = new DefaultSerializationInput(new BufferedInputStream(inputStream));
        globBinReader = binReaderFactory.createGlobBinReader(serializedInput);
        bufferedOutputStream = new BufferedOutputStream(outputStream);
        serializedOutput = new DefaultSerializationOutput(bufferedOutputStream);
        binWriter = binWriterFactory.create(serializedOutput);
        executorService = Executors.newSingleThreadExecutor();
        executorService.execute(this::read);
    }

    record ResponseInfo(CompletableFuture<Glob> result, GlobType resultType) {
    }

    public CompletableFuture<Glob> request(String path, Glob data, GlobType resultType) throws IOException {
        long order = writeOrder.incrementAndGet();
        CompletableFuture<Glob> result = new CompletableFuture<>();
        requests.put(order, new ResponseInfo(result, resultType));
        synchronized (binWriter) {
            serializedOutput.write(order);
            serializedOutput.writeUtf8String(path);
            binWriter.write(data);
        }
        bufferedOutputStream.flush(); //internally synchronized
        return result;
    }

    private void read() {
        try {
            while (true) {
                final long readOrder = serializedInput.readNotNullLong();
                final ResponseInfo responseInfo = requests.remove(readOrder);
                if (responseInfo == null) {
                    throw new RuntimeException("Bug : no response found for order " + readOrder);
                } else {
                    responseInfo.result.complete(globBinReader.read(responseInfo.resultType).orElse(null));
                }
            }
        } catch (Throwable throwable) {
            for (Map.Entry<Long, ResponseInfo> longResponseInfoEntry : requests.entrySet()) {
                longResponseInfoEntry.getValue().result.completeExceptionally(throwable);
            }
            if (shutdown) {
                log.info("Shutting down");
                return;
            }
            log.error("Error in read", throwable);
            try {
                socket.close();
            } catch (IOException e) {
                log.error("Error closing socket", e);
                throw new RuntimeException(e);
            }
        }
    }

    public boolean waitEndOfPendingRequests(Duration maxWait) {
        long startAt = System.currentTimeMillis();
        while (requests.size() > 0 && startAt + maxWait.toMillis() > System.currentTimeMillis()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        return requests.isEmpty();
    }

    public void close() throws IOException {
        shutdown = true;
        try {
            socket.close();
        } finally {
            executorService.shutdown();
        }
    }
}
