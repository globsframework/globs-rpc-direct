package org.globsframework.network.exchange.impl.single;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.model.Glob;
import org.globsframework.core.utils.serialization.BufferInputStreamWithLimit;
import org.globsframework.core.utils.serialization.ByteBufferSerializationOutput;
import org.globsframework.core.utils.serialization.DefaultSerializationInput;
import org.globsframework.network.exchange.Exchange;
import org.globsframework.network.exchange.GlobSingleClient;
import org.globsframework.network.exchange.impl.CommandId;
import org.globsframework.network.exchange.impl.DataSerialisationUtils;
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
import java.net.Socket;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class GlobSingleClientImpl implements GlobSingleClient {
    private static final Logger log = LoggerFactory.getLogger(GlobSingleClientImpl.class);
    private final String host;
    private final int port;
    private final Socket socket;
    private final DefaultSerializationInput serializedInput;
    private final BinReader globBinReader;
    private final ByteBufferSerializationOutput serializedOutput;
    private final BinWriter binWriter;
    private final ExecutorService executorService;
    private final AtomicLong lastStreamId;
    private final OutputStream socketOutputStream;
    private final BufferInputStreamWithLimit bufferInputStreamWithLimit;
    private Map<Long, StreamInfo> requests = new ConcurrentHashMap<>();
    private int maxMessageSize;
    private volatile boolean close = false;

    public GlobSingleClientImpl(String host, int port) throws IOException {
        this.maxMessageSize = 10 * 1024;
        lastStreamId = new AtomicLong(0);
        this.host = host;
        this.port = port;
        BinReaderFactory binReaderFactory = BinReaderFactory.create();
        BinWriterFactory binWriterFactory = BinWriterFactory.create();
        socket = new Socket();
        socket.setTcpNoDelay(true);
        socket.connect(new InetSocketAddress(host, port));
        final InputStream inputStream = socket.getInputStream();
        socketOutputStream = socket.getOutputStream();
        bufferInputStreamWithLimit = new BufferInputStreamWithLimit(inputStream);
        serializedInput = new DefaultSerializationInput(bufferInputStreamWithLimit);
        globBinReader = binReaderFactory.createGlobBinReader(serializedInput);
        serializedOutput = new ByteBufferSerializationOutput(new byte[maxMessageSize]);
        binWriter = binWriterFactory.create(serializedOutput);
        executorService = Executors.newSingleThreadExecutor();
        executorService.execute(this::read);
    }

    private void read() {
        try {
            serializedOutput.write(1); //max Read/Write version
            socketOutputStream.write(serializedOutput.getBuffer(), 0, serializedOutput.position());
            int maxReadWriteVersion = serializedInput.readNotNullInt();

            while (!close) {
                final long streamId = serializedInput.readNotNullLong();
                if (streamId < 0) {
                    final int code = serializedInput.readNotNullInt();
                    if (code == CommandId.CLOSE_STREAM.id) { // close
                        final StreamInfo streamInfo = requests.remove(-streamId);
                        if (streamInfo != null) {
                            streamInfo.dataReceiver().close();
                        }
                    } else if (code == CommandId.ACK.id) { // ack
                        final int requestId = serializedInput.readNotNullInt();
                        final StreamInfo streamInfo = requests.get(-streamId);
                        if (streamInfo == null) {
                            // may be closed.
                        } else {
                            final CompletableFuture<Boolean> remove = streamInfo.results().remove(requestId);
                            if (remove != null) {
                                remove.complete(true);
                            } else {
                                throw new RuntimeException("BUG : No result found for request id " + requestId);
                            }
                        }
                    } else if (code == CommandId.ERROR_APPLICATIVE.id || code == CommandId.ERROR_DESERIALISATION.id) {
                        final int requestId = serializedInput.readNotNullInt();
                        String errorMessage = serializedInput.readUtf8String();
                        final StreamInfo streamInfo = requests.get(-streamId);
                        if (streamInfo == null) {
                            // may be closed.
                        } else {
                            final CompletableFuture<Boolean> remove = streamInfo.results().remove(requestId);
                            if (remove != null) {
                                remove.completeExceptionally(new RuntimeException(errorMessage));
                            } else {
                                throw new RuntimeException("BUG : No result found for request id " + requestId);
                            }
                        }
                    }
                } else {
                    final StreamInfo responseInfo = requests.get(streamId);
                    final int requestId = serializedInput.readNotNullInt();
                    final int dataSize = serializedInput.readNotNullInt();
                    bufferInputStreamWithLimit.limit(dataSize);
                    if (responseInfo == null) {
                        globBinReader.read(null);
                        if (!bufferInputStreamWithLimit.readToLimit()) {
                            throw new RuntimeException("Bug : stream not read to limit.");
                        }
                    } else {
                        Optional<Glob> read;
                        try {
                            read = globBinReader.read(responseInfo.receiveType());
                        } catch (Exception e) {
                            bufferInputStreamWithLimit.readToLimit();
                            log.error("Fail to read type " + responseInfo.receiveType().getName() + " check the type or serialization.");
                            break;
                        }
                        if (!bufferInputStreamWithLimit.readToLimit()) {
                            throw new RuntimeException("Bug : stream not read to limit.");
                        }
                        try {
                            responseInfo.dataReceiver().receive(read.orElse(null));
                        } catch (Exception e) {
                            log.error("Error in receiver.", e);
                        }
                    }
                }
            }
        } catch (Throwable throwable) {
            log.error("GlobClient read error", throwable);
        }
    }

    record StreamInfo(long streamId, DataReceiver dataReceiver, GlobType receiveType,
                      Map<Integer, CompletableFuture<Boolean>> results) {
    }

    @Override
    public Exchange connect(String path, DataReceiver dataReceiver, GlobType receiveType, AckOption ackOption, SendOption sendOption) {
        final long streamId = this.lastStreamId.incrementAndGet();
        Map<Integer, CompletableFuture<Boolean>> results = ackOption == AckOption.NO_ACK ? Map.of() : new ConcurrentHashMap<>();
        requests.put(streamId, new StreamInfo(streamId, dataReceiver, receiveType, results));
        synchronized (serializedOutput) {
            serializedOutput.reset();
            serializedOutput.write(-streamId);
            serializedOutput.write(CommandId.NEW.id);
            serializedOutput.writeUtf8String(path);
            serializedOutput.write(ackOption.opt);
            try {
                socketOutputStream.write(serializedOutput.getBuffer(), 0, serializedOutput.position());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return new SingleExchangeClientSide(streamId, results, ackOption);
    }

    @Override
    public void close() {
        executorService.shutdown();
        close = true;
        try {
            socket.close();
        } catch (IOException e) {
        }
    }

    private class SingleExchangeClientSide implements Exchange {
        private final long streamId;
        private final Map<Integer, CompletableFuture<Boolean>> results;
        private final AckOption ackOption;
        private final AtomicInteger lastRequestId = new AtomicInteger(0);

        public SingleExchangeClientSide(long streamId, Map<Integer, CompletableFuture<Boolean>> results, AckOption ackOption) {
            this.streamId = streamId;
            this.results = results;
            this.ackOption = ackOption;
        }

        @Override
        public CompletableFuture<Boolean> send(Glob data) {
            final int requestId = lastRequestId.incrementAndGet();
            final CompletableFuture<Boolean> value;
            if (ackOption != AckOption.NO_ACK) {
                value = new CompletableFuture<>();
                results.put(requestId, value);
            } else {
                value = CompletableFuture.completedFuture(true);
            }
            synchronized (serializedOutput) {
                DataSerialisationUtils.serializeMessageData(data, streamId, requestId, serializedOutput, binWriter);
                try {
                    socketOutputStream.write(serializedOutput.getBuffer(), 0, serializedOutput.position());
                } catch (IOException e) {
                    return CompletableFuture.failedFuture(e);
                }
            }
            return value;
        }

        @Override
        public void close() {
            requests.remove(streamId);
            synchronized (serializedOutput) {
                serializedOutput.reset();
                serializedOutput.write(-streamId);
                serializedOutput.write(CommandId.CLOSE_STREAM.id);
                try {
                    socketOutputStream.write(serializedOutput.getBuffer(), 0, serializedOutput.position());
                } catch (IOException e) {
                }
            }
            if (!results.isEmpty()) {
                Exception exception = new RuntimeException("Connection closed");
                results.forEach((key, value) -> {
                    value.completeExceptionally(exception);
                });
            }
        }
    }
}
