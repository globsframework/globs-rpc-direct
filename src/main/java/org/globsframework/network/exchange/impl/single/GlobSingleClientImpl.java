package org.globsframework.network.exchange.impl.single;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.model.Glob;
import org.globsframework.core.utils.serialization.ByteBufferSerializationOutput;
import org.globsframework.core.utils.serialization.DefaultSerializationInput;
import org.globsframework.network.exchange.Exchange;
import org.globsframework.network.exchange.GlobSingleClient;
import org.globsframework.network.exchange.impl.CommandId;
import org.globsframework.serialisation.BinReader;
import org.globsframework.serialisation.BinReaderFactory;
import org.globsframework.serialisation.BinWriter;
import org.globsframework.serialisation.BinWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
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
    private final Executor executorService;
    private final AtomicLong streamId;
    private final OutputStream socketOutputStream;
    private Map<Long, StreamInfo> requests = new ConcurrentHashMap<>();

    public GlobSingleClientImpl(String host, int port) throws IOException {
        streamId = new AtomicLong(0);
        this.host = host;
        this.port = port;
        BinReaderFactory binReaderFactory = BinReaderFactory.create();
        BinWriterFactory binWriterFactory = BinWriterFactory.create();
        socket = new Socket();
        socket.connect(new InetSocketAddress(host, port));
        final InputStream inputStream = socket.getInputStream();
        socketOutputStream = socket.getOutputStream();
        serializedInput = new DefaultSerializationInput(new BufferedInputStream(inputStream));
        globBinReader = binReaderFactory.createGlobBinReader(serializedInput);
        serializedOutput = new ByteBufferSerializationOutput(socketOutputStream);
        binWriter = binWriterFactory.create(serializedOutput);
        executorService = Executors.newSingleThreadExecutor();
        executorService.execute(this::read);
    }

    private void read() {
        try {
            serializedOutput.write(1); //max Read/Write version
            int maxReadWriteVersion = serializedInput.readNotNullInt();

            while (true) {
                final long streamId = serializedInput.readNotNullLong();
                if (streamId < 0) {
                    final int code = serializedInput.readNotNullInt();
                    if (code == CommandId.CLOSE.id) { // close
                        final StreamInfo streamInfo = requests.remove(-streamId);
                        if (streamInfo != null) {
                            streamInfo.dataReceiver().close();
                        }
                    } else if (code == CommandId.ACK.id) { // ack
                        final int requestId = serializedInput.readNotNullInt();
                        final StreamInfo streamInfo = requests.get(-streamId);
                        if (streamInfo == null) {
                            // may be closed.
                        }else {
                            final CompletableFuture<Boolean> remove = streamInfo.results().remove(requestId);
                            if (remove != null) {
                                remove.complete(true);
                            } else {
                                throw new RuntimeException("BUG : No result found for request id " + requestId);
                            }
                        }
                    }
                }
                else {
                    final StreamInfo responseInfo = requests.get(streamId);
                    final int requestId = serializedInput.readNotNullInt();
                    if (responseInfo == null) {
                        globBinReader.read(null);
                    } else {
                        final Optional<Glob> read = globBinReader.read(responseInfo.receiveType());
                        responseInfo.dataReceiver().receive(read.orElse(null));
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
    public Exchange connect(String path, DataReceiver dataReceiver, GlobType receiveType, Option option) {
        final long current = streamId.incrementAndGet();
        Map<Integer, CompletableFuture<Boolean>> results = option == Option.NO_ACK ? Map.of() : new ConcurrentHashMap<>();
        requests.put(current, new StreamInfo(current, dataReceiver, receiveType, results));
        synchronized (serializedOutput) {
            serializedOutput.write(-current);
            serializedOutput.write(CommandId.NEW.id);
            serializedOutput.writeUtf8String(path);
            serializedOutput.write(option.opt);
        }
        return new SingleExchangeClientSide(current, results, option);
    }

    private class SingleExchangeClientSide implements Exchange {
        private final long current;
        private final Map<Integer, CompletableFuture<Boolean>> results;
        private final Option option;
        private final AtomicInteger seq = new AtomicInteger(0);

        public SingleExchangeClientSide(long current, Map<Integer, CompletableFuture<Boolean>> results, Option option) {
            this.current = current;
            this.results = results;
            this.option = option;
        }

        @Override
        public CompletableFuture<Boolean> send(Glob data) {
            final int lSeq = seq.incrementAndGet();
            final CompletableFuture<Boolean> value;
            if (option != Option.NO_ACK) {
                value = new CompletableFuture<>();
                results.put(lSeq, value);
            }
            else {
                value = CompletableFuture.completedFuture(true);
            }
            synchronized (serializedOutput) {
                serializedOutput.write(current);
                serializedOutput.write(lSeq);
                binWriter.write(data);
                serializedOutput.flush();
            }
//            serializedOutput.flush();
            return value;
        }

        @Override
        public void close() {
            requests.remove(current);
            serializedOutput.write(-current);
            serializedOutput.write(1);
            if (!results.isEmpty()) {
                Exception exception = new RuntimeException("Connection closed");
                results.forEach((key, value) -> {
                    value.completeExceptionally(exception);
                });
            }
        }
    }
}
