package org.globsframework.network.exchange.impl.multi;

import org.globsframework.core.model.Glob;
import org.globsframework.core.utils.serialization.NByteBufferSerializationInput;
import org.globsframework.network.exchange.impl.CommandId;
import org.globsframework.serialisation.BinReader;
import org.globsframework.serialisation.BinReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class EndPointServer implements SendData, NByteBufferSerializationInput.NextBuffer {
    private final static Logger log = LoggerFactory.getLogger(EndPointServer.class);
    private final GlobMultiClientImpl.ServerAddress serverAddress;
    private final GlobMultiClientImpl.AddPendingWrite addPendingWrite;
    private final ClientShare clientShare;
    private final RequestAccess requestAccess;
    private final NByteBufferSerializationInput serializedInput;
    private final BinReader globBinReader;
    private final SocketChannel channel;
    private final PendingWrite pendingWrite = new PendingWrite();
    private final Selector readSelector;
    private final ByteBuffer readByteBuffer;
    private final SelectionKey readSelectionKey;
    private final GlobMultiClientImpl.SetPendingWrite setPendingWrite;
    private final String clientUUID;
    private volatile boolean shutdown;

    public EndPointServer(GlobMultiClientImpl.ServerAddress serverAddress, GlobMultiClientImpl.AddPendingWrite addPendingWrite,
                          ClientShare clientShare, RequestAccess requestAccess, Executor executor, BinReaderFactory binReaderFactory) throws IOException {
        this.serverAddress = serverAddress;
        this.addPendingWrite = addPendingWrite;
        this.clientShare = clientShare;
        this.requestAccess = requestAccess;
        clientUUID = UUID.randomUUID().toString();
        channel = SocketChannel.open();
        channel.configureBlocking(false);
        channel.connect(new InetSocketAddress(serverAddress.host(), serverAddress.port()));
        channel.socket().setTcpNoDelay(true);

        Data data = clientShare.getFreeData();
        data.incWriter();
        data.serializedOutput.write(1);
        data.serializedOutput.writeUtf8String(clientUUID);
        data.complete(-1, -1);
        pendingWrite.add(data);
        setPendingWrite = this.addPendingWrite.add(channel, pendingWrite);
        readSelector = Selector.open();
        readSelectionKey = channel.register(readSelector, SelectionKey.OP_READ);
        readByteBuffer = clientShare.getFreeDirectBuffer();
        readByteBuffer.limit(0);
        serializedInput = new NByteBufferSerializationInput(readByteBuffer, this, null, 100);
        globBinReader = binReaderFactory.createGlobBinReader(serializedInput);
        executor.execute(this::read);
    }

    @Override
    public ByteBuffer refill(ByteBuffer byteBuffer) {
        while (true) {
            try {
                long start = System.nanoTime();
                final int select = readSelector.select();
                if (log.isDebugEnabled()) {
                    final long micros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
                    log.debug("refill unlock " + select + " " + ( micros > 10000 ? TimeUnit.MICROSECONDS.toMillis(micros) + "ms" :  micros + "us")  + "  " + clientUUID);
                }
                if (readSelectionKey.isReadable()) {
                    readByteBuffer.clear();
                    int read = channel.read(readByteBuffer);
                    if (read == -1) {
                        throw new RuntimeException("Connection closed");
                    }
                    readByteBuffer.flip();
                    if (readByteBuffer.hasRemaining()) {
                        return readByteBuffer;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public interface RequestAccess {
        void ack(long streamId, int requestId);

        void close(long streamId);

        DataReceivedInfo dataReceived(long streamId, int requestId);

        void error(long streamId, int requestId, String errorMessage);
    }

    private void read() {
        try {
            channel.finishConnect();
            setPendingWrite.set();
            int maxReadWriteVersion = serializedInput.readNotNullInt();
            clientShare.connectionOK(serverAddress, this);

            while (!shutdown) {
                final long streamId = serializedInput.readNotNullLong();
                if (streamId < 0) {
                    final int code = serializedInput.readNotNullInt();
                    if (log.isDebugEnabled()) {
                        log.debug("Received command " + code + " for stream " + streamId  + "  " + clientUUID);
                    }
                    if (code == CommandId.CLOSE_STREAM.id) {
                        requestAccess.close(-streamId);
                    } else if (code == CommandId.ACK.id) {
                        final int requestId = serializedInput.readNotNullInt();
                        requestAccess.ack(-streamId, requestId);
                    } else if (code == CommandId.ERROR_DESERIALISATION.id) {
                        final int requestId = serializedInput.readNotNullInt();
                        String errorMessage = serializedInput.readUtf8String();
                        requestAccess.error(-streamId, requestId, errorMessage);
                    } else if (code == CommandId.ERROR_APPLICATIVE.id) {
                        final int requestId = serializedInput.readNotNullInt();
                        String errorMessage = serializedInput.readUtf8String();
                        requestAccess.error(-streamId, requestId, errorMessage);
                    }
                } else {
                    final int requestId = serializedInput.readNotNullInt();
                    if (log.isDebugEnabled()) {
                        log.debug("Received request from " + serverAddress + " request " + requestId + " and stream " + streamId + "  " + clientUUID);
                    }
                    final DataReceivedInfo responseInfo = requestAccess.dataReceived(streamId, requestId);
                    int dataSize = serializedInput.readNotNullInt();
                    serializedInput.limit(dataSize);
                    if (responseInfo == null) {
                        globBinReader.read(null);
                    } else {
                        Optional<Glob> read = null;
                        try {
                            read = globBinReader.read(responseInfo.receiveType());
                        } catch (Exception e) {
                            log.error("Error reading data " + serverAddress + " for type " + responseInfo.receiveType().getName()  + "  " + clientUUID, e);
                            serializedInput.readToLimit();
                        }
                        if (read != null) {
                            try {
                                if (log.isDebugEnabled()) {
                                    log.debug("Data read " + serverAddress + ", propagate request " + requestId + " for stream " + streamId  + "  " + clientUUID);
                                }
                                responseInfo.dataReceiver().receive(read.orElse(null));
                            } catch (Exception e) {
                                log.error("Error in receiver.", e);
                            }
                        }
                    }
                    serializedInput.resetLimit();
                }
            }
            log.info("Shutting down " + serverAddress + "  " + clientUUID);
        } catch (Throwable throwable) {
            if (shutdown) {
                log.info("Shutting down " + serverAddress + "  " + clientUUID);
                return;
            }
            clientShare.connectionLost(serverAddress);
            log.error("GlobClient read error", throwable);
        }
        finally {
            clientShare.releaseDirectBuffer(readByteBuffer);
        }
    }

    public boolean send(Data data) {
        if (!channel.isConnected()){
            return false;
        }
        data.incWriter();
        if (!pendingWrite.addWriteIfNeeded(data)) {
            try {
                data.byteBuffer.mark();
                if (log.isDebugEnabled()) {
                    log.debug("Send data to " +  serverAddress + " on stream " + data.streamId + " and request " + data.requestId  + "  " + clientUUID);
                }
                channel.write(data.byteBuffer);
            } catch (IOException e) {
                log.error("Error writing data to " + serverAddress + "  " + clientUUID, e);
                data.byteBuffer.reset();
                data.release();
                clientShare.connectionLost(serverAddress);
                // remove endPoint => add to retry.
                return false;
            }
            if (data.byteBuffer.hasRemaining()) {
                if (log.isDebugEnabled()) {
                    log.debug("Request not fully sent to " + serverAddress + ", add to pending write for " + data.streamId + " and request " + data.requestId + "  " + clientUUID);
                }
                pendingWrite.add(data);
                setPendingWrite.set();
                data.byteBuffer.reset();
            } else {
                data.byteBuffer.reset();
                data.release();
            }
        }
        return true;
    }

    public void shutdown() {
        shutdown = true;
        try {
            channel.close();
        } catch (IOException e) {
        }
        try {
            readSelector.close();
        } catch (IOException e) {
        }
    }
}
