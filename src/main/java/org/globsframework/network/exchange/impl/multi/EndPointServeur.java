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
import java.util.concurrent.Executor;

public class EndPointServeur implements SendData, NByteBufferSerializationInput.NextBuffer {
    private final static Logger log = LoggerFactory.getLogger(EndPointServeur.class);
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
    private volatile boolean shutdown;

    public EndPointServeur(GlobMultiClientImpl.ServerAddress serverAddress, GlobMultiClientImpl.AddPendingWrite addPendingWrite,
                           ClientShare clientShare,
                           RequestAccess requestAccess, Executor executor) throws IOException {
        this.serverAddress = serverAddress;
        this.addPendingWrite = addPendingWrite;
        this.clientShare = clientShare;
        this.requestAccess = requestAccess;
        BinReaderFactory binReaderFactory = BinReaderFactory.create();
        channel = SocketChannel.open();
        channel.configureBlocking(false);
        channel.connect(new InetSocketAddress(serverAddress.host(), serverAddress.port()));
        channel.socket().setTcpNoDelay(true);
        readByteBuffer = ByteBuffer.allocateDirect(10 * 1024);
        readByteBuffer.limit(0);
        serializedInput = new NByteBufferSerializationInput(readByteBuffer, this);
        globBinReader = binReaderFactory.createGlobBinReader(serializedInput);

        Data data = clientShare.getFreeData();
        data.incWriter();
        data.serializedOutput.write(1);
        data.complete();
        pendingWrite.add(data);
        setPendingWrite = this.addPendingWrite.add(channel, pendingWrite);
        setPendingWrite.set();
        readSelector = Selector.open();
        readSelectionKey = channel.register(readSelector, SelectionKey.OP_READ);
        executor.execute(this::read);
    }

    @Override
    public ByteBuffer refill(ByteBuffer byteBuffer) {
        while (true) {
            try {
                readSelector.select();
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

    interface RequestAccess {
        void ack(long streamId, int requestId);

        void close(long streamId);

        DataReceivedInfo dataReceived(long streamId, int requestId);

        void error(long streamId, int requestId, String errorMessage);
    }

    private void read() {
        try {
            channel.finishConnect();
            int maxReadWriteVersion = serializedInput.readNotNullInt();
            clientShare.connectionOK(serverAddress, this);

            while (!shutdown) {
                final long streamId = serializedInput.readNotNullLong();
                if (streamId < 0) {
                    final int code = serializedInput.readNotNullInt();
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
                            log.error("Error reading data for type " + responseInfo.receiveType().getName(), e);
                            serializedInput.readToLimit();
                        }
                        if (read != null) {
                            try {
                                responseInfo.dataReceiver().receive(read.orElse(null));
                            } catch (Exception e) {
                                log.error("Error in receiver.", e);
                            }
                        }
                    }
                    serializedInput.resetLimit();
                }
            }
            log.info("Shutting down " + serverAddress);
        } catch (Throwable throwable) {
            if (shutdown) {
                log.info("Shutting down " + serverAddress);
                return;
            }
            clientShare.connectionLost(serverAddress);
            log.error("GlobClient read error", throwable);
        }
    }

    synchronized public boolean send(Data data) {
        if (!channel.isConnected()){
            return false;
        }
        data.incWriter();
        if (!pendingWrite.addWriteIfNeeded(data)) {
            try {
                data.byteBuffer.mark();
                channel.write(data.byteBuffer);
            } catch (IOException e) {
                log.error("Error writing data", e);
                data.byteBuffer.reset();
                data.release();
                clientShare.connectionLost(serverAddress);
                // remove endPoint => add to retry.
                return false;
            }
            if (data.byteBuffer.hasRemaining()) {
                log.debug("EndPointServeur.send has remaining");
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
        readSelector.wakeup();
    }
}
