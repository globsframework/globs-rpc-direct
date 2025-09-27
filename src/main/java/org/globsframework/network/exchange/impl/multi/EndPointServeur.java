package org.globsframework.network.exchange.impl.multi;

import org.globsframework.core.model.Glob;
import org.globsframework.core.utils.serialization.NByteBufferSerializationInput;
import org.globsframework.core.utils.serialization.SerializedInput;
import org.globsframework.network.exchange.GlobMultiClient;
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

class EndPointServeur implements GlobMultiClient.Endpoint, SendData, NByteBufferSerializationInput.NextBuffer {
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
        readByteBuffer = ByteBuffer.allocateDirect(1024 * 1024);
        readByteBuffer.limit(0);
        serializedInput = new NByteBufferSerializationInput(readByteBuffer, this);
        globBinReader = binReaderFactory.createGlobBinReader(serializedInput);

        Data data = clientShare.getFreeData();
        data.incWriter();
        data.serializedOutput.write(1);
        data.complete();
        pendingWrite.add(data);
        setPendingWrite = addPendingWrite.add(channel, pendingWrite);
        setPendingWrite.set();
        readSelector = Selector.open();

        readSelectionKey = channel.register(readSelector, SelectionKey.OP_READ);
        channel.finishConnect();
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

    interface RequestAccess{
        void ack(long streamId, int requestId);

        void close(long streamId);

        DataReceivedInfo dataReceived(long streamId, int requestId);
    }

    private void read() {
        try {
            int maxReadWriteVersion = serializedInput.readNotNullInt();
            while (true) {
                final long streamId = serializedInput.readNotNullLong();
                if (streamId < 0) {
                    final int code = serializedInput.readNotNullInt();
                    if (code == CommandId.CLOSE.id) {
                        requestAccess.close(-streamId);
                    } else if (code == CommandId.ACK.id) {
                        final int requestId = serializedInput.readNotNullInt();
                        requestAccess.ack(-streamId, requestId);
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
        } catch (Throwable throwable) {
            clientShare.close(serverAddress);
            log.error("GlobClient read error", throwable);
        }
    }

    synchronized public void send(Data data) {
        data.incWriter();
        if (!pendingWrite.addWriteIfNeeded(data)) {
            try {
                data.byteBuffer.mark();
                channel.write(data.byteBuffer);
            } catch (IOException e) {
                log.error("Error writing data", e);
                data.byteBuffer.reset();
                data.release();
                // remove endPoint => add to retry.
                return;
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
    }

    @Override
    public void unregister() {

    }
}
