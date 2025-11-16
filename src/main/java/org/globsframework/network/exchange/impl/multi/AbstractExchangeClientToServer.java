package org.globsframework.network.exchange.impl.multi;

import org.globsframework.core.model.Glob;
import org.globsframework.core.utils.serialization.ByteBufferSerializationOutput;
import org.globsframework.network.exchange.Exchange;
import org.globsframework.network.exchange.impl.CommandId;
import org.globsframework.network.exchange.impl.DataSerialisationUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

abstract class AbstractExchangeClientToServer implements Exchange {
    private final long streamId;
    private final ClientShare clientShare;
    private final GlobMultiClientImpl.AckMgt ackMgt;
    private final AtomicInteger nextRequestId = new AtomicInteger(0);

    public AbstractExchangeClientToServer(long streamId, ClientShare clientShare, GlobMultiClientImpl.AckMgt ackMgt) {
        this.streamId = streamId;
        this.clientShare = clientShare;
        this.ackMgt = ackMgt;
    }

    @Override
    public CompletableFuture<Boolean> send(Glob data) {
        final SendData[] endPointServers = clientShare.getEndPointServers();
        if (endPointServers.length == 0) {
            return CompletableFuture.failedFuture(new RuntimeException("No endpoint server"));
        }
        final int requestId = nextRequestId.incrementAndGet();
        final CompletableFuture<Boolean> value = ackMgt.newAck(requestId);
        final Data sendableData = clientShare.getFreeData();
        final ByteBufferSerializationOutput serializedOutput = sendableData.serializedOutput;

        DataSerialisationUtils.serializeMessageData(data, streamId, requestId, serializedOutput, sendableData.binWriter);
        sendableData.complete(streamId, requestId);
        sendableData.incWriter();
        if (!send(sendableData)) {
            value.completeExceptionally(new RuntimeException("Send failed"));
        }
        sendableData.release();
        return value;
    }

    abstract boolean send(Data data);

    @Override
    public void close() {
        clientShare.releaseClient(streamId);
        final Data data = clientShare.getFreeData();
        data.serializedOutput.write(-streamId);
        data.serializedOutput.write(CommandId.CLOSE_STREAM.id);
        data.complete(-1, -1);
        data.incWriter();
        for (SendData endPointServeur : clientShare.getEndPointServers()) {
            endPointServeur.send(data);
        }
        data.release();
        ackMgt.close();
    }
}
