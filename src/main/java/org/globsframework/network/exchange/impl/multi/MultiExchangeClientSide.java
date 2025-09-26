package org.globsframework.network.exchange.impl.multi;

import org.globsframework.core.model.Glob;
import org.globsframework.core.utils.serialization.ByteBufferSerializationOutput;
import org.globsframework.network.exchange.Exchange;
import org.globsframework.network.exchange.impl.DataSerialisationUtils;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

class MultiExchangeClientSide implements Exchange {
    private final long streamId;
    private final ClientShare clientShare;
    private final GlobMultiClientImpl.AckMgt ackMgt;
    private final AtomicInteger nextRequestId = new AtomicInteger(0);

    public MultiExchangeClientSide(long streamId, ClientShare clientShare, GlobMultiClientImpl.AckMgt ackMgt) {
        this.streamId = streamId;
        this.clientShare = clientShare;
        this.ackMgt = ackMgt;
    }

    @Override
    public CompletableFuture<Boolean> send(Glob data) {
        final List<SendData> endPointServeurs = clientShare.getEndPointServeurs();
        if (endPointServeurs.isEmpty()) {
            return CompletableFuture.failedFuture(new RuntimeException("No endpoint server"));
        }
        final int requestId = nextRequestId.incrementAndGet();
        final CompletableFuture<Boolean> value = ackMgt.newAck(requestId);
        final Data sendableData = clientShare.getFreeData();
        final ByteBufferSerializationOutput serializedOutput = sendableData.serializedOutput;

        DataSerialisationUtils.serializeMessageData(data, streamId, requestId, serializedOutput, sendableData.binWriter);
        sendableData.complete();
        for (SendData endPointServeur : endPointServeurs) {
            endPointServeur.send(sendableData);
        }
        return value;
    }

    @Override
    public void close() {
        clientShare.releaseClient(streamId);
        final Data data = clientShare.getFreeData();
        data.serializedOutput.write(-streamId);
        data.serializedOutput.write(1);
        data.complete();
        data.incWriter();
        for (SendData endPointServeur : clientShare.getEndPointServeurs()) {
            endPointServeur.send(data);
        }
        data.release();
        ackMgt.close();
    }
}
