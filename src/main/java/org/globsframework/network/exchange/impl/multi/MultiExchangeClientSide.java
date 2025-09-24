package org.globsframework.network.exchange.impl.multi;

import org.globsframework.core.model.Glob;
import org.globsframework.network.exchange.Exchange;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

class MultiExchangeClientSide implements Exchange {
    private final long current;
    private final ClientShare clientShare;
    private final GlobMultiClientImpl.AckMgt ackMgt;
    private final AtomicInteger seq = new AtomicInteger(0);

    public MultiExchangeClientSide(long current, ClientShare clientShare, GlobMultiClientImpl.AckMgt ackMgt) {
        this.current = current;
        this.clientShare = clientShare;
        this.ackMgt = ackMgt;
    }

    @Override
    public CompletableFuture<Boolean> send(Glob data) {
        final int lSeq = seq.incrementAndGet();
        final CompletableFuture<Boolean> value = ackMgt.newAck(lSeq);
        final Data sendableData = clientShare.getFreeData();
        sendableData.serializedOutput.write(current);
        sendableData.serializedOutput.write(lSeq);
        sendableData.binWriter.write(data);
        sendableData.complete();
        for (SendData endPointServeur : clientShare.getEndPointServeurs()) {
            endPointServeur.send(sendableData);
        }
        return value;
    }

    @Override
    public void close() {
        clientShare.releaseClient(current);
        final Data data = clientShare.getFreeData();
        data.serializedOutput.write(-current);
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
