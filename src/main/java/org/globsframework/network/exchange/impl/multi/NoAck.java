package org.globsframework.network.exchange.impl.multi;

import java.util.concurrent.CompletableFuture;

class NoAck implements GlobMultiClientImpl.AckMgt {
    static final NoAck instance = new NoAck();

    @Override
    public void close() {
    }

    @Override
    public CompletableFuture<Boolean> newAck(int lSeq) {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public void received(int requestId) {
    }
}
