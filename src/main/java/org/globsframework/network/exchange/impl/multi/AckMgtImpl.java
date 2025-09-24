package org.globsframework.network.exchange.impl.multi;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

class AckMgtImpl implements GlobMultiClientImpl.AckMgt {
    private final Map<Integer, CompletableFuture<Boolean>> results = new ConcurrentHashMap<>();

    @Override
    public void close() {
        if (!results.isEmpty()) {
            Exception exception = new RuntimeException("Connection closed");
            results.forEach((key, value) -> {
                value.completeExceptionally(exception);
            });
        }
    }

    @Override
    public CompletableFuture<Boolean> newAck(int lSeq) {
        final CompletableFuture<Boolean> value = new CompletableFuture<>();
        results.put(lSeq, value);
        return value;
    }

    @Override
    public void received(int requestId) {
        final CompletableFuture<Boolean> remove = results.remove(requestId);
        if (remove == null) {
            return;
        }
        remove.complete(true);
    }
}
