package org.globsframework.network.exchange;

import org.globsframework.core.model.Glob;

import java.util.concurrent.CompletableFuture;

public interface Exchange {
    CompletableFuture<Boolean> send(Glob data); // synchronous send. CompletableFuture to handle ack

    void close();
}
