package org.globsframework.network.rpc.direct.impl;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.model.Glob;
import org.globsframework.network.rpc.direct.GlobClient;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class GlobClientProxy implements GlobClient {
    private final String host;
    private final int port;
    private AsyncSimpleClient simpleClient;

    public GlobClientProxy(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public CompletableFuture<Glob> request(String path, Glob data, GlobType resultType) {
        try {
            synchronized (this) {
                if (simpleClient == null) {
                    long connectStart = System.nanoTime();
                    simpleClient = new AsyncSimpleClient(host, port);
                    long connectComplete = System.nanoTime();
                    System.out.println("GlobClientProxy.request " +
                                       TimeUnit.NANOSECONDS.toMicros(connectComplete - connectStart) + " µs");
                }
            }
            return simpleClient.request(path, data, resultType);
        } catch (Exception e) {
            synchronized (this) {
                simpleClient = null;
            }
            throw new RuntimeException(e);
        }
    }
}
