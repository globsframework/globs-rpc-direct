package org.globsframework.rpc.direct.impl;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.model.Glob;
import org.globsframework.rpc.direct.GlobClient;

import java.util.concurrent.TimeUnit;

public class GlobClientProxy implements GlobClient {
    private final String host;
    private final int port;
    private DirectSimpleClient simpleClient;

    public GlobClientProxy(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public Glob request(String path, Glob data, GlobType type) {
        try {
            synchronized (this) {
                if (simpleClient == null) {
                    long connectStart = System.nanoTime();
                    simpleClient = new DirectSimpleClient(host, port);
                    long connectComplete = System.nanoTime();
                    System.out.println("GlobClientProxy.request " +
                                       TimeUnit.NANOSECONDS.toMicros(connectComplete - connectStart) + " µs");
                }
            }
            return simpleClient.request(path, data, type);
        } catch (Exception e) {
            synchronized (this) {
                simpleClient = null;
            }
            throw new RuntimeException(e);
        }
    }
}
