package org.globsframework.network.rpc.direct.impl;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.model.Glob;
import org.globsframework.network.rpc.direct.RpcGlobClient;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class RpcGlobClientProxy implements RpcGlobClient {
    private final String host;
    private final int port;
    private AsyncSimpleClient simpleClient;

    public RpcGlobClientProxy(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public CompletableFuture<Glob> request(String path, Glob data, GlobType resultType) {
        AsyncSimpleClient tmp = null;
        try {
            synchronized (this) {
                tmp = simpleClient;
                if (tmp == null) {
                    tmp = new AsyncSimpleClient(host, port);
                    simpleClient = tmp;
                }
            }
            return tmp.request(path, data, resultType);
        } catch (Exception e) {
            synchronized (this) {
                if (tmp == simpleClient) {
                    try {
                        simpleClient.close();
                    } catch (Exception ex) {
                    }
                    simpleClient = null;
                }
            }
            throw new RuntimeException(e);
        }
    }
}
