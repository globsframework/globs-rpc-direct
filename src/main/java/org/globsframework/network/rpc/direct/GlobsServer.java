package org.globsframework.network.rpc.direct;

import org.globsframework.network.rpc.direct.impl.AsyncSimpleServer;

public interface GlobsServer {
    ExposedEndPoint addEndPoint(String host, int port);

    void shutdown();

    static GlobsServer create(){
        return new AsyncSimpleServer();
    }
}
