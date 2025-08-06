package org.globsframework.rpc.direct;

import org.globsframework.rpc.direct.impl.DirectSimpleServer;

public interface GlobsServer {
    ExposedEndPoint addEndPoint(String host, int port);

    void shutdown();

    static GlobsServer create(){
        return new DirectSimpleServer();
    }
}
