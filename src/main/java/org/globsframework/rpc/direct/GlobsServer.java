package org.globsframework.rpc.direct;

import org.globsframework.rpc.direct.impl.DirectSimpleServer;
import org.globsframework.serialisation.field.reader.GlobTypeIndexResolver;

public interface GlobsServer {
    ExposedEndPoint addEndPoint(String host, int port, GlobTypeIndexResolver globTypeResolver);

    void shutdown();

    static GlobsServer create(){
        return new DirectSimpleServer();
    }
}
