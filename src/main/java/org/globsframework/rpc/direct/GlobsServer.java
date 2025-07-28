package org.globsframework.rpc.direct;

import org.globsframework.serialisation.field.reader.GlobTypeIndexResolver;

public interface GlobsServer {
    ExposedEndPoint addEndPoint(String host, int port, GlobTypeIndexResolver globTypeResolver, ExposedEndPoint.Receiver receiver);

    void shutdown();
}
