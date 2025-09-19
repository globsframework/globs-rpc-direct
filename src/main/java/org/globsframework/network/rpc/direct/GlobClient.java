package org.globsframework.network.rpc.direct;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.model.Glob;
import org.globsframework.network.rpc.direct.impl.GlobClientProxy;

import java.util.concurrent.CompletableFuture;

public interface GlobClient {
    CompletableFuture<Glob> request(String path, Glob data, GlobType resultType);

    static GlobClient create(String host, int port) {
        return new GlobClientProxy(host, port);
    }
}
