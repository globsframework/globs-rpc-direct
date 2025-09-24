package org.globsframework.network.rpc.direct;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.model.Glob;
import org.globsframework.network.rpc.direct.impl.RpcGlobClientProxy;

import java.util.concurrent.CompletableFuture;

public interface RpcGlobClient {
    CompletableFuture<Glob> request(String path, Glob data, GlobType resultType);

    static RpcGlobClient create(String host, int port) {
        return new RpcGlobClientProxy(host, port);
    }
}
