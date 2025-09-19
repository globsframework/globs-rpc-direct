package org.globsframework.network.rpc.direct;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.GlobInstantiator;

import java.util.concurrent.CompletableFuture;

public interface ExposedEndPoint {
    void addReceiver(String path, Receiver receiver, GlobType receivedType);
    interface Receiver {
        CompletableFuture<Glob> receive(Glob data, GlobInstantiator globInstantiator);
    }
}
