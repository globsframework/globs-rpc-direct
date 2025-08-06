package org.globsframework.rpc.direct;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.model.Glob;

public interface ExposedEndPoint {
    void addReceiver(String path, Receiver receiver, GlobType receivedType);
    interface Receiver {
        Glob receive(Glob data);
    }
}
