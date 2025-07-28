package org.globsframework.rpc.direct;

import org.globsframework.core.model.Glob;

public interface ExposedEndPoint {
    void addReceiver(String path, ExposedEndPoint.Receiver receiver);
    interface Receiver {
        Glob receive(Glob data);
    }
}
