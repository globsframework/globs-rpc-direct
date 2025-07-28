package org.globsframework.rpc.direct;

import org.globsframework.core.model.Glob;

public interface ExposedEndPoint {
    interface Receiver {
        Glob receive(Glob data);
    }
}
