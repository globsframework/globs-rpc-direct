package org.globsframework.network.exchange;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.model.Glob;

public interface Exchange {
    void send(String path, Glob data);

    void addReceiver(String path, Receiver receiver, GlobType receivedType);

    interface Receiver {
        void receive(Glob data);
    }
}
