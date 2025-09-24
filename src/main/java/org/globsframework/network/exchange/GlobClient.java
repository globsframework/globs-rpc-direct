package org.globsframework.network.exchange;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.model.Glob;

public interface GlobClient {
    Exchange connect(String path, DataReceiver dataReceiver, GlobType receiveType, Option option);

    enum Option {
        NO_ACK(0),
        WITH_ACK_BEFORE_READ_DATA(1),
        WITH_ACK_BEFORE_CLIENT_CALL(2),
        WITH_ACK_AFTER_CLIENT_CALL(3);

        public final int opt;

        Option(int opt) {
            this.opt = opt;
        }
    }

    interface DataReceiver {
        void receive(Glob glob);

        void close();
    }
}
