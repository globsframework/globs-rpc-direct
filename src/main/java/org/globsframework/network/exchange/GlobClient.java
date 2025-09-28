package org.globsframework.network.exchange;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.model.Glob;

public interface GlobClient {
    Exchange connect(String path, DataReceiver dataReceiver, GlobType receiveType, AckOption ackOption, SendOption sendOption);

    enum AckOption {
        NO_ACK(0),
        WITH_ACK_BEFORE_READ_DATA(1),
        WITH_ACK_BEFORE_CLIENT_CALL(2),
        WITH_ACK_AFTER_CLIENT_CALL(3);

        public final int opt;

        AckOption(int opt) {
            this.opt = opt;
        }
    }

    enum SendOption {
        SEND_TO_ALL(0),
        SEND_TO_FIRST(1),
        SEND_TO_ANY(2);

        public final int opt;

        SendOption(int opt) {
            this.opt = opt;
        }
    }

    interface DataReceiver {
        void receive(Glob glob);

        void close();
    }
}
