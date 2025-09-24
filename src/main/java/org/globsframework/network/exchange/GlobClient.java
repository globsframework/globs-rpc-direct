package org.globsframework.network.exchange;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.model.Glob;
import org.globsframework.network.exchange.impl.GlobClientImpl;

import java.io.IOException;

public interface GlobClient {

    Exchange connect(String path, DataReceiver dataReceiver, GlobType receiveType, Option option);

    static GlobClient create(String host, int port) throws IOException {
        return new GlobClientImpl(host, port);
    }

    interface DataReceiver {
        void receive(Glob glob);

        void close();
    }

    class AlreadyRegisteredException extends RuntimeException {
        public AlreadyRegisteredException(String message) {
            super(message);
        }
    }

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
}
