package org.globsframework.network.exchange;

import org.globsframework.network.exchange.impl.multi.GlobMultiClientImpl;

import java.io.IOException;

public interface GlobMultiClient extends GlobClient  {

    int waitForActifServer(int count, int timeoutInMSEC);

    Endpoint add(String host, int port) throws IOException;

    interface Endpoint {
        void unregister();
        void setActive();
    }

    static GlobMultiClient create() throws IOException {
        return new GlobMultiClientImpl(10 * 1024);
    }

    class AlreadyRegisteredException extends RuntimeException {
        public AlreadyRegisteredException(String message) {
            super(message);
        }
    }

}
