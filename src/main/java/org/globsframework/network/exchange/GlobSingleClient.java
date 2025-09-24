package org.globsframework.network.exchange;

import org.globsframework.network.exchange.impl.single.GlobSingleClientImpl;

import java.io.IOException;

public interface GlobSingleClient extends GlobClient {

    static GlobSingleClient create(String host, int port) throws IOException {
        return new GlobSingleClientImpl(host, port);
    }

    class AlreadyRegisteredException extends RuntimeException {
        public AlreadyRegisteredException(String message) {
            super(message);
        }
    }
}
