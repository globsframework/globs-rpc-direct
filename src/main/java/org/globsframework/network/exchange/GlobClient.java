package org.globsframework.network.exchange;

public interface GlobClient {

    Exchange connect(String host, int port);

    static GlobClient create(String host, int port) {
        return null;

    }
}
