package org.globsframework.network.exchange;

public interface GlobsServer {
    Exchange addEndPoint(String host, int port);

    void shutdown();

    static GlobsServer create() {
        return null;
    }
}
