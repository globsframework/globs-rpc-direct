package org.globsframework.network.exchange;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.model.Glob;
import org.globsframework.network.exchange.impl.ExchangeGlobsServer;

public interface GlobsServer {

    static GlobsServer create(String host, int port) {
        final ExchangeGlobsServer exchangeGlobsServer = new ExchangeGlobsServer(host, port);
        exchangeGlobsServer.init();
        return exchangeGlobsServer;
    }

    void shutdown();

    void onPath(String path, OnClient onClient, GlobType receiveType);

    interface OnClient {
        Receiver onNewClient(OnData onData);
    }

    interface Receiver {
        void receive(Glob data);
        void closed();
    }

    interface OnData {
        void onData(Glob data);

        void close();
    }
}
