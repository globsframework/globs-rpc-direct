package org.globsframework.network.exchange;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.model.Glob;
import org.globsframework.network.exchange.impl.ExchangeGlobsServer;

import java.util.concurrent.Executor;

public interface GlobsServer {

    static GlobsServer create(String host, int port, Executor executor) {
        ExchangeGlobsServer.Builder builder = ExchangeGlobsServer.Builder.create(host, port);
        builder.with(executor);
        return builder.build();
    }

    int getPort();

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
