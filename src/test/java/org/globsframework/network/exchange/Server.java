package org.globsframework.network.exchange;

import org.globsframework.core.model.Glob;

public class Server {
    public static void main(String[] args) throws InterruptedException {
        GlobsServer server = GlobsServer.create("localhost", 13_000);
        server.onPath("/path/call", new GlobsServer.OnClient() {
            @Override
            public GlobsServer.Receiver onNewClient(GlobsServer.OnData onData) {
                return new  GlobsServer.Receiver() {
                    @Override
                    public void receive(Glob data) {
                        onData.onData(data);
                    }

                    @Override
                    public void closed() {

                    }
                };
            }
        }, Client.ExchangeData.TYPE);
        Thread.currentThread().join();
    }
}
