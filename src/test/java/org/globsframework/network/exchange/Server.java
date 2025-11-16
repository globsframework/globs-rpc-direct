package org.globsframework.network.exchange;

import org.globsframework.commandline.ParseCommandLine;
import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.fields.IntegerField;
import org.globsframework.core.model.Glob;

import java.util.concurrent.Executors;

public class Server {
    public static void main(String[] args) throws InterruptedException {
        final Glob option = ParseCommandLine.parse(Option.TYPE, args);
        GlobsServer server = GlobsServer.create("localhost", option.get(Option.port, 13_000),
                Executors.newCachedThreadPool());
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
        }, ClientSend.ExchangeData.TYPE);
        Thread.currentThread().join();
    }

    static class Option {
        public static final GlobType TYPE;

        public static final IntegerField port;

        static {
            final GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("Option");
            TYPE = typeBuilder.unCompleteType();
            port = typeBuilder.declareIntegerField("port");
            typeBuilder.complete();
        }
    }

}
