package org.globsframework.network.exchange.multi;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.fields.LongField;
import org.globsframework.core.model.Glob;
import org.globsframework.core.utils.exceptions.ItemNotFound;
import org.globsframework.network.exchange.*;
import org.globsframework.serialisation.model.FieldNumber;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiClientTest {

    @Test
    public void simpleMulti() throws IOException {
        final ExecutorService executor = Executors.newCachedThreadPool();
        GlobsServer server1 = GlobsServer.create("localhost", 0, executor);
        GlobsServer server2 = GlobsServer.create("localhost", 0, executor);
        GlobsServer server3 = GlobsServer.create("localhost", 0, executor);

        AtomicInteger count = new AtomicInteger(0);
        GlobsServer.OnClient onClient = new GlobsServer.OnClient() {
            @Override
            public GlobsServer.Receiver onNewClient(GlobsServer.OnData onData) {
                return new ResendReceiver(onData, count);
            }
        };
        server1.onPath("/path", onClient, ExchangeData.TYPE);
        server2.onPath("/path", onClient, ExchangeData.TYPE);
        server3.onPath("/path", onClient, ExchangeData.TYPE);

        final GlobMultiClient globMultiClient = GlobMultiClient.create();
         GlobMultiClient.Endpoint endpoint1 = globMultiClient.add("localhost", server1.getPort());
         GlobMultiClient.Endpoint endpoint2 = globMultiClient.add("localhost", server2.getPort());
        GlobMultiClient.Endpoint endpoint3 = globMultiClient.add("localhost", server3.getPort());
        final CountDataReceiver dataReceiver = new CountDataReceiver();
        final Exchange exchange = globMultiClient.connect("/path", dataReceiver, ExchangeData.TYPE,
                GlobClient.Option.WITH_ACK_AFTER_CLIENT_CALL);

        int serverCount = globMultiClient.waitForServer(3, 1000);
        Assertions.assertEquals(3, serverCount);
        exchange.send(ExchangeData.create("d", 1)).join();
        Assertions.assertEquals(3, count.get());
        Assertions.assertEquals(3, dataReceiver.count.get());

        final CompletableFuture<Void> d = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < 1000; i++) {
                exchange.send(ExchangeData.create("d", i)).join();
                try {
                    Thread.sleep(0, 300_000);
                } catch (InterruptedException e) {
                }
            }
        });

        server3.shutdown();
        server2.shutdown();
        server3 = GlobsServer.create("localhost", 0, executor);
        endpoint3 = globMultiClient.add("localhost", server3.getPort());
        serverCount = globMultiClient.waitForServer(2, 1000);
        Assertions.assertEquals(2, serverCount);

        d.join();

        Assertions.assertTrue(count.get() > 1000);
        Assertions.assertTrue(dataReceiver.count.get() > 1000);

        endpoint1.unregister();
        endpoint2.unregister();
        endpoint3.unregister();

        server1.shutdown();
        server2.shutdown();
        server3.shutdown();
        executor.shutdown();
    }

    @Test
    void UnexpectedType() throws IOException, ExecutionException, InterruptedException, TimeoutException {
        final ExecutorService executor = Executors.newCachedThreadPool();
        GlobsServer server1 = GlobsServer.create("localhost", 0, executor);
        AtomicInteger count = new AtomicInteger(0);
        GlobsServer.OnClient onClient = new GlobsServer.OnClient() {
            @Override
            public GlobsServer.Receiver onNewClient(GlobsServer.OnData onData) {
                return new ResendReceiver(onData, count);
            }
        };
        server1.onPath("/path", onClient, ExchangeData.TYPE);
        final GlobMultiClient globMultiClient = GlobMultiClient.create();
        GlobMultiClient.Endpoint endpoint1 = globMultiClient.add("localhost", server1.getPort());
        final CountDataReceiver dataReceiver = new CountDataReceiver();
        final Exchange exchange = globMultiClient.connect("/path", dataReceiver, ExchangeData.TYPE,
                GlobClient.Option.WITH_ACK_AFTER_CLIENT_CALL);

        int serverCount = globMultiClient.waitForServer(1, 1000);
        Assertions.assertEquals(1, serverCount);
        try {
            exchange.send(UnexpectedType.TYPE.instantiate().set(UnexpectedType.id, 1L)).get(2, TimeUnit.SECONDS);
            Assertions.fail("Should have thrown timeout");
        } catch (TimeoutException e) {
        }

        Assertions.assertEquals(0, count.get());
        Assertions.assertEquals(0, dataReceiver.count.get());

        exchange.send(ExchangeData.create("d", 1)).join();

        Assertions.assertEquals(1, count.get());
        Assertions.assertEquals(1, dataReceiver.count.get());
    }


    @Test
    void UnexpectedResponse() throws IOException {
        final ExecutorService executor = Executors.newCachedThreadPool();
        GlobsServer server1 = GlobsServer.create("localhost", 0, executor);
        AtomicInteger count = new AtomicInteger(0);
        GlobsServer.OnClient onClient = new GlobsServer.OnClient() {
            @Override
            public GlobsServer.Receiver onNewClient(GlobsServer.OnData onData) {
                return new GlobsServer.Receiver() {
                    @Override
                    public void receive(Glob data) {
                        count.incrementAndGet();
                        if (data.get(ExchangeData.id) == 1L) {
                            onData.onData(UnexpectedType.TYPE.instantiate().set(UnexpectedType.id, 1L));
                        }
                        else {
                            onData.onData(data);
                        }
                    }

                    @Override
                    public void closed() {
                    }
                };
            }
        };
        server1.onPath("/path", onClient, ExchangeData.TYPE);
        final GlobMultiClient globMultiClient = GlobMultiClient.create();
        GlobMultiClient.Endpoint endpoint1 = globMultiClient.add("localhost", server1.getPort());
        final CountDataReceiver dataReceiver = new CountDataReceiver();
        final Exchange exchange = globMultiClient.connect("/path", dataReceiver, ExchangeData.TYPE,
                GlobClient.Option.WITH_ACK_AFTER_CLIENT_CALL);

        int serverCount = globMultiClient.waitForServer(1, 1000);
        Assertions.assertEquals(1, serverCount);
        exchange.send(ExchangeData.create("sdf", 1)).join();
        exchange.send(ExchangeData.create("sdf", 2)).join();
        exchange.send(ExchangeData.create("sdf", 1)).join();
        exchange.send(ExchangeData.create("sdf", 2)).join();
        Assertions.assertEquals(4, count.get());
        Assertions.assertEquals(2, dataReceiver.count.get());
    }

    private static class ResendReceiver implements GlobsServer.Receiver {
        AtomicInteger count;
        private final GlobsServer.OnData onData;

        public ResendReceiver(GlobsServer.OnData onData, AtomicInteger count) {
            this.onData = onData;
            this.count = count;
        }

        @Override
        public void receive(Glob data) {
            count.incrementAndGet();
            onData.onData(data);
        }

        @Override
        public void closed() {

        }
    }

    private static class CountDataReceiver implements GlobClient.DataReceiver {
        AtomicInteger count = new AtomicInteger(0);
        @Override
        public void receive(Glob glob) {
            count.incrementAndGet();
        }

        @Override
        public void close() {
        }
    }

    public static class UnexpectedType {
        public static final GlobType TYPE;

        public static LongField id;

        static {
            final GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("UnexpectedType");
            TYPE = typeBuilder.unCompleteType();
            id = typeBuilder.declareLongField("id", FieldNumber.create(1));
            typeBuilder.complete();
        }
    }
}
