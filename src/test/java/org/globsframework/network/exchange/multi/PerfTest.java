package org.globsframework.network.exchange.multi;

import org.globsframework.core.model.Glob;
import org.globsframework.network.exchange.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class PerfTest {

    @Test
    void perfMulti() throws IOException, InterruptedException {
        final ExecutorService executor = Executors.newCachedThreadPool();
        GlobsServer server1 = GlobsServer.create("localhost", 0, executor);
        GlobsServer server2 = GlobsServer.create("localhost", 0, executor);
        GlobsServer server3 = GlobsServer.create("localhost", 0, executor);

        AtomicInteger count = new AtomicInteger(0);
        GlobsServer.OnClient onClient = new GlobsServer.OnClient() {
            @Override
            public GlobsServer.Receiver onNewClient(GlobsServer.OnData onData) {
                try {
                    final int nanos = (int) (10000 * Math.random());
                    if (nanos > 100) {
                        Thread.sleep(0, nanos);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
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
                GlobClient.AckOption.NO_ACK, GlobClient.SendOption.SEND_TO_ALL);

        int serverCount = globMultiClient.waitForActifServer(3, 1000);
        Assertions.assertEquals(3, serverCount);

        final CompletableFuture<Void> d = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < 1_000_000; i++) {
                exchange.send(ExchangeData.create("d", i)).join();
            }
        });
        d.join();
        System.out.println("PerfTest.perfMulti : end send");
        while(count.get() != 3_000_000) {
            Thread.sleep(1000);
        };
        System.out.println("PerfTest.perfMulti : end read");

        endpoint1.unregister();
        endpoint2.unregister();
        endpoint3.unregister();

        server1.shutdown();
        server2.shutdown();
        server3.shutdown();
        executor.shutdown();

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

}
