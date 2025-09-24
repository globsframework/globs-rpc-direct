package org.globsframework.network.rpc.direct;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import junit.framework.TestCase;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.MutableGlob;
import org.globsframework.network.rpc.direct.impl.AsyncSimpleServer;
import org.globsframework.network.rpc.direct.impl.RpcGlobClientProxy;
import org.junit.Assert;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncSimpleServerTest extends TestCase {

    public void testName() throws IOException {
        AsyncSimpleServer server = new AsyncSimpleServer();

        AtomicInteger counter = new AtomicInteger();
        ExposedEndPoint remote = server.addEndPoint("localhost", 3000);
        remote.addReceiver("/", (data, globInstantiator) -> {
            counter.incrementAndGet();
            return CompletableFuture.completedFuture(data);
        }, DummyObject.TYPE);
        MutableGlob query = DummyObject.TYPE.instantiate()
                .set(DummyObject.id, 1)
                .set(DummyObject.name, "test");
        RpcGlobClient client = new RpcGlobClientProxy("localhost", 3000);
        Glob response = null;
        for (int i = 0; i < 1000; i++) {
            response = client.request("/", query, DummyObject.TYPE).join();
        }
        Assert.assertEquals(1000, counter.get());
        Histogram histogram = new Histogram(new UniformReservoir());

        long startAt = System.currentTimeMillis() + 1000;
        int count = 0;
        long endAt= 0;
        long tot = 0;
        while (startAt > (endAt = System.currentTimeMillis())) {
            query.set(DummyObject.name, "test " + count);
            long start = System.nanoTime();
            response = client.request("/", query, DummyObject.TYPE).join();
            long end = System.nanoTime();
            final long micros = TimeUnit.NANOSECONDS.toMicros(end - start);
            tot += micros;
            histogram.update(micros);
            count++;
        }

        Assert.assertEquals(1, response.get(DummyObject.id, 0));
        Assert.assertEquals("test " + (count - 1), response.get(DummyObject.name));

        final Snapshot snapshot = histogram.getSnapshot();
        System.out.println("call per second " + count/30.);
        System.out.println("max : " + snapshot.getMax());
        System.out.println("min : " + snapshot.getMin());
        System.out.println("average : " + snapshot.getMean());
        System.out.println("99 : " + snapshot.get99thPercentile());
        System.out.println("98 : " + snapshot.get98thPercentile());
        System.out.println("95 : " + snapshot.get95thPercentile());

        server.shutdown();
    }

}