package org.globsframework.network;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.MutableGlob;
import org.globsframework.serialisation.field.reader.GlobTypeIndexResolver;
import org.junit.Assert;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Client {
    public static void main(String[] args) throws IOException, InterruptedException {

        GlobClient client = new GlobClientProxy("localhost", 3000,
                GlobTypeIndexResolver.from(DummyObject.TYPE));

        ExecutorService executor = Executors.newFixedThreadPool(10);
        executor.execute(() -> loop(client));
//        executor.execute(() -> loop(client));
//        executor.execute(() -> loop(client));
        executor.shutdown();
        executor.awaitTermination(40, TimeUnit.SECONDS);
    }

    private static void loop(GlobClient client) {
        Glob response = null;
        final MutableGlob mutableGlob = DummyObject.TYPE.instantiate();
        for (int i = 0; i < 10000; i++) {
            Glob query = mutableGlob
                    .set(DummyObject.id, i)
                    .set(DummyObject.name, "Echo message #" + i);
            response = client.request(query);
            Assert.assertEquals(i, response.get(DummyObject.id).intValue());
        }
        Histogram histogram = new Histogram(new UniformReservoir());

        long startAt = System.currentTimeMillis() + 30000;
        long tot = 0;
        long count = 0;
        while (startAt >  System.currentTimeMillis()) {
            Glob query = mutableGlob.set(DummyObject.id, count)
                    .set(DummyObject.name, "Echo message #" + count);
            long start = System.nanoTime();
            response = client.request(query);
            Assert.assertEquals(count, response.get(DummyObject.id).intValue());
            long end = System.nanoTime();
            final long micros = TimeUnit.NANOSECONDS.toMicros(end - start);
            tot += micros;
            count++;
            histogram.update(micros);
        }

        Assert.assertEquals("Echo message #" + (count -1), response.get(DummyObject.name));

        synchronized (Client.class) {
            System.out.println("call per second " + count/30.);

            final Snapshot snapshot = histogram.getSnapshot();
            System.out.println("mean :  " + (tot / count) + " us.");
            System.out.println("max : " + snapshot.getMax());
            System.out.println("min : " + snapshot.getMin());
            System.out.println("average : " + snapshot.getMean());
            System.out.println("99.9 : " + snapshot.get999thPercentile());
            System.out.println("99 : " + snapshot.get99thPercentile());
            System.out.println("98 : " + snapshot.get98thPercentile());
            System.out.println("95 : " + snapshot.get95thPercentile());
        }
    }
}
