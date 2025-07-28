package org.globsframework.rpc.direct;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.MutableGlob;
import org.globsframework.rpc.direct.impl.GlobClientProxy;
import org.globsframework.serialisation.field.reader.GlobTypeIndexResolver;
import org.junit.Assert;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

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

    public static class Wait {
        private final int ratePserSecond;
        private final long targetLeapTime;
        private long leapTime;

        public Wait(int ratePerSecond) {
            this.ratePserSecond = ratePerSecond;
            leapTime = 0;
            targetLeapTime = 1_000_000_000 / ratePserSecond;
        }

        public void limitRate() {
            if (leapTime == 0) {
                leapTime = System.nanoTime();
                return;
            }
            long nowIs = System.nanoTime();
            long waitTime = targetLeapTime - (nowIs - leapTime);
            if (waitTime > 0) {
                LockSupport.parkNanos(waitTime);
                leapTime = nowIs + waitTime;
            }
            else {
                leapTime = nowIs;
            }
        }
    }

    private static void loop(GlobClient client) {
        Glob response = null;
        final MutableGlob mutableGlob = DummyObject.TYPE.instantiate();
        for (int i = 0; i < 10000; i++) {
            Glob query = mutableGlob
                    .set(DummyObject.id, i)
                    .set(DummyObject.sendAt, System.nanoTime());
            response = client.request("/", query);
            Assert.assertEquals(i, response.get(DummyObject.id).intValue());
        }
//        if (true) {
//            return;
//        }

//        System.out.println("press enter");
//        try {
//            System.in.read();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
        Histogram histogram = new Histogram(new UniformReservoir());

//        Wait wait = new Wait(10000);
        long startAt = System.currentTimeMillis();
        long tot = 0;
        long count = 0;
        while (startAt  + 30000 >  System.currentTimeMillis()) {
//        while (count < 100000) {
//            wait.limitRate();
            long start = System.nanoTime();
            Glob query = mutableGlob.set(DummyObject.id, count)
//                    .set(DummyObject.name, "Echo message #" + count);
                    .set(DummyObject.sendAt, start);
            response = client.request("/", query);
            Assert.assertEquals(count, response.get(DummyObject.id).intValue());
            long end = System.nanoTime();
            final long micros = TimeUnit.NANOSECONDS.toMicros(end - start);
            tot += micros;
            count++;
            histogram.update(micros);
        }

        long endAt = System.currentTimeMillis();

//        Assert.assertEquals("Echo message #" + (count -1), response.get(DummyObject.name));

        synchronized (Client.class) {
            System.out.println("call per second " + ((double) count) / (endAt - startAt) * 1000);

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
