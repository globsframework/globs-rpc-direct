package org.globsframework.network.exchange;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import com.lmax.disruptor.*;
import org.globsframework.commandline.ParseCommandLine;
import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.fields.StringArrayField;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.MutableGlob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiClientSendReplyWithDisruptor {

    private static final Logger log = LoggerFactory.getLogger(MultiClientSendReplyWithDisruptor.class);

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {

        Map<Integer, RingBuffer<MutableGlob>> ringBufferMap = new ConcurrentHashMap<>();
        final Glob options = ParseCommandLine.parse(Option.TYPE, args);
        GlobMultiClient globClient = GlobMultiClient.create();
        final String[] host = options.getOrEmpty(Option.host);
        String[] port = options.getOrEmpty(Option.port);
        for (int i = 0; i < port.length; i++) {
            globClient.add(host.length == 0 ? "localhost" : host[i], Integer.parseInt(port[i]));
        }
        globClient.waitForActifServer(1, 1000);
        final MyDataReceiver dataReceiver = new MyDataReceiver(ringBufferMap);
        final Exchange connect = globClient.connect("/path/call", dataReceiver, ExchangeData.TYPE, GlobClient.AckOption.NO_ACK,
                GlobClient.SendOption.SEND_TO_ANY);

        AtomicInteger count = new AtomicInteger(0);

        final int key = count.incrementAndGet();
        loop(connect, key, createRingBuffer(ringBufferMap, key));

        final ExecutorService executorService = Executors.newCachedThreadPool();
        final Future<?> submit1 = executorService.submit(() -> {
            final int key1 = count.incrementAndGet();
            loop(connect, key1, createRingBuffer(ringBufferMap, key1));
        });
        final Future<?> submit2 = executorService.submit(() -> {
            final int key1 = count.incrementAndGet();
            loop(connect, key1, createRingBuffer(ringBufferMap, key1));
        });
        final Future<?> submit3 = executorService.submit(() -> {
            final int key1 = count.incrementAndGet();
            loop(connect, key1, createRingBuffer(ringBufferMap, key1));
        });
        submit1.get();
        submit2.get();
        submit3.get();
        System.out.println("done");
        executorService.shutdown();
        connect.close();
        globClient.close();
    }

    private static RingBuffer<MutableGlob> createRingBuffer(Map<Integer, RingBuffer<MutableGlob>> ringBufferMap, int key) {
        return ringBufferMap.computeIfAbsent(key, coef ->
                RingBuffer.createSingleProducer(ExchangeData.TYPE::instantiate, 16, new BusySpinWaitStrategy()));
    }

    private static void loop(Exchange connect, int coefficient,
                             RingBuffer<MutableGlob> ringBufferMap) {
        try {
            final Histogram histogram = new Histogram(new UniformReservoir(10000));
            final SequenceBarrier sequenceBarrier = ringBufferMap.newBarrier();
            long currentPosition = ringBufferMap.getCursor() + 1;
            final int end = 100000 * (coefficient + 1);
            for (int i = 100000 * coefficient; i < end; i++) {
                final long startAt = System.nanoTime();
                connect.send(ExchangeData.create("bla bla", i, startAt, coefficient)).join();
                final long cursor = sequenceBarrier.waitFor(currentPosition);
                for (; currentPosition <= cursor; currentPosition++) {
                    final MutableGlob mutableGlob = ringBufferMap.get(currentPosition);
                    final long endAt = System.nanoTime();
                    final long micros = TimeUnit.NANOSECONDS.toMicros(endAt - startAt);
                    if (mutableGlob.get(ExchangeData.sendAtNS) != startAt) {
                        log.error("Bug ");
                    }
                    if (micros > 1000) {
                        log.warn("long delay of " + micros + " us at " + i);
                    }
                    histogram.update(micros);
                }
                if (i % 100 == 0) {
                    try {
                        Thread.sleep(Duration.of(500, ChronoUnit.MICROS));
                    } catch (InterruptedException e) {
                    }
                }
            }
            dump(histogram);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    synchronized private static void dump(Histogram histogram1) {
        Snapshot snapshot;
        snapshot = histogram1.getSnapshot();
        System.out.println("---------------------------");
        System.out.println("count : " + histogram1.getCount());
        System.out.println("max : " + snapshot.getMax());
        System.out.println("99.9% : " + snapshot.get999thPercentile());
        System.out.println("99% : " + snapshot.get99thPercentile());
        System.out.println("98% : " + snapshot.get98thPercentile());
        System.out.println("95% : " + snapshot.get95thPercentile());
        System.out.println("75% : " + snapshot.get75thPercentile());
        System.out.println("mean : " + snapshot.getMean());
        System.out.println("min : " + snapshot.getMin());
        System.out.println("---------------------------");
    }


    private static class MyDataReceiver implements GlobSingleClient.DataReceiver {
        private final Map<Integer, RingBuffer<MutableGlob>> ringBufferMap;
        int received = 0;

        public MyDataReceiver(Map<Integer, RingBuffer<MutableGlob>> ringBufferMap) {
            this.ringBufferMap = ringBufferMap;
        }

        @Override
        public void receive(Glob glob) {
            received++;
            final long micros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - glob.get(ExchangeData.sendAtNS));
            if (micros > 1000) {
                log.warn("long delay of " + micros + " for " + glob.get(ExchangeData.id));
            }
            final RingBuffer<MutableGlob> ringBuffer = ringBufferMap.get(glob.get(ExchangeData.groupId));
            final long next = ringBuffer.next();
            final MutableGlob mutableGlob = ringBuffer.get(next);
            mutableGlob.set(ExchangeData.DATA, glob.get(ExchangeData.DATA));
            mutableGlob.set(ExchangeData.sendAtNS, glob.get(ExchangeData.sendAtNS));
            mutableGlob.set(ExchangeData.groupId, glob.get(ExchangeData.groupId));
            mutableGlob.set(ExchangeData.id, glob.get(ExchangeData.id));
            ringBuffer.publish(next);
//            final CompletableFuture<Glob> sendData = send.remove(glob.get(ExchangeData.id));
//                sendData.complete(glob);
        }

        @Override
        public void close() {
        }
    }


    static class Option {
        public static final GlobType TYPE;

        public static final StringArrayField host;

        public static final StringArrayField port;

        static {
            final GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("Option");
            TYPE = typeBuilder.unCompleteType();
            host = typeBuilder.declareStringArrayField("host");
            port = typeBuilder.declareStringArrayField("port");
            typeBuilder.complete();
        }
    }
}
