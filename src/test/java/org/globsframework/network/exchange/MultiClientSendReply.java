package org.globsframework.network.exchange;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import org.globsframework.commandline.ParseCommandLine;
import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.fields.StringArrayField;
import org.globsframework.core.model.Glob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiClientSendReply {

    private static final Logger log = LoggerFactory.getLogger(MultiClientSendReply.class);

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        final Glob options = ParseCommandLine.parse(Option.TYPE, args);
        GlobMultiClient globClient = GlobMultiClient.create();
        final String[] host = options.getOrEmpty(Option.host);
        String[] port = options.getOrEmpty(Option.port);
        for (int i = 0; i < port.length; i++) {
            globClient.add(host.length == 0 ? "localhost" : host[i], Integer.parseInt(port[i]));
        }
        globClient.waitForActifServer(1, 1000);
        Map<Integer, CompletableFuture<Glob>> send = new ConcurrentHashMap<>();
        final MyDataReceiver dataReceiver = new MyDataReceiver(send);
        final Exchange connect = globClient.connect("/path/call", dataReceiver, ExchangeData.TYPE, GlobClient.AckOption.NO_ACK,
                GlobClient.SendOption.SEND_TO_ANY);

//        Histogram histogram = new Histogram(new UniformReservoir());
//        for (int i = 0; i < 100; i++) {
//            final Glob data = ExchangeData.create("bla bla", i, System.nanoTime());
//            final CompletableFuture<Glob> value = new CompletableFuture<>();
//            send.put(i, value);
//            connect.send(data);
//            value.join();
//        }
//        dump(histogram);

        AtomicInteger count = new AtomicInteger(0);

        loop(send, connect, count.incrementAndGet());

        final ExecutorService executorService = Executors.newCachedThreadPool();
        final Future<?> submit1 = executorService.submit(() -> {
            loop(send, connect, count.incrementAndGet());
        });
        final Future<?> submit2 = executorService.submit(() -> {
            loop(send, connect, count.incrementAndGet());
        });
        final Future<?> submit3 = executorService.submit(() -> {
            loop(send, connect, count.incrementAndGet());
        });
        submit1.get();
        submit2.get();
        submit3.get();
        System.out.println("done");
        executorService.shutdown();
        connect.close();
        globClient.close();
    }

    private static void loop(Map<Integer, CompletableFuture<Glob>> send, Exchange connect, int coefficient) {
        final Histogram histogram = new Histogram(new UniformReservoir(10000));
        final int end = 10000 * (coefficient + 1);
        for (int i = 10000 * coefficient; i < end; i++) {
            final long startAt = System.nanoTime();
            final CompletableFuture<Glob> value = new CompletableFuture<>();
            send.put(i, value);
            connect.send(ExchangeData.create("bla bla", i, startAt)).join();
            value.join();
            final long endAt = System.nanoTime();
            final long micros = TimeUnit.NANOSECONDS.toMicros(endAt - startAt);
            histogram.update(micros);
            if (micros > 1000) {
                log.warn("long delay of " + micros + "us at " + i);
            }
            if (i % 100 == 0) {
                try {
                    Thread.sleep(Duration.of(500, ChronoUnit.MICROS));
                } catch (InterruptedException e) {
                }
            }
        }
        dump(histogram);
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
        private final Map<Integer, CompletableFuture<Glob>> send;
        int received = 0;

        public MyDataReceiver(Map<Integer, CompletableFuture<Glob>> send) {
            this.send = send;
        }

        @Override
        public void receive(Glob glob) {
            received++;
            final CompletableFuture<Glob> sendData = send.remove(glob.get(ExchangeData.id));
            if (sendData != null) {
                final long micros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - glob.get(ExchangeData.sendAtNS));
                if (micros > 1000) {
                    log.warn("long delay of " + micros + " for " + glob.get(ExchangeData.id));
                }
                sendData.complete(glob);
            }
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
