package org.globsframework.network.exchange;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import org.globsframework.commandline.ParseCommandLine;
import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.fields.IntegerField;
import org.globsframework.core.metamodel.fields.LongField;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.model.Glob;
import org.globsframework.serialisation.model.FieldNumber;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ClientSend {

    public static void main(String[] args) throws IOException, InterruptedException {
        final Glob options = ParseCommandLine.parse(Option.TYPE, args);
        GlobSingleClient globSingleClient = GlobSingleClient.create(options.get(Option.host,  "localhost"), options.get(Option.port, 10_000));
        final UniformReservoir reservoir = new UniformReservoir();
        Histogram histogram = new Histogram(reservoir);
        final MyDataReceiver dataReceiver = new MyDataReceiver(histogram);
        final Exchange connect = globSingleClient.connect("/path/call", dataReceiver, ExchangeData.TYPE, GlobClient.AckOption.WITH_ACK_AFTER_CLIENT_CALL, GlobClient.SendOption.SEND_TO_ALL);

        for (int i = 0; i < 100; i++) {
            connect.send(ExchangeData.create("bla bla", i, System.nanoTime())).join();
        }
        Thread.sleep(1000);
        dump(histogram);

        final Histogram histogram1 = new Histogram(reservoir);
        dataReceiver.setHistogram(histogram1);
        for (int i = 0; i < 100000; i++) {
            final long startAt = System.nanoTime();
            connect.send(ExchangeData.create("bla bla", i, startAt)).join();
            final long endAt = System.nanoTime();
            if (TimeUnit.NANOSECONDS.toMillis(endAt - startAt) > 1) {
                System.out.println("long delay of " + TimeUnit.NANOSECONDS.toMicros(endAt - startAt) + "us");
            }
        }
        Thread.sleep(1000);
        dump(histogram1);
    }

    private static void dump(Histogram histogram1) {
        Snapshot snapshot;
        snapshot = histogram1.getSnapshot();
        System.out.println("count : " + histogram1.getCount());
        System.out.println("max : " + snapshot.getMax());
        System.out.println("99% : " + snapshot.get99thPercentile());
        System.out.println("98% : " + snapshot.get98thPercentile());
        System.out.println("95% : " + snapshot.get95thPercentile());
        System.out.println("75% : " + snapshot.get75thPercentile());
        System.out.println("mean : " + snapshot.getMean());
        System.out.println("min : " + snapshot.getMin());
    }


    static class ExchangeData {
        public static final GlobType TYPE;

        public static final IntegerField id;

        public static final StringField DATA;

        public static final LongField sendAtNS;

        static {
            final GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("Exchange");
            TYPE = typeBuilder.unCompleteType();
            id = typeBuilder.declareIntegerField("id", FieldNumber.create(1));
            DATA = typeBuilder.declareStringField("data", FieldNumber.create(2));
            sendAtNS = typeBuilder.declareLongField("updatedAt", FieldNumber.create(3));
            typeBuilder.complete();
        }

        public static Glob create(String data, int id, long value) {
            return TYPE.instantiate()
                    .set(DATA, data)
                    .set(ExchangeData.id, id)
                    .set(sendAtNS, value);
        }
    }

    private static class MyDataReceiver implements GlobSingleClient.DataReceiver {
        private Histogram histogram;
        int received = 0;

        public MyDataReceiver(Histogram histogram) {
            this.histogram = histogram;
        }

        public void setHistogram(Histogram histogram) {
            this.histogram = histogram;
        }

        @Override
        public void receive(Glob glob) {
            received++;
            histogram.update(TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - glob.get(ExchangeData.sendAtNS)));
        }

        @Override
        public void close() {

        }
    }

    static class Option {
        public static final GlobType TYPE;

        public static final StringField host;

        public static final IntegerField port;

        static {
            final GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("Option");
            TYPE = typeBuilder.unCompleteType();
            host = typeBuilder.declareStringField("host");
            port = typeBuilder.declareIntegerField("port");
            typeBuilder.complete();
        }
    }
}
