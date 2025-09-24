package org.globsframework.network.exchange;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.fields.IntegerField;
import org.globsframework.core.metamodel.fields.LongField;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.model.Glob;
import org.globsframework.serialisation.model.FieldNumber;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class Client {

    public static void main(String[] args) throws IOException, InterruptedException {
        GlobClient globClient = GlobClient.create("localhost", 13000);
        final UniformReservoir reservoir = new UniformReservoir();
        Histogram histogram = new Histogram(reservoir);
        final MyDataReceiver dataReceiver = new MyDataReceiver(histogram);
        final Exchange connect = globClient.connect("/path/call", dataReceiver, ExchangeData.TYPE, GlobClient.Option.WITH_ACK_AFTER_CLIENT_CALL);

        for (int i = 0; i < 100000; i++) {
            connect.send(ExchangeData.create("bla bla", i)).join();
        }
        Thread.sleep(1000);
        dump(histogram);

        final Histogram histogram1 = new Histogram(reservoir);
        dataReceiver.setHistogram(histogram1);
        for (int i = 0; i < 1000; i++) {
            connect.send(ExchangeData.create("bla bla", i)).join();
            Thread.sleep(1);
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
            sendAtNS = typeBuilder.declareLongField("updatedAt",  FieldNumber.create(3));
            typeBuilder.complete();
        }

        public static Glob create(String data, int id) {
            return TYPE.instantiate()
                    .set(DATA, data)
                    .set(ExchangeData.id, id)
                    .set(sendAtNS, System.nanoTime());
        }
    }

    private static class MyDataReceiver implements GlobClient.DataReceiver {
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
}
