package org.globsframework.network.exchange;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import org.globsframework.commandline.ParseCommandLine;
import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.fields.*;
import org.globsframework.core.model.Glob;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class MultiClient {

    public static void main(String[] args) throws IOException, InterruptedException {
        final Glob options = ParseCommandLine.parse(Option.TYPE, args);
        GlobMultiClient globClient = GlobMultiClient.create();
        final String[] host = options.getOrEmpty(Option.host);
        String[] port = options.getOrEmpty(Option.port);
        for (int i = 0; i < port.length; i++) {
            globClient.add(host.length == 0 ? "localhost" : host[i], Integer.parseInt(port[i]));
        }
        final UniformReservoir reservoir = new UniformReservoir();
        Histogram histogram = new Histogram(reservoir);
        final MyDataReceiver dataReceiver = new MyDataReceiver(histogram);
        final Exchange connect = globClient.connect("/path/call", dataReceiver, ExchangeData.TYPE, GlobClient.AckOption.WITH_ACK_AFTER_CLIENT_CALL, GlobClient.SendOption.SEND_TO_ALL);

        for (int i = 0; i < 100000; i++) {
            connect.send(ExchangeData.create("bla bla", i)).join();
        }
        Thread.sleep(1000);
        dump(histogram);

        final Histogram histogram1 = new Histogram(reservoir);
        dataReceiver.setHistogram(histogram1);
        for (int i = 0; i < 1000; i++) {
            connect.send(ExchangeData.create("bla bla", i)).join();
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
