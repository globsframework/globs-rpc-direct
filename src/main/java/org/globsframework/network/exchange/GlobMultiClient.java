package org.globsframework.network.exchange;

import org.globsframework.network.exchange.impl.multi.GlobMultiClientImpl;
import org.globsframework.serialisation.BinReaderFactory;
import org.globsframework.serialisation.BinWriterFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public interface GlobMultiClient extends GlobClient  {

    int waitForActifServer(int count, int timeoutInMSEC);

    Endpoint add(String host, int port) throws IOException;

    interface Endpoint {
        Closeable unregister(); // return a closeable that must be call as soon as no more response are expected.
        void setActive();
    }

    static GlobMultiClient create() throws IOException {
        return new GlobMultiClientImpl(10 * 1024, Executors.newThreadPerTaskExecutor(Executors.defaultThreadFactory()),
                BinReaderFactory.create(), BinWriterFactory.create());
    }

    static GlobMultiClient create(Executor executors, BinReaderFactory binReaderFactory, BinWriterFactory binWriterFactory) throws IOException {
        return new GlobMultiClientImpl(10 * 1024, executors, binReaderFactory, binWriterFactory);
    }

    class AlreadyRegisteredException extends RuntimeException {
        public AlreadyRegisteredException(String message) {
            super(message);
        }
    }

}
