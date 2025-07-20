package org.globsframework.network;

import org.globsframework.core.model.Glob;
import org.globsframework.core.utils.serialization.DefaultSerializationInput;
import org.globsframework.core.utils.serialization.DefaultSerializationOutput;
import org.globsframework.core.utils.serialization.SerializedInput;
import org.globsframework.core.utils.serialization.SerializedOutput;
import org.globsframework.serialisation.BinReader;
import org.globsframework.serialisation.BinReaderFactory;
import org.globsframework.serialisation.BinWriter;
import org.globsframework.serialisation.BinWriterFactory;
import org.globsframework.serialisation.field.reader.GlobTypeIndexResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class DirectSimpleClient {
    private static final Logger log = LoggerFactory.getLogger(DirectSimpleClient.class);
    private final String host;
    private final int port;
    private final Socket socket;
    private final BinReader globBinReader;
    private final BinWriter binWriter;
    private final BufferedOutputStream bufferedOutputStream;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final SerializedOutput serializedOutput;
    private final SerializedInput serializedInput;
    private long writeOrder;
    private long readOrder;

    public DirectSimpleClient(String host, int port, GlobTypeIndexResolver globTypeResolver) throws IOException {
        this.host = host;
        this.port = port;
        BinReaderFactory binReaderFactory = BinReaderFactory.create(globTypeResolver);
        BinWriterFactory binWriterFactory = BinWriterFactory.create();
        socket = new Socket();
        socket.connect(new InetSocketAddress(host, port));
        final InputStream inputStream = socket.getInputStream();
        final OutputStream outputStream = socket.getOutputStream();
        serializedInput = new DefaultSerializationInput(new BufferedInputStream(inputStream));
        globBinReader = binReaderFactory.createGlobBinReader(serializedInput);
        bufferedOutputStream = new BufferedOutputStream(outputStream);
        serializedOutput = new DefaultSerializationOutput(bufferedOutputStream);
        binWriter = binWriterFactory.create(serializedOutput);
    }

    public Glob request(Glob data) throws IOException {
        long order;
        synchronized (binWriter) {
            order = writeOrder++;
            serializedOutput.write(order);
            binWriter.write(data);
        }
        bufferedOutputStream.flush();
        lock.lock();
        try {
            while (order != readOrder) {
                condition.awaitUninterruptibly();
                if (order != readOrder) {
                    condition.signalAll();
                }
            }
            readOrder++;
            condition.signal();
            final long readOrder = serializedInput.readNotNullLong();
            if (readOrder != order) {
                throw new RuntimeException("BUG : Read order " + readOrder + " != order " + order);
            }
            return globBinReader.read().orElse(null);
        } finally {
            lock.unlock();
        }
    }
}
