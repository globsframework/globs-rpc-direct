package org.globsframework.rpc.direct.impl;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.model.Glob;
import org.globsframework.core.utils.serialization.DefaultSerializationInput;
import org.globsframework.core.utils.serialization.DefaultSerializationOutput;
import org.globsframework.core.utils.serialization.SerializedInput;
import org.globsframework.core.utils.serialization.SerializedOutput;
import org.globsframework.serialisation.BinReader;
import org.globsframework.serialisation.BinReaderFactory;
import org.globsframework.serialisation.BinWriter;
import org.globsframework.serialisation.BinWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class DirectSimpleClient implements AutoCloseable {
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

    public DirectSimpleClient(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        BinReaderFactory binReaderFactory = BinReaderFactory.create();
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

    public Glob request(String path, Glob data, GlobType type) throws IOException {
        long order;
        synchronized (binWriter) {
            order = writeOrder++;
            serializedOutput.write(order);
            serializedOutput.writeUtf8String(path);
            binWriter.write(data);
        }
        bufferedOutputStream.flush(); //internally synchronized

        // read response, we must read in the same order we do write.
        // Thread sequence can change between lock in write, flush and read
        // we keep write order to force read order.
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
            return globBinReader.read(type).orElse(null);
        } finally {
            lock.unlock();
        }
    }

    public void close() throws IOException {
        socket.close();
    }
}
