package org.globsframework.network.exchange.impl.multi;

import org.globsframework.core.utils.serialization.ByteBufferSerializationOutput;
import org.globsframework.serialisation.BinWriter;
import org.globsframework.serialisation.BinWriterFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class Data {
    private final AtomicInteger pendingWrites = new AtomicInteger(0);
    public final ByteBuffer byteBuffer;
    public final OnRelease release;
    public final ByteBufferSerializationOutput serializedOutput;
    public final BinWriter binWriter;
    public long streamId;
    public int requestId;

    public void reset() {
        serializedOutput.reset();
    }

    public void complete(long streamId, int requestId) {
        this.streamId = streamId;
        this.requestId = requestId;
        byteBuffer.position(0);
        byteBuffer.limit(serializedOutput.position());
    }

    public void incWriter() {
        pendingWrites.incrementAndGet();
    }

    interface OnRelease {
        void release(Data data);
    }

    Data(int maxMessageSize, OnRelease release, BinWriterFactory binWriterFactory) {
        this.release = release;
        final byte[] buffer = new byte[maxMessageSize];
        serializedOutput = new ByteBufferSerializationOutput(buffer);
        binWriter = binWriterFactory.create(serializedOutput);
        this.byteBuffer = ByteBuffer.wrap(buffer);
    }

    void release() {
        if (pendingWrites.decrementAndGet() == 0) {
            release.release(this);
        }
    }
}
