package org.globsframework.network.exchange.impl.multi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

class PendingWrite {
    private static final Logger log = LoggerFactory.getLogger(PendingWrite.class);
    private final Deque<DataWithByteBuffer> pendingWrites = new ArrayDeque<>();

    record DataWithByteBuffer(Data data, ByteBuffer buffer) {}

    synchronized DataWithByteBuffer getCurrent() {
        if (pendingWrites.isEmpty()) {
            throw new RuntimeException("Bug no pending write");
        }
        return pendingWrites.peek();
    }

    synchronized DataWithByteBuffer releaseCurrentAndGetNext() {
        pendingWrites.pop();
        if (pendingWrites.isEmpty()) {
            return null;
        }
        return pendingWrites.peek();
    }

    synchronized boolean addWriteIfNeeded(Data data) {
        if (pendingWrites.isEmpty()) {
            return false;
        } else {
            add(data);
            return true;
        }
    }

    void close() {
    }

    synchronized public void add(Data data) {
        if (log.isDebugEnabled()) {
            log.debug("add write pending " + data.requestId);
        }
        final ByteBuffer wrap = ByteBuffer.wrap(data.byteBuffer.array());
        wrap.limit(data.byteBuffer.limit());
        wrap.position(data.byteBuffer.position());
        pendingWrites.add(new DataWithByteBuffer(data, wrap));
    }
}
