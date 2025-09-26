package org.globsframework.network.exchange.impl;

import org.globsframework.core.model.Glob;
import org.globsframework.core.utils.serialization.ByteBufferSerializationOutput;
import org.globsframework.serialisation.BinWriter;

public class DataSerialisationUtils {
    public static void serializeMessageData(Glob data, long streamId, int requestId,
                                            ByteBufferSerializationOutput serializedOutput, BinWriter binWriter) {
        serializedOutput.reset();
        serializedOutput.write(streamId);
        serializedOutput.write(requestId);
        final int position = serializedOutput.position();
        serializedOutput.write(0);
        binWriter.write(data);
        final int lastPosition = serializedOutput.position();
        serializedOutput.reset(position);
        serializedOutput.write(lastPosition - position - 4);
        serializedOutput.reset(lastPosition);
    }
}
