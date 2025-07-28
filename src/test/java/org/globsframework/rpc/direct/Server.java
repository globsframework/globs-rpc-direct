package org.globsframework.rpc.direct;

import org.globsframework.serialisation.field.reader.GlobTypeIndexResolver;

import java.io.IOException;

public class Server {
    public static void main(String[] args) throws IOException {
        DirectSimpleServer server = new DirectSimpleServer();

        ExposedEndPoint remote = server.addEndPoint("localhost", 3000,
                GlobTypeIndexResolver.from(DummyObject.TYPE), data -> {
            return data;
//                    return data.duplicate()
//                            .set(DummyObject.receivedAt, System.nanoTime());
        });

        System.in.read();

    }
}
