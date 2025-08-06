package org.globsframework.rpc.direct;

import org.globsframework.rpc.direct.impl.DirectSimpleServer;

import java.io.IOException;

public class Server {
    public static void main(String[] args) throws IOException {
        DirectSimpleServer server = new DirectSimpleServer();

        ExposedEndPoint remote = server.addEndPoint("localhost", 3000);
        remote.addReceiver("/", data -> {
            return data;
//                    return data.duplicate()
//                            .set(DummyObject.receivedAt, System.nanoTime());
        }, DummyObject.TYPE);

        System.in.read();

    }
}
