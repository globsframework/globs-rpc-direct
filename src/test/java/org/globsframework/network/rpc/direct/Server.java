package org.globsframework.network.rpc.direct;

import org.globsframework.network.rpc.direct.impl.DirectSimpleServer;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class Server {
    public static void main(String[] args) throws IOException {
        DirectSimpleServer server = new DirectSimpleServer();

        ExposedEndPoint remote = server.addEndPoint("localhost", 3000);
        remote.addReceiver("/", (data, globInstantiator) -> {
            return CompletableFuture.completedFuture(data);
//                    return data.duplicate()
//                            .set(DummyObject.receivedAt, System.nanoTime());
        }, DummyObject.TYPE);

        System.in.read();

    }
}
