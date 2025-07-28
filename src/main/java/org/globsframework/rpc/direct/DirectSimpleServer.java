package org.globsframework.rpc.direct;

import org.globsframework.serialisation.field.reader.GlobTypeIndexResolver;

import java.util.ArrayList;
import java.util.List;

public class DirectSimpleServer implements GlobsServer {
    private final List<DirectExposedEndPoint> endpoints = new ArrayList<>();

    public ExposedEndPoint addEndPoint(String host, int port, GlobTypeIndexResolver globTypeResolver, ExposedEndPoint.Receiver receiver) {
        DirectExposedEndPoint endpoint = new DirectExposedEndPoint(host, port, globTypeResolver, receiver);
        endpoints.add(endpoint);
        return endpoint;
    }

    public void shutdown() {
        // Shutdown all endpoints
        for (DirectExposedEndPoint endpoint : endpoints) {
            endpoint.shutdown();
        }
        endpoints.clear();
    }
}
