package org.globsframework.rpc.direct.impl;

import org.globsframework.rpc.direct.ExposedEndPoint;
import org.globsframework.rpc.direct.GlobsServer;
import org.globsframework.serialisation.field.reader.GlobTypeIndexResolver;

import java.util.ArrayList;
import java.util.List;

public class DirectSimpleServer implements GlobsServer {
    private final List<DirectExposedEndPoint> endpoints = new ArrayList<>();

    public ExposedEndPoint addEndPoint(String host, int port, GlobTypeIndexResolver globTypeResolver) {
        DirectExposedEndPoint endpoint = new DirectExposedEndPoint(host, port, globTypeResolver);
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
