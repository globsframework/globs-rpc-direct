package org.globsframework.rpc.direct;

import org.globsframework.core.model.Glob;
import org.globsframework.serialisation.field.reader.GlobTypeIndexResolver;

public class GlobClientProxy implements GlobClient {
    private final String host;
    private final int port;
    private final GlobTypeIndexResolver globTypeResolver;
    private DirectSimpleClient simpleClient;

    public GlobClientProxy(String host, int port, GlobTypeIndexResolver globTypeResolver) {
        this.host = host;
        this.port = port;
        this.globTypeResolver = globTypeResolver;
    }

    public Glob request(Glob data) {
        try {
            synchronized (this) {
                if (simpleClient == null) {
                    simpleClient = new DirectSimpleClient(host, port, globTypeResolver);
                }
            }
            return simpleClient.request(data);
        } catch (Exception e) {
            synchronized (this) {
                simpleClient = null;
            }
            throw new RuntimeException(e);
        }
    }
}
