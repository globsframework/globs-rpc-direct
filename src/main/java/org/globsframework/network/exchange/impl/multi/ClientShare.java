package org.globsframework.network.exchange.impl.multi;

import java.util.List;

interface ClientShare {

    List<SendData> getEndPointServeurs();

    Data getFreeData();

    void release(Data data);

    void releaseClient(long current);

    void close(GlobMultiClientImpl.ServerAddress serverAddress);
}
