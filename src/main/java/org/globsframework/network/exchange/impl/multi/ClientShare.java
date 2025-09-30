package org.globsframework.network.exchange.impl.multi;

interface ClientShare {

    SendData[] getEndPointServers();

    void sendToAll(Data data);

    void sendToOne(Data data);

    void sendToFirst(Data data);

    Data getFreeData();

    void release(Data data);

    void releaseClient(long current);

    void connectionLost(GlobMultiClientImpl.ServerAddress serverAddress);

    void connectionOK(GlobMultiClientImpl.ServerAddress serverAddress, EndPointServeur endPointServeur);
}
