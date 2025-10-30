package org.globsframework.network.exchange.impl.multi;

interface ClientShare {

    SendData[] getEndPointServers();

    boolean sendToAll(Data data);

    boolean sendToOne(Data data);

    boolean sendToActif(Data data);

    boolean sendToFirst(Data data);

    Data getFreeData();

    void release(Data data);

    void releaseClient(long current);

    void connectionLost(GlobMultiClientImpl.ServerAddress serverAddress);

    void connectionOK(GlobMultiClientImpl.ServerAddress serverAddress, EndPointServeur endPointServeur);
}
