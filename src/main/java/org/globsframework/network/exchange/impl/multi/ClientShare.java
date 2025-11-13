package org.globsframework.network.exchange.impl.multi;

import java.nio.ByteBuffer;

public interface ClientShare {

    SendData[] getEndPointServers();

    boolean sendToAll(Data data);

    boolean sendToOne(Data data);

    boolean sendToActif(Data data);

    boolean sendToFirst(Data data);

    Data getFreeData();

    void release(Data data);

    void releaseClient(long current);

    void connectionLost(GlobMultiClientImpl.ServerAddress serverAddress);

    void connectionOK(GlobMultiClientImpl.ServerAddress serverAddress, EndPointServer endPointServer);

    ByteBuffer getFreeDirectBuffer();

    void releaseDirectBuffer(ByteBuffer readByteBuffer);
}
