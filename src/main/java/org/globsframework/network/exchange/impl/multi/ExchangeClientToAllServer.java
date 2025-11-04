package org.globsframework.network.exchange.impl.multi;

class ExchangeClientToAllServer extends AbstractExchangeClientToServer {
    private final ClientShare clientShare;

    public ExchangeClientToAllServer(long streamId, ClientShare clientShare, GlobMultiClientImpl.AckMgt ackMgt) {
        super(streamId, clientShare, ackMgt);
        this.clientShare = clientShare;
    }

    @Override
    boolean send(Data data) {
        return clientShare.sendToAll(data);
    }
}
