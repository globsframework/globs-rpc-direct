package org.globsframework.network.exchange.impl.multi;

class ExchangeClientToFirstServer extends AbstractExchangeClientToServer {
    private final ClientShare clientShare;

    public ExchangeClientToFirstServer(long streamId, ClientShare clientShare, GlobMultiClientImpl.AckMgt ackMgt) {
        super(streamId, clientShare, ackMgt);
        this.clientShare = clientShare;
    }

    @Override
    boolean send(Data data) {
        return clientShare.sendToFirst(data);
    }
}
