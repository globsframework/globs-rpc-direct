package org.globsframework.network.exchange.impl.multi;

class ExchangeClientToRandomServer extends AbstractExchangeClientToServer {
    private final ClientShare clientShare;

    public ExchangeClientToRandomServer(long streamId, ClientShare clientShare, GlobMultiClientImpl.AckMgt ackMgt) {
        super(streamId, clientShare, ackMgt);
        this.clientShare = clientShare;
    }

    @Override
    boolean send(Data data) {
        return clientShare.sendToOne(data);
    }
}
