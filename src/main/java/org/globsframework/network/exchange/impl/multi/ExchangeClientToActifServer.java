package org.globsframework.network.exchange.impl.multi;

class ExchangeClientToActifServer extends AbstractExchangeClientToServer {
    private final ClientShare clientShare;

    public ExchangeClientToActifServer(long streamId, ClientShare clientShare, GlobMultiClientImpl.AckMgt ackMgt) {
        super(streamId, clientShare, ackMgt);
        this.clientShare = clientShare;
    }

    @Override
    boolean send(Data data) {
        return clientShare.sendToActif(data);
    }
}
