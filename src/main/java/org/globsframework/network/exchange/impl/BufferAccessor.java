package org.globsframework.network.exchange.impl;

interface BufferAccessor {
    ExchangeGlobsServer.ResponseData getResponseData();

    void release(ExchangeGlobsServer.ResponseData responseData);
}
