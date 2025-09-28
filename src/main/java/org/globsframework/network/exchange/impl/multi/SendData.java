package org.globsframework.network.exchange.impl.multi;

public interface SendData {
    boolean send(Data data); //boolean false if immediate write failed
}
