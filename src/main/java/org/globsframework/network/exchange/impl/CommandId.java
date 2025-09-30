package org.globsframework.network.exchange.impl;

public enum CommandId {
    CLOSE(1),
    ACK(2),
    NEW(3),
    CLOSE_STREAM(4),
    ERROR_DESERIALISATION(5),
    ERROR_APPLICATIVE(6)
    ;

    public final short id;

    CommandId(int id) {
        this.id = (short) id;
    }
}
