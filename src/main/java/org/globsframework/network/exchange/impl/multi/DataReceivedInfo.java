package org.globsframework.network.exchange.impl.multi;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.network.exchange.GlobClient;
import org.globsframework.network.exchange.GlobMultiClient;

public record DataReceivedInfo(GlobClient.DataReceiver dataReceiver, GlobType receiveType) {
}
