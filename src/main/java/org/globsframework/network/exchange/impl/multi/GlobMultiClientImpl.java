package org.globsframework.network.exchange.impl.multi;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.network.exchange.Exchange;
import org.globsframework.network.exchange.GlobMultiClient;
import org.globsframework.network.exchange.impl.CommandId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class GlobMultiClientImpl implements GlobMultiClient, ClientShare, EndPointServeur.RequestAccess {
    private static final Logger log = LoggerFactory.getLogger(GlobMultiClientImpl.class);
    private final Lock serverAndConnectionLock = new ReentrantLock(); // prevent mixing adding new server and receiveing a new request
    private final Condition condition = serverAndConnectionLock.newCondition();
    private final AtomicLong lastStreamId;
    private final Executor executorService;
    private final Map<Long, ClientConnectionInfo> requests = new ConcurrentHashMap<>();
    private final Selector selector;
    private final int maxMessageSize;
    private final Map<ServerAddress, EndPointServeur> connecting = new ConcurrentHashMap<>();
    private final Map<ServerAddress, Endpoint> serverAddresses = new HashMap<>();
    private final Map<ServerAddress, EndPointServeur> actifEndPoints = new HashMap<>();
    private final Set<ServerAddress> pendingServerAddresses = new HashSet<>();
    private final Deque<Data> freeSendableData = new ConcurrentLinkedDeque<>();
    private SendData[] actifServer = new SendData[0];

    public GlobMultiClientImpl(int maxMessageSize) throws IOException {
        this.maxMessageSize = maxMessageSize;
        lastStreamId = new AtomicLong(0);
        freeSendableData.add(new Data(maxMessageSize, this::release));
        freeSendableData.add(new Data(maxMessageSize, this::release));
        executorService = Executors.newThreadPerTaskExecutor(Executors.defaultThreadFactory());
        selector = Selector.open();
        executorService.execute(this::flushPendingWrite);
        executorService.execute(this::connectServer);
    }

    private void connectServer() {
        serverAndConnectionLock.lock();
        try {
            while (true) {
                if (pendingServerAddresses.isEmpty()) {
                    condition.await();
                }
                Set<ServerAddress> dup = new HashSet<>(pendingServerAddresses);
                pendingServerAddresses.clear();
                for (ServerAddress serverAddress : dup) {
                    try {
                        if (!connecting.containsKey(serverAddress)) {
                            final EndPointServeur endPointServeur = new EndPointServeur(serverAddress, this::addPendingWrite, this, this, executorService);
                            connecting.put(serverAddress, endPointServeur);
                        }
                    } catch (IOException e) {
                        pendingServerAddresses.add(serverAddress);
                    }
                }
                condition.signalAll();
                condition.await(5, TimeUnit.SECONDS); // prevent looping top frequently on the pending server not responding
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            serverAndConnectionLock.unlock();
        }
    }

    @Override
    public void ack(long streamId, int requestId) {
        final ClientConnectionInfo exchangeClientSide = requests.get(streamId);
        if (exchangeClientSide != null) {
            exchangeClientSide.ackMgt.received(requestId);
        }
    }

    @Override
    public void error(long streamId, int requestId, String errorMessage) {
        final ClientConnectionInfo exchangeClientSide = requests.get(streamId);
        if (exchangeClientSide != null) {
            exchangeClientSide.ackMgt.error(requestId, errorMessage);
        }
    }

    @Override
    public void close(long streamId) { // closed recieved from server
        final ClientConnectionInfo clientConnectionInfo = requests.get(streamId);
        if (clientConnectionInfo != null) {
            clientConnectionInfo.ackMgt().close();
            clientConnectionInfo.dataReceivedInfo().dataReceiver().close();
        }
    }

    @Override
    public DataReceivedInfo dataReceived(long streamId, int requestId) {
        final ClientConnectionInfo clientConnectionInfo = requests.get(streamId);
        if (clientConnectionInfo != null) {
            return clientConnectionInfo.dataReceivedInfo;
        }
        return null;
    }

    interface AddPendingWrite {
        SetPendingWrite add(SocketChannel channel, PendingWrite pendingWrite);
    }

    interface SetPendingWrite {
        void set();
    }

    record PendingSelection(SocketChannel channel, PendingWrite pendingWrite) {
    }

    synchronized SetPendingWrite addPendingWrite(SocketChannel channel, PendingWrite pendingWrite) {
        try {
            final SelectionKey selectionKey = channel.register(selector, SelectionKey.OP_WRITE, new PendingSelection(channel, pendingWrite));
            return new SetPendingWriteForSelector(selectionKey);
        } catch (ClosedChannelException e) {
            throw new RuntimeException(e);
        }
    }

    void flushPendingWrite() {
        try {
            while (true) {
                final int select = selector.select();
                if (select != 0) {
                    final Set<SelectionKey> selectionKeys = selector.selectedKeys();
                    for (SelectionKey selectionKey : selectionKeys) {
                        if (selectionKey.isWritable()) {
                            final PendingSelection p = (PendingSelection) selectionKey.attachment();
                            PendingWrite.DataWithByteBuffer current = p.pendingWrite().getCurrent();
                            while (true) {
                                try {
                                    p.channel().write(current.buffer());
                                } catch (IOException e) {
                                    current.data().release();
                                    selectionKey.cancel();
                                    p.pendingWrite.close();
                                    break;
                                }
                                if (current.buffer().hasRemaining()) {
                                    log.debug("GlobMultiClientImpl.flushPendingWrite has remaining" );
                                    break;
                                }
                                current.data().release();
                                current = p.pendingWrite().releaseCurrentAndGetNext();
                                if (current == null) {
                                    log.debug("GlobMultiClientImpl.flushPendingWrite no more remaining" );
                                    selectionKey.interestOps(0);
                                    break;
                                }
                            }
                        }
                    }
                    selectionKeys.clear();
                }
            }
        } catch (IOException e) {
            log.error("Error in flushPending", e);
            throw new RuntimeException(e);
        }
    }

    public record ServerAddress(String host, int port) {
    }

    public int waitForActifServer(int count, int timeoutInMSEC) {
        final long endAt = System.currentTimeMillis() + timeoutInMSEC;
        serverAndConnectionLock.lock();
        try {
            while (actifEndPoints.size() < count && System.currentTimeMillis() < endAt) {
                condition.await(Math.max(1, System.currentTimeMillis() - endAt), TimeUnit.MILLISECONDS);
            }
            return actifEndPoints.size();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            serverAndConnectionLock.unlock();
        }
    }

    @Override
    public Endpoint add(String host, int port) throws IOException {
        final ServerAddress serverAddress = new ServerAddress(host, port);
        serverAndConnectionLock.lock();
        try {
            if (serverAddresses.containsKey(serverAddress)) {
                return serverAddresses.get(serverAddress);
            }
            else {
                pendingServerAddresses.add(serverAddress);
                condition.signalAll();
                final Endpoint endpoint = new Endpoint() {
                    @Override
                    public void unregister() {
                        serverAndConnectionLock.lock();
                        try {
                            serverAddresses.remove(serverAddress);
                            pendingServerAddresses.remove(serverAddress);
                            final EndPointServeur remove = actifEndPoints.remove(serverAddress);
                            actifServer = actifEndPoints.values().toArray(new EndPointServeur[0]);
                            if (remove != null) {
                                remove.shutdown();
                            }
                        } finally {
                            serverAndConnectionLock.unlock();
                        }
                    }
                };
                serverAddresses.put(serverAddress, endpoint);
                return endpoint;
            }
        }finally {
            serverAndConnectionLock.unlock();
        }
    }

    private Data createConnectData(Long longExchangeClientSideEntry, String longExchangeClientSideEntry1, AckOption longExchangeClientSideEntry2) {
        final Data data = getFreeData();
        data.serializedOutput.write(-longExchangeClientSideEntry);
        data.serializedOutput.write(CommandId.NEW.id);
        data.serializedOutput.writeUtf8String(longExchangeClientSideEntry1);
        data.serializedOutput.write(longExchangeClientSideEntry2.opt);
        data.complete();
        data.incWriter();
        return data;
    }

    record ClientConnectionInfo(DataReceivedInfo dataReceivedInfo, String path, AckOption ackOption, AckMgt ackMgt,
                                AbstractExchangeClientToServer exchangeClientSide) {
    }

    @Override
    public Exchange connect(String path, DataReceiver dataReceiver, GlobType receiveType, AckOption ackOption, SendOption sendOption) {
        serverAndConnectionLock.lock();
        final AbstractExchangeClientToServer exchangeClientSide;
        try {
            final long streamId = lastStreamId.incrementAndGet();
            AckMgt ackMgt = ackOption == AckOption.NO_ACK ? NoAck.instance : new AckMgtImpl();
            final Data data = createConnectData(streamId, path, ackOption);
            for (SendData sendData : actifServer) {
                sendData.send(data);
            }
            data.release();
            DataReceivedInfo dataReceivedInfo = new DataReceivedInfo(dataReceiver, receiveType);
            exchangeClientSide = switch (sendOption) {
                case SEND_TO_ALL -> new ExchangeClientToAllServer(streamId, this, ackMgt);
                case SEND_TO_FIRST -> new ExchangeClientToFirstServer(streamId, this, ackMgt);
                case SEND_TO_ANY -> new ExchangeClientToRandomServer(streamId, this, ackMgt);
            };
            requests.put(streamId, new ClientConnectionInfo(dataReceivedInfo, path, ackOption, ackMgt, exchangeClientSide));
        } finally {
            serverAndConnectionLock.unlock();
        }
        return exchangeClientSide;
    }

    interface AckMgt {

        void close();

        CompletableFuture<Boolean> newAck(int lSeq);

        void received(int requestId);

        void error(int requestId, String errorMessage);
    }


    @Override
    public void sendToAll(Data data) {
        for (SendData endPointServeur : actifServer) {
            endPointServeur.send(data);
        }
    }

    @Override
    public void sendToOne(Data data) {
        final SendData[] copy = actifServer;
        final int length = copy.length;
        int random = (int) (Math.random() * length);
        for (int i = 0; i < length; i++) {
            if (copy[(i + random) % length].send(data)) {
                return;
            }
        }
    }

    @Override
    public void sendToFirst(Data data) {
        for (SendData endPointServeur : actifServer) {
            if (endPointServeur.send(data)) {
                return;
            }
        }
    }

    @Override
    public SendData[] getEndPointServers() {
        return actifServer;
    }

    public Data getFreeData() {
        Data sendableData = freeSendableData.poll();
        if (sendableData == null) {
            sendableData = new Data(maxMessageSize, this::release);
        }
        sendableData.reset();
        return sendableData;
    }

    public void release(Data data) {
        freeSendableData.offer(data);
    }

    @Override
    public void releaseClient(long current) {
        requests.remove(current);
    }

    @Override
    public void connectionLost(ServerAddress serverAddress) {
        serverAndConnectionLock.lock();
        try {
            actifEndPoints.remove(serverAddress);
            actifServer = actifEndPoints.values().toArray(new EndPointServeur[0]);
            pendingServerAddresses.add(serverAddress);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            serverAndConnectionLock.unlock();
        }
    }

    @Override
    public void connectionOK(ServerAddress serverAddress, EndPointServeur endPointServeur) {
        serverAndConnectionLock.lock();
        try {
            for (Map.Entry<Long, ClientConnectionInfo> longExchangeClientSideEntry : requests.entrySet()) {
                final Data data = createConnectData(longExchangeClientSideEntry.getKey(), longExchangeClientSideEntry.getValue().path,
                        longExchangeClientSideEntry.getValue().ackOption);
                if (!endPointServeur.send(data)){
                    data.release();
                    throw new RuntimeException("Could not send data to " + serverAddress);
                }
                data.release();
            }
            connecting.remove(serverAddress);
            pendingServerAddresses.remove(serverAddress);
            actifEndPoints.put(serverAddress, endPointServeur);
            actifServer = actifEndPoints.values().toArray(new EndPointServeur[0]);
        } finally {
            serverAndConnectionLock.unlock();
        }
    }

    private class SetPendingWriteForSelector implements SetPendingWrite {
        private final SelectionKey selectionKey;

        public SetPendingWriteForSelector(SelectionKey selectionKey) {
            this.selectionKey = selectionKey;
        }

        @Override
        public void set() {
            selectionKey.interestOps(SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }
}
