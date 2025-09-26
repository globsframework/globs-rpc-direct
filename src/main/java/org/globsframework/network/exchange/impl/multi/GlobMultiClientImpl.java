package org.globsframework.network.exchange.impl.multi;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.network.exchange.Exchange;
import org.globsframework.network.exchange.GlobClient;
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
    private final Lock addRemoveServerLock = new ReentrantLock();
    private final Condition condition = addRemoveServerLock.newCondition();
    private final AtomicLong lastStreamId;
    private final Executor executorService;
    private final Map<Long, ClientConnectionInfo> requests = new ConcurrentHashMap<>();
    private final Selector selector;
    private final int maxMessageSize;
    private final List<EndPointServeur> endPointServeurs = new CopyOnWriteArrayList<>();
    private final Map<ServerAddress, Endpoint> serverAddresses = new HashMap<>();
    private final Map<ServerAddress, EndPointServeur> actifEndPoints = new HashMap<>();
    private final Set<ServerAddress> pendingServerAddresses = new HashSet<>();
    private final Deque<Data> freeSendableData = new ConcurrentLinkedDeque<>();

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
        addRemoveServerLock.lock();
        try {
            while (true) {
                if (pendingServerAddresses.isEmpty()) {
                    condition.await();
                }
                Set<ServerAddress> dup = new HashSet<>(pendingServerAddresses);
                pendingServerAddresses.clear();
                for (ServerAddress serverAddress : dup) {
                    try {
                        tryAdd(serverAddress);
                    } catch (IOException e) {
                        pendingServerAddresses.add(serverAddress);
                    }
                }
                condition.signalAll();
                condition.await(5, TimeUnit.SECONDS); // prevent looping on pending server not responding
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            addRemoveServerLock.unlock();
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
    public void close(long streamId) {
// on server close ?
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
                                    break;
                                }
                                current.data().release();
                                current = p.pendingWrite().releaseCurrentAndGetNext();
                                if (current == null) {
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

    public int waitForServer(int count, int timeoutInMSEC) {
        final long endAt = System.currentTimeMillis() + timeoutInMSEC;
        addRemoveServerLock.lock();
        try {
            while (endPointServeurs.size() < count && System.currentTimeMillis() < endAt) {
                condition.await(Math.max(1, System.currentTimeMillis() - endAt), TimeUnit.MILLISECONDS);
            }
            return endPointServeurs.size();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            addRemoveServerLock.unlock();
        }
    }

    @Override
    public Endpoint add(String host, int port) throws IOException {
        final ServerAddress serverAddress = new ServerAddress(host, port);
        addRemoveServerLock.lock();
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
                        addRemoveServerLock.lock();
                        try {
                            serverAddresses.remove(serverAddress);
                            pendingServerAddresses.remove(serverAddress);
                        } finally {
                            addRemoveServerLock.unlock();
                        }
                    }
                };
                serverAddresses.put(serverAddress, endpoint);
                return endpoint;
            }
        }finally {
            addRemoveServerLock.unlock();
        }
    }

    public Endpoint tryAdd(ServerAddress serverAddress) throws IOException {
        final EndPointServeur endPointServeur = new EndPointServeur(serverAddress, this::addPendingWrite, this, this, executorService);

        serverAndConnectionLock.lock();
        try {
            for (Map.Entry<Long, ClientConnectionInfo> longExchangeClientSideEntry : requests.entrySet()) {
                final Data data = createConnectData(longExchangeClientSideEntry.getKey(), longExchangeClientSideEntry.getValue().path,
                        longExchangeClientSideEntry.getValue().option);
                endPointServeur.send(data);
                data.release();
            }
            endPointServeurs.add(endPointServeur);
            actifEndPoints.put(serverAddress, endPointServeur);
        } finally {
            serverAndConnectionLock.unlock();
        }
        return endPointServeur;
    }

    private Data createConnectData(Long longExchangeClientSideEntry, String longExchangeClientSideEntry1, Option longExchangeClientSideEntry2) {
        final Data data = getFreeData();
        data.serializedOutput.write(-longExchangeClientSideEntry);
        data.serializedOutput.write(CommandId.NEW.id);
        data.serializedOutput.writeUtf8String(longExchangeClientSideEntry1);
        data.serializedOutput.write(longExchangeClientSideEntry2.opt);
        data.complete();
        data.incWriter();
        return data;
    }

    record ClientConnectionInfo(DataReceivedInfo dataReceivedInfo, String path, Option option, AckMgt ackMgt,
                                MultiExchangeClientSide exchangeClientSide) {
    }

    @Override
    public Exchange connect(String path, DataReceiver dataReceiver, GlobType receiveType, Option option) {
        serverAndConnectionLock.lock();
        final MultiExchangeClientSide exchangeClientSide;
        try {
            final long streamId = lastStreamId.incrementAndGet();
            AckMgt ackMgt = option == GlobClient.Option.NO_ACK ? NoAck.instance : new AckMgtImpl();
            final Data data = createConnectData(streamId, path, option);
            for (EndPointServeur endPointServeur : endPointServeurs) {
                endPointServeur.send(data);
            }
            data.release();
            DataReceivedInfo dataReceivedInfo = new DataReceivedInfo(dataReceiver, receiveType);
            exchangeClientSide = new MultiExchangeClientSide(streamId, this, ackMgt);
            requests.put(streamId, new ClientConnectionInfo(dataReceivedInfo, path, option, ackMgt, exchangeClientSide));
        } finally {
            serverAndConnectionLock.unlock();
        }
        return exchangeClientSide;
    }

    interface AckMgt {

        void close();

        CompletableFuture<Boolean> newAck(int lSeq);

        void received(int requestId);
    }

    @Override
    public List<SendData> getEndPointServeurs() {
        return (List<SendData>) (List<?>) endPointServeurs;
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
    public void close(ServerAddress serverAddress) {
        addRemoveServerLock.lock();
        try {
            final EndPointServeur remove = actifEndPoints.remove(serverAddress);
            endPointServeurs.remove(remove);
            pendingServerAddresses.remove(serverAddress);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            addRemoveServerLock.unlock();
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
