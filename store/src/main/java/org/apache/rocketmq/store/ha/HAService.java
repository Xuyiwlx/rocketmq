/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;

/*
* RocketMQ 主从同步核心实现类
* */
public class HAService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AtomicInteger connectionCount = new AtomicInteger(0);

    private final List<HAConnection> connectionList = new LinkedList<>();

    private final AcceptSocketService acceptSocketService;

    private final DefaultMessageStore defaultMessageStore;

    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    private final GroupTransferService groupTransferService;

    private final HAClient haClient;

    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        this.acceptSocketService =
            new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService();
        this.haClient = new HAClient();
    }

    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
            result
                && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    public void notifyTransferSome(final long offset) {
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    // public void notifyTransferSome() {
    // this.groupTransferService.notifyTransferSome();
    // }

    public void start() throws Exception {
        // 主服务器启动,并在特定端口上监听从服务器的连接
        this.acceptSocketService.beginAccept();
        // 从服务器主动连接主服务器,主服务器接收客户端的连接,
        // 并建立相关TCP连接
        this.acceptSocketService.start();
        // 从服务器主动向主服务器发送待拉取消息偏移量
        // 主服务器解析请求并返回消息给从服务器
        this.groupTransferService.start();
        // 从服务器保存消息并继续发送新的消息同步请求
        this.haClient.start();
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * Listens to slave connections to create {@link HAConnection}.
     * HA Master 端监听 Slave 连接
     * 接收 Slave 节点连接
     */
    class AcceptSocketService extends ServiceThread {
        // Broker 服务监听套接字(本地IP+端口号)
        private final SocketAddress socketAddressListen;
        // 服务端 Socket 通道,基于NIO
        private ServerSocketChannel serverSocketChannel;
        // 时间选择器,基于NIO
        private Selector selector;

        public AcceptSocketService(final int port) {
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * Starts listening to slave connections.
         *
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            // 创建 serverSocketChannel
            this.serverSocketChannel = ServerSocketChannel.open();
            // 创建 selector
            this.selector = RemotingUtil.openSelector();
            // 设置 TCP reuseAddress
            this.serverSocketChannel.socket().setReuseAddress(true);
            // 绑定监听端口
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            // 设置为非阻塞模式
            this.serverSocketChannel.configureBlocking(false);
            // 注册 OP_ACCEPT(连接事件)
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 选择器每1s处理一次连接就绪事件
                    this.selector.select(1000);
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) { // 连接事件就绪
                        for (SelectionKey k : selected) {
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                // 创建 SocketChannel
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());

                                    try {
                                        // 为每一个连接创建一个 HAConnection 对象
                                        // 负责M-S数据同步逻辑
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        conn.start();
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * GroupTransferService Service
     * 主从同步通知实现类
     * 如果是同步主从模式,消息发送者将消息刷写到磁盘后,
     * 需要继续等待新数据被传输到从服务器
     */
    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
        private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new ArrayList<>();
        private volatile List<CommitLog.GroupCommitRequest> requestsRead = new ArrayList<>();

        public synchronized void putRequest(final CommitLog.GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            if (hasNotified.compareAndSet(false, true)) {
                waitPoint.countDown(); // notify
            }
        }

        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        private void swapRequests() {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        private void doWaitTransfer() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                        // 主从同步完成
                        // Slave 中已成功复制的最大偏移量是否大于等于
                        // 消息生产者发送消息后,消息服务端返回下一条消息的起始偏移量
                        boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        // 每一个任务在一批任务中循环判断5次
                        for (int i = 0; !transferOK && i < 5; i++) {
                            // 等待1s再次判断
                            this.notifyTransferObject.waitForRunning(1000);
                            transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        }

                        if (!transferOK) {
                            log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                        }

                        // 唤醒消息发送线程
                        req.wakeupCustomer(transferOK);
                    }

                    this.requestsRead.clear();
                }
            }
        }

        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10);
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }

    /*
    * 主从同步Slave端核心实现类
    * 对 Master 节点连接、读写数据
    * */
    class HAClient extends ServiceThread {
        // Socket 读缓存区大小
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
        // master 地址
        private final AtomicReference<String> masterAddress = new AtomicReference<>();
        // Slave 向 Master 发起主从同步的拉取偏移量
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
        // 网络传输通道
        private SocketChannel socketChannel;
        // NIO 事件选择器
        private Selector selector;
        // 上一次写入时间戳
        private long lastWriteTimestamp = System.currentTimeMillis();
        // 反馈 Slave 当前的偏移量
        private long currentReportedOffset = 0;
        // 本次已处理读缓存区的指针
        private int dispatchPostion = 0;
        // 读缓存区,大小为4M
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        // 读缓存区备份,与 BufferRead 进行交换
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }

        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        /*
        * 判断是否需要向 Master 反馈当前待拉取偏移量
        * */
        private boolean isTimeToReportOffset() {
            long interval =
                HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            // Master 与 Slave 的 HA 心跳发送间隔默认为5s
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                .getHaSendHeartbeatInterval();

            return needHeart;
        }

        /*
        * 向 Master 服务器反馈待拉取偏移量
        * 1.对于 Slave 来说,是发送下次待拉取消息偏移量
        * 2.对于 Master 来说,既可以认为是本次请求拉取的消息偏移量
        *   也可以理解为 Slave 的消息同步 ACK 确认消息
        * */
        private boolean reportSlaveMaxOffset(final long maxOffset) {
            this.reportOffset.position(0);
            // 待写入字节长度
            this.reportOffset.limit(8);
            // 写入待拉取偏移量
            this.reportOffset.putLong(maxOffset);
            // 从写模式切换到读模式
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            // 循环判断写入通道
            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                        + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            return !this.reportOffset.hasRemaining();
        }

        private void reallocateByteBuffer() {
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPostion;
            if (remain > 0) {
                this.byteBufferRead.position(this.dispatchPostion);

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            this.swapByteBuffer();

            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            this.dispatchPostion = 0;
        }

        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }

        /*
        * 处理网络读请求,即处理从 Master 服务器传回的消息数据
        * */
        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;
            // 循环判断 byteBufferRead 是否还有剩余空间
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    // 将通道中的数据读入到读缓冲区中
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) { // 读取到的字节数大于0
                        // 更新最后一次写入时间戳
                        lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
                        // 重置读到0字节的次数
                        readSizeZeroTimes = 0;
                        // 对读取到的数据进行处理
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0) { // 连续3次从网络通道读取到0个字节
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else { // 读取到的字节数小于0
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) { // 发生IO异常
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }

        /*
        * 读取 Master 传输的 CommitLog 数据,并返回是否异常
        * 如果读取到数据,写入 CommitLog
        * 异常原因:
        *    1.Master 传输来的数据 offset 不等于 Slave 的 CommitLog 数据最大 offset
        *    2.上报到 Master 进度失败
        * */
        private boolean dispatchReadRequest() {
            final int msgHeaderSize = 8 + 4; // phyoffset + size
            int readSocketPos = this.byteBufferRead.position();

            while (true) {
                // 读取到请求
                int diff = this.byteBufferRead.position() - this.dispatchPostion;
                if (diff >= msgHeaderSize) {
                    // 读取masterPhyOffset、bodySize
                    // 使用 dispatchPostion 原因:处理数据'粘包',导致数据读取不完整
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPostion);
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPostion + 8);

                    // 读取Slave 的 CommitLog 数据最大 offset
                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    if (slavePhyOffset != 0) {
                        // 校验是否相等
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }

                    // 读取到消息
                    if (diff >= (msgHeaderSize + bodySize)) {
                        // 写入 CommitLog
                        byte[] bodyData = new byte[bodySize];
                        this.byteBufferRead.position(this.dispatchPostion + msgHeaderSize);
                        this.byteBufferRead.get(bodyData);

                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);

                        // 设置处理到的位置
                        this.byteBufferRead.position(readSocketPos);
                        this.dispatchPostion += msgHeaderSize + bodySize;

                        // 上报到 Master 进度
                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        continue;
                    }
                }

                // 空间写满,重新分配空间
                if (!this.byteBufferRead.hasRemaining()) {
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
            if (currentPhyOffset > this.currentReportedOffset) {
                this.currentReportedOffset = currentPhyOffset;
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            return result;
        }

        private boolean connectMaster() throws ClosedChannelException {
            if (null == socketChannel) { // socketChannel 为空,则尝试连接 Master
                String addr = this.masterAddress.get();
                if (addr != null) { // Master 地址不为空

                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        // 建立到 Master 的 TCP 连接
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            // 注册 OP_READ(网络读事件)
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }
                // 初始化 currentReportedOffset, 为 commitlog 文件的最大偏移量
                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
                // 上次写入时间戳为当前时间戳
                this.lastWriteTimestamp = System.currentTimeMillis();
            }

            return this.socketChannel != null;
        }

        private void closeMaster() {
            if (null != this.socketChannel) {
                try {

                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }

                    this.socketChannel.close();

                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                this.lastWriteTimestamp = 0;
                this.dispatchPostion = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // Slave 服务器连接 Master 服务器
                    if (this.connectMaster()) {

                        if (this.isTimeToReportOffset()) { // 需要向 Master 反馈当前待拉取偏移量
                            // 向 Master 反馈拉取偏移量
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            if (!result) {
                                this.closeMaster();
                            }
                        }

                        // 进行事件选择,执行间隔1s
                        this.selector.select(1000);

                        // 处理读取事件(处理 Master 传输 Slave 的 CommitLog 数据)
                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            this.closeMaster();
                        }

                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        // Master 过久未返回数据,关闭连接
                        long interval =
                            HAService.this.getDefaultMessageStore().getSystemClock().now()
                                - this.lastWriteTimestamp;
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }
        // private void disableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops &= ~SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }
        // private void enableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops |= SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }
}
