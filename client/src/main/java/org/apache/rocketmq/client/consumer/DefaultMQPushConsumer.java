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
package org.apache.rocketmq.client.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.hook.ConsumeMessageTraceHookImpl;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * In most scenarios, this is the mostly recommended class to consume messages.
 * 在大多数情况下，这是最为推荐的用作消息消费的类
 * </p>
 *
 * Technically speaking, this push client is virtually a wrapper of the underlying pull service. Specifically, on
 * arrival of messages pulled from brokers, it roughly invokes the registered callback handler to feed the messages.
 * 技术上来说，推模式的客户端底层事实上是一个拉服务，具体来说，当从代理中提取的消息到达时，它大致是调用注册的回调处理程序来提供消息。
 * </p>
 *
 * See quickstart/Consumer in the example module for a typical usage.
 * </p>
 *
 * <p>
 * <strong>Thread Safety:</strong> After initialization, the instance can be regarded as thread-safe.
 * </p>
 */
public class DefaultMQPushConsumer extends ClientConfig implements MQPushConsumer {

    private final InternalLogger log = ClientLogger.getLog();

    /**
     * Internal implementation. Most of the functions herein are delegated to it.
     * 内部实现，此处大多数的只能都委托给他
     */
    protected final transient DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    /**
     * Consumers of the same role is required to have exactly same subscriptions and consumerGroup to correctly achieve
     * load balance. It's required and needs to be globally unique.
     * 相同角色的消费这必须要有完全相同的订阅和消费分组才能够正确的实现负载均衡，且必须全局唯一
     * </p>
     *  消费者所属组
     * See <a href="http://rocketmq.apache.org/docs/core-concept/">here</a> for further discussion.
     */
    private String consumerGroup;

    /**
     * Message model defines the way how messages are delivered to each consumer clients.
     * 消息模型定义了如何将消息传递给每个客户机。
     * </p>
     *
     * RocketMQ supports two message models: clustering and broadcasting. If clustering is set, consumer clients with
     * the same {@link #consumerGroup} would only consume shards of the messages subscribed, which achieves load
     * balances; Conversely, if the broadcasting is set, each consumer client will consume all subscribed messages
     * separately.
     * RocketMq支持两种消息模型：集群和广播模式。
     * 如果设置了集群，具有相同ConsumerGroup的消费客户端将只有一个消费订阅的消息，从而实现负载平衡；
     * 相反，如果设置了广播，则每个消费客户端将分别消费所订阅的消息。
     * </p>
     *
     *
     * This field defaults to clustering.
     * 默认为集群模式
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;

    /**
     * Consuming point on consumer booting.
     * 消费者启动的消费要点
     * </p>
     *
     * There are three consuming points:
     * 这里有三个要点
     * <ul>
     * <li>
     * <code>CONSUME_FROM_LAST_OFFSET</code>: consumer clients pick up where it stopped previously.
     * 消费者将从前期停止的地方挂载
     * If it were a newly booting up consumer client, according aging of the consumer group, there are two
     * cases:
     * 如果有新的消费者端启动，根据消费者组的启动时长，这里有两种情况：
     * <ol>
     * <li>
     * if the consumer group is created so recently that the earliest message being subscribed has yet
     * expired, which means the consumer group represents a lately launched business, consuming will
     * start from the very beginning;
     * 如果消费者组是最近创建的,最早订阅的消息还没过期，新启动的业务消费群将从头进行消费
     * </li>
     * <li>
     * if the earliest message being subscribed has expired, consuming will start from the latest
     * messages, meaning messages born prior to the booting timestamp would be ignored.
     * 如果最早订阅的消息已经过期了，消费将从最尾部开始消费，意味着在启动时间戳之前生成的消息将被忽略。
     * </li>
     * </ol>
     * </li>
     * <li>
     * <code>CONSUME_FROM_FIRST_OFFSET</code>: Consumer client will start from earliest messages available.
     * 消费者将从最早可用的消息开始消费
     * </li>
     * <li>
     * <code>CONSUME_FROM_TIMESTAMP</code>: Consumer client will start from specified timestamp, which means
     * messages born prior to {@link #consumeTimestamp} will be ignored
     * 消费者将将从指定时间消费，在此之前的消息将会被忽略
     * </li>
     * </ul>
     * 根据消息进度从消息服务器拉取不到消息时重新计算消费策略
     * CONSUME_FROM_LAST_OFFSET : 从队列当前最大偏移量开始消费
     * CONSUME_FROM_FIRST_OFFSET : 从队列当前最小偏移量开始消费
     * CONSUME_FROM_TIMESTAMP : 从指定的时间戳开始消费
     * 注意:如果消息进度服务 OffsetStore 读取到 MessageQueue 中的偏移量不小于0,则使用读取到的偏移量;
     * 只有在读到的偏移量小于0时,上述策略才会生效
     */
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    /**
     * Backtracking consumption time with second precision. Time format is
     * 20131223171201
     * 回溯消费时间会精确到秒，时间格式为20131223171201
     * <br>
     * Implying Seventeen twelve and 01 seconds on December 23, 2013 year<br>
     * Default backtracking consumption time Half an hour ago.
     * 默认回溯消费时间为半小时前
     */
    private String consumeTimestamp = UtilAll.timeMillisToHumanString3(System.currentTimeMillis() - (1000 * 60 * 30));

    /**
     * Queue allocation algorithm specifying how message queues are allocated to each consumer clients.
     * 队列分配算法，指定如何将消息队列分配给每个消费者客户端
     * 集群模式下,消息队列负载策略
     */
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy;

    /**
     * Subscription relationship
     * 订阅关系（主题->订阅表达式）
     */
    private Map<String /* topic */, String /* sub expression */> subscription = new HashMap<String, String>();

    /**
     * Message listener
     * 消息业务监听器
     */
    private MessageListener messageListener;

    /**
     * Offset Storage
     * 消息消费进度存储器
     */
    private OffsetStore offsetStore;

    /**
     * Minimum consumer thread number
     * 最小消费线程数
     */
    private int consumeThreadMin = 20;

    /**
     * Max consumer thread number
     * 最大消费线程数
     * 由于消费者线程池使用无界队列,所以消费者线程个数最多只有 consumeThreadMin 个
     */
    private int consumeThreadMax = 64;

    /**
     * Threshold for dynamic adjustment of the number of thread pool
     * 动态调整线程数量阀值
     */
    private long adjustThreadPoolNumsThreshold = 100000;

    /**
     * Concurrently max span offset.it has no effect on sequential consumption
     * 并发的最大跨距偏移量。它对连续消耗没有影响。
     * 并发消息消费时处理队列最大跨度,默认2000
     * 表示如果消息处理队列中偏移量最大的消息与
     * 偏移量最小的消息的跨度超过2000,则延迟50ms后再拉取消息
     */
    private int consumeConcurrentlyMaxSpan = 2000;

    /**
     * Flow control threshold on queue level, each message queue will cache at most 1000 messages by default,
     * Consider the {@code pullBatchSize}, the instantaneous value may exceed the limit
     * 队列级别的流控阈值，每个消息队列默认最多可缓存1000条消息，考虑到pullBatchSize,瞬时值可能超过此限制
     * 每1000次流控后打印流控日志
     */
    private int pullThresholdForQueue = 1000;

    /**
     * Limit the cached message size on queue level, each message queue will cache at most 100 MiB messages by default,
     * Consider the {@code pullBatchSize}, the instantaneous value may exceed the limit
     * 在队列级别上缓存消息大小的限制，每隔消息默认最大可缓存100M，考虑到pullBatchSize,瞬时值可能超过此限制
     * <p>
     * The size of a message only measured by message body, so it's not accurate
     */
    private int pullThresholdSizeForQueue = 100;

    /**
     * Flow control threshold on topic level, default value is -1(Unlimited)
     * 主题级别的流控阀值，默认值为-1(不限制)
     * <p>
     * The value of {@code pullThresholdForQueue} will be overwrote and calculated based on
     * {@code pullThresholdForTopic} if it is't unlimited
     * 如果pullThresholdForTopic不是无限制的话，值pullThresholdForQueue会被基于pullThresholdForTopic计算处的值覆盖
     * <p>
     * For example, if the value of pullThresholdForTopic is 1000 and 10 message queues are assigned to this consumer,
     * then pullThresholdForQueue will be set to 100
     * 举例，如果pullThresholdForTopic是1000，且已将10个消息队列分配给此消费者，则pullThresholdForQueue将被设定未100
     */
    private int pullThresholdForTopic = -1;

    /**
     * Limit the cached message size on topic level, default value is -1 MiB(Unlimited)
     * 主题级别的缓存消息大小限制，默认-1（不限制）
     * <p>
     * The value of {@code pullThresholdSizeForQueue} will be overwrote and calculated based on
     * {@code pullThresholdSizeForTopic} if it is't unlimited
     * 如果pullThresholdSizeForTopic不是无限制的话，值pullThresholdSizeForQueue会被基于pullThresholdSizeForTopic计算出的值覆盖
     * <p>
     * For example, if the value of pullThresholdSizeForTopic is 1000 MiB and 10 message queues are
     * assigned to this consumer, then pullThresholdSizeForQueue will be set to 100 MiB
     *  举例，如果pullThresholdForTopic是1000M，且已将10个消息队列分配给此消费者，则pullThresholdForQueue将被设定未100M
     */
    private int pullThresholdSizeForTopic = -1;

    /**
     * Message pull Interval
     * 消息拉取间隔
     * 推模式下拉取任务间隔时间,默认一次拉取任务完成继续拉取
     */
    private long pullInterval = 0;

    /**
     * Batch consumption size
     * 批量消费大小
     * 消息并发消费时,一次消费消息条数
     */
    private int consumeMessageBatchMaxSize = 1;

    /**
     * Batch pull size
     * 批量拉取大小
     * 每次消息拉取所拉取的条数,默认32条
     */
    private int pullBatchSize = 32;

    /**
     * Whether update subscription relationship when every pull
     * 是否每次拉取消息都更新订阅关系,默认为false
     */
    private boolean postSubscriptionWhenPull = false;

    /**
     * Whether the unit of subscription group
     * 是否单位模式来订阅组
     */
    private boolean unitMode = false;

    /**
     * Max re-consume times. -1 means 16 times.
     * 最大消费重试次数, -1 代表 16次
     * </p>
     *
     * If messages are re-consumed more than {@link #maxReconsumeTimes} before success, it's be directed to a deletion
     * queue waiting.
     * 成功前如果重复消费次数超过此数值，它将被定向到一个正在等待删除的队列中
     */
    private int maxReconsumeTimes = -1;

    /**
     * Suspending pulling time for cases requiring slow pulling like flow-control scenario.
     * 为像流量控制场景，需要缓慢拉取的情况指定可延缓拉取的时间。
     * 延迟将该队列的消息提交到消费者线程的等待时间,默认延迟1s
     */
    private long suspendCurrentQueueTimeMillis = 1000;

    /**
     * Maximum amount of time in minutes a message may block the consuming thread.
     * 消息可能阻塞消耗线程的最长时间（分钟）
     * 消息消费超时时间,默认为15分钟
     */
    private long consumeTimeout = 15;

    /**
     * Interface of asynchronous transfer data
     * 异步传输数据接口
     */
    private TraceDispatcher traceDispatcher = null;

    /**
     * Default constructor.
     */
    public DefaultMQPushConsumer() {
        this(MixAll.DEFAULT_CONSUMER_GROUP, null, new AllocateMessageQueueAveragely());
    }

    /**
     * Constructor specifying consumer group, RPC hook and message queue allocating algorithm.
     *
     * @param consumerGroup Consume queue.
     * @param rpcHook RPC hook to execute before each remoting command.
     * @param allocateMessageQueueStrategy Message queue allocating algorithm.
     */
    public DefaultMQPushConsumer(final String consumerGroup, RPCHook rpcHook,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.consumerGroup = consumerGroup;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(this, rpcHook);
    }

    /**
     * Constructor specifying consumer group, RPC hook, message queue allocating algorithm, enabled msg trace flag and customized trace topic name.
     *
     * @param consumerGroup Consume queue.
     * @param rpcHook RPC hook to execute before each remoting command.
     * @param allocateMessageQueueStrategy message queue allocating algorithm.
     * @param enableMsgTrace Switch flag instance for message trace.
     * @param customizedTraceTopic The name value of message trace topic.If you don't config,you can use the default trace topic name.
     */
    public DefaultMQPushConsumer(final String consumerGroup, RPCHook rpcHook,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy, boolean enableMsgTrace, final String customizedTraceTopic) {
        this.consumerGroup = consumerGroup;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(this, rpcHook);
        if (enableMsgTrace) {
            try {
                AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(customizedTraceTopic, rpcHook);
                dispatcher.setHostConsumer(this.getDefaultMQPushConsumerImpl());
                traceDispatcher = dispatcher;
                this.getDefaultMQPushConsumerImpl().registerConsumeMessageHook(
                    new ConsumeMessageTraceHookImpl(traceDispatcher));
            } catch (Throwable e) {
                log.error("system mqtrace hook init failed ,maybe can't send msg trace data");
            }
        }
    }

    /**
     * Constructor specifying RPC hook.
     *
     * @param rpcHook RPC hook to execute before each remoting command.
     */
    public DefaultMQPushConsumer(RPCHook rpcHook) {
        this(MixAll.DEFAULT_CONSUMER_GROUP, rpcHook, new AllocateMessageQueueAveragely());
    }


    /**
     * Constructor specifying consumer group and enabled msg trace flag.
     *
     * @param consumerGroup Consumer group.
     * @param enableMsgTrace Switch flag instance for message trace.
     */
    public DefaultMQPushConsumer(final String consumerGroup, boolean enableMsgTrace) {
        this(consumerGroup, null, new AllocateMessageQueueAveragely(), enableMsgTrace, null);
    }

    /**
     * Constructor specifying consumer group, enabled msg trace flag and customized trace topic name.
     *
     * @param consumerGroup Consumer group.
     * @param enableMsgTrace Switch flag instance for message trace.
     * @param customizedTraceTopic The name value of message trace topic.If you don't config,you can use the default trace topic name.
     */
    public DefaultMQPushConsumer(final String consumerGroup, boolean enableMsgTrace, final String customizedTraceTopic) {
        this(consumerGroup, null, new AllocateMessageQueueAveragely(), enableMsgTrace, customizedTraceTopic);
    }

    /**
     * Constructor specifying consumer group.
     *
     * @param consumerGroup Consumer group.
     */
    public DefaultMQPushConsumer(final String consumerGroup) {
        this(consumerGroup, null, new AllocateMessageQueueAveragely());
    }

    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.defaultMQPushConsumerImpl.createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.defaultMQPushConsumerImpl.searchOffset(mq, timestamp);
    }

    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPushConsumerImpl.maxOffset(mq);
    }

    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPushConsumerImpl.minOffset(mq);
    }

    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.defaultMQPushConsumerImpl.earliestMsgStoreTime(mq);
    }

    @Override
    public MessageExt viewMessage(
        String offsetMsgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.defaultMQPushConsumerImpl.viewMessage(offsetMsgId);
    }

    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        return this.defaultMQPushConsumerImpl.queryMessage(topic, key, maxNum, begin, end);
    }

    @Override
    public MessageExt viewMessage(String topic,
        String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            MessageDecoder.decodeMessageId(msgId);
            return this.viewMessage(msgId);
        } catch (Exception e) {
            // Ignore
        }
        return this.defaultMQPushConsumerImpl.queryMessageByUniqKey(topic, msgId);
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public int getConsumeConcurrentlyMaxSpan() {
        return consumeConcurrentlyMaxSpan;
    }

    public void setConsumeConcurrentlyMaxSpan(int consumeConcurrentlyMaxSpan) {
        this.consumeConcurrentlyMaxSpan = consumeConcurrentlyMaxSpan;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public int getConsumeMessageBatchMaxSize() {
        return consumeMessageBatchMaxSize;
    }

    public void setConsumeMessageBatchMaxSize(int consumeMessageBatchMaxSize) {
        this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public int getConsumeThreadMax() {
        return consumeThreadMax;
    }

    public void setConsumeThreadMax(int consumeThreadMax) {
        this.consumeThreadMax = consumeThreadMax;
    }

    public int getConsumeThreadMin() {
        return consumeThreadMin;
    }

    public void setConsumeThreadMin(int consumeThreadMin) {
        this.consumeThreadMin = consumeThreadMin;
    }

    public DefaultMQPushConsumerImpl getDefaultMQPushConsumerImpl() {
        return defaultMQPushConsumerImpl;
    }

    public MessageListener getMessageListener() {
        return messageListener;
    }

    public void setMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public int getPullBatchSize() {
        return pullBatchSize;
    }

    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }

    public long getPullInterval() {
        return pullInterval;
    }

    public void setPullInterval(long pullInterval) {
        this.pullInterval = pullInterval;
    }

    public int getPullThresholdForQueue() {
        return pullThresholdForQueue;
    }

    public void setPullThresholdForQueue(int pullThresholdForQueue) {
        this.pullThresholdForQueue = pullThresholdForQueue;
    }

    public int getPullThresholdForTopic() {
        return pullThresholdForTopic;
    }

    public void setPullThresholdForTopic(final int pullThresholdForTopic) {
        this.pullThresholdForTopic = pullThresholdForTopic;
    }

    public int getPullThresholdSizeForQueue() {
        return pullThresholdSizeForQueue;
    }

    public void setPullThresholdSizeForQueue(final int pullThresholdSizeForQueue) {
        this.pullThresholdSizeForQueue = pullThresholdSizeForQueue;
    }

    public int getPullThresholdSizeForTopic() {
        return pullThresholdSizeForTopic;
    }

    public void setPullThresholdSizeForTopic(final int pullThresholdSizeForTopic) {
        this.pullThresholdSizeForTopic = pullThresholdSizeForTopic;
    }

    public Map<String, String> getSubscription() {
        return subscription;
    }

    public void setSubscription(Map<String, String> subscription) {
        this.subscription = subscription;
    }

    /**
     * Send message back to broker which will be re-delivered in future.
     *
     * @param msg Message to send back.
     * @param delayLevel delay level.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any broker error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, null);
    }

    /**
     * Send message back to the broker whose name is <code>brokerName</code> and the message will be re-delivered in
     * future.
     *
     * @param msg Message to send back.
     * @param delayLevel delay level.
     * @param brokerName broker name.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any broker error.
     * @throws InterruptedException if the thread is interrupted.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel, String brokerName)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, brokerName);
    }

    /*
    * 获取消费者对topic分配了哪些消息队列
    * */
    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        return this.defaultMQPushConsumerImpl.fetchSubscribeMessageQueues(topic);
    }

    /**
     * This method gets internal infrastructure readily to serve. Instances must call this method after configuration.
     *
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void start() throws MQClientException {
        this.defaultMQPushConsumerImpl.start();
        if (null != traceDispatcher) {
            try {
                traceDispatcher.start(this.getNamesrvAddr());
            } catch (MQClientException e) {
                log.warn("trace dispatcher start failed ", e);
            }
        }
    }

    /**
     * Shut down this client and releasing underlying resources.
     */
    @Override
    public void shutdown() {
        this.defaultMQPushConsumerImpl.shutdown();
        if (null != traceDispatcher) {
            traceDispatcher.shutdown();
        }
    }

    @Override
    @Deprecated
    public void registerMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }

    /**
     * Register a callback to execute on message arrival for concurrent consuming.
     * 注册并发消费事件监听器
     * @param messageListener message handling callback.
     */
    @Override
    public void registerMessageListener(MessageListenerConcurrently messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }

    /**
     * Register a callback to execute on message arrival for orderly consuming.
     * 注册顺序消费事件监听器
     * @param messageListener message handling callback.
     */
    @Override
    public void registerMessageListener(MessageListenerOrderly messageListener) {
        this.messageListener = messageListener;
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }

    /**
     * Subscribe a topic to consuming subscription.
     * 基于主题订阅消息,消息过滤方式使用表达式
     * @param topic topic to subscribe. 消息主题
     * @param subExpression subscription expression.it only support or operation such as "tag1 || tag2 || tag3" <br>
     * if null or * expression,meaning subscribe all 消息过滤表达式
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void subscribe(String topic, String subExpression) throws MQClientException {
        this.defaultMQPushConsumerImpl.subscribe(topic, subExpression);
    }

    /**
     * Subscribe a topic to consuming subscription.
     * 基于主题订阅消息,消息过滤方式使用类模式
     * @param topic topic to consume. 消息主题
     * @param fullClassName full class name,must extend org.apache.rocketmq.common.filter. MessageFilter 过滤类全路径名
     * @param filterClassSource class source code,used UTF-8 file encoding,must be responsible for your code safety 过滤类代码
     */
    @Override
    public void subscribe(String topic, String fullClassName, String filterClassSource) throws MQClientException {
        this.defaultMQPushConsumerImpl.subscribe(topic, fullClassName, filterClassSource);
    }

    /**
     * Subscribe a topic by message selector.
     *
     * @param topic topic to consume.
     * @param messageSelector {@link org.apache.rocketmq.client.consumer.MessageSelector}
     * @see org.apache.rocketmq.client.consumer.MessageSelector#bySql
     * @see org.apache.rocketmq.client.consumer.MessageSelector#byTag
     */
    @Override
    public void subscribe(final String topic, final MessageSelector messageSelector) throws MQClientException {
        this.defaultMQPushConsumerImpl.subscribe(topic, messageSelector);
    }

    /**
     * Un-subscribe the specified topic from subscription.
     * 取消消息订阅
     * @param topic message topic
     */
    @Override
    public void unsubscribe(String topic) {
        this.defaultMQPushConsumerImpl.unsubscribe(topic);
    }

    /**
     * Update the message consuming thread core pool size.
     *
     * @param corePoolSize new core pool size.
     */
    @Override
    public void updateCorePoolSize(int corePoolSize) {
        this.defaultMQPushConsumerImpl.updateCorePoolSize(corePoolSize);
    }

    /**
     * Suspend pulling new messages.
     */
    @Override
    public void suspend() {
        this.defaultMQPushConsumerImpl.suspend();
    }

    /**
     * Resume pulling.
     */
    @Override
    public void resume() {
        this.defaultMQPushConsumerImpl.resume();
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    public String getConsumeTimestamp() {
        return consumeTimestamp;
    }

    public void setConsumeTimestamp(String consumeTimestamp) {
        this.consumeTimestamp = consumeTimestamp;
    }

    public boolean isPostSubscriptionWhenPull() {
        return postSubscriptionWhenPull;
    }

    public void setPostSubscriptionWhenPull(boolean postSubscriptionWhenPull) {
        this.postSubscriptionWhenPull = postSubscriptionWhenPull;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }

    public long getAdjustThreadPoolNumsThreshold() {
        return adjustThreadPoolNumsThreshold;
    }

    public void setAdjustThreadPoolNumsThreshold(long adjustThreadPoolNumsThreshold) {
        this.adjustThreadPoolNumsThreshold = adjustThreadPoolNumsThreshold;
    }

    public int getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(final int maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }

    public long getSuspendCurrentQueueTimeMillis() {
        return suspendCurrentQueueTimeMillis;
    }

    public void setSuspendCurrentQueueTimeMillis(final long suspendCurrentQueueTimeMillis) {
        this.suspendCurrentQueueTimeMillis = suspendCurrentQueueTimeMillis;
    }

    public long getConsumeTimeout() {
        return consumeTimeout;
    }

    public void setConsumeTimeout(final long consumeTimeout) {
        this.consumeTimeout = consumeTimeout;
    }

    public TraceDispatcher getTraceDispatcher() {
        return traceDispatcher;
    }
}
