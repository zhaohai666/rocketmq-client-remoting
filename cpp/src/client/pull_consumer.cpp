#include "rocketmq/client/pull_consumer.h"

#include <chrono>
#include <exception>
#include <utility>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/validators.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/exception.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/headers.h"
#include "rocketmq/remoting/protocol/remoting_command.h"

namespace rocketmq {

namespace {

std::string trim(const std::string& s) {
    size_t b = s.find_first_not_of(" \t\r\n");
    if (b == std::string::npos) return std::string();
    size_t e = s.find_last_not_of(" \t\r\n");
    return s.substr(b, e - b + 1);
}

std::vector<std::string> splitSemicolon(const std::string& addr) {
    std::vector<std::string> out;
    size_t start = 0;
    while (start <= addr.size()) {
        size_t pos = addr.find(';', start);
        std::string piece =
            addr.substr(start, pos == std::string::npos ? std::string::npos : pos - start);
        std::string t = trim(piece);
        if (!t.empty()) out.push_back(t);
        if (pos == std::string::npos) break;
        start = pos + 1;
    }
    return out;
}

int64_t extInt(const RemotingCommand& r, const std::string& key, int64_t fallback) {
    auto it = r.extFields.find(key);
    if (it == r.extFields.end() || it->second.empty()) return fallback;
    try {
        return std::stoll(it->second);
    } catch (...) {
        return fallback;
    }
}

}  // namespace

DefaultMQPullConsumer::DefaultMQPullConsumer(const std::string& consumerGroup) {
    // 与 push/lite-pull 生产者同款首道守卫：只挡空白组名。完整校验
    // （长度/字符表/DEFAULT_CONSUMER 保留名）在 start() 里走 Validators::checkGroup，
    // 与 Python 一致。⚠ 旧实现用 empty()，纯空白组名会漏过——改为 UtilAll::isBlank。
    if (UtilAll::isBlank(consumerGroup)) {
        throw MQClientException("consumerGroup is empty");
    }
    consumerGroup_ = consumerGroup;
}

DefaultMQPullConsumer::~DefaultMQPullConsumer() {
    try {
        shutdown();
    } catch (...) {
    }
}

// ---------------------------------------------------------------- 配置
void DefaultMQPullConsumer::setNamesrvAddr(const std::string& addr) {
    nameServerAddrs_ = splitSemicolon(addr);
}

void DefaultMQPullConsumer::setNameServerAddresses(const std::vector<std::string>& addrs) {
    nameServerAddrs_ = addrs;
}

void DefaultMQPullConsumer::registerMessageQueueListener(
    const std::string& topic, std::shared_ptr<MessageQueueListener> listener) {
    if (listener == nullptr || topic.empty()) return;
    registerTopics_.insert(topic);
    messageQueueListeners_[topic] = std::move(listener);
}

// ---------------------------------------------------------------- 生命周期
void DefaultMQPullConsumer::start() {
    if (started_) return;
    // 对齐 Java DefaultMQPullConsumer.start()：把消费组套上命名空间（ns%group），
    // 之后所有面向 broker 的组名（心跳 / 位点 / 回投）都用包装后的值。
    if (!namespace_.empty()) {
        consumerGroup_ = NamespaceUtil::wrapNamespace(namespace_, consumerGroup_);
    }
    // 对应 Java DefaultMQPullConsumerImpl.checkConfig(:772) / Python：组名合法性 +
    // 挡掉 DEFAULT_CONSUMER（共用默认组会混掉订阅关系与位点）。纯本地校验，
    // 排在地址检查与建客户端实例之前，失败必须**不碰网络**。
    Validators::checkGroup(consumerGroup_);
    if (consumerGroup_ == MixAll::DEFAULT_CONSUMER_GROUP) {
        throw MQClientException(
            "consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.");
    }
    if (nameServerAddrs_.empty()) {
        throw MQClientException("name server address is not set");
    }
    // 对应 Java DefaultMQPullConsumerImpl.checkConfig(:803) / 本端口 push 与 lite：
    // 策略为空直接拒绝启动。
    if (!allocateStrategy_) {
        throw MQClientException("allocateMessageQueueStrategy is null");
    }
    // Java `DefaultMQPullConsumerImpl#start`:712-714：CLUSTERING 才改写 instanceName，
    // clientId 口径是 `ClientConfig#buildMQClientId` 的 `<本机 IP>@<instanceName>`。
    if (messageModel_ == MessageModel::CLUSTERING) {
        instanceName_ = changeInstanceNameToPID(instanceName_);
    }
    if (clientId_.empty()) {
        clientId_ = buildClientId(instanceName_, unitName_, enableStreamRequestType_);
    }
    // 拉模式消费者默认开着 stream（Java `DefaultMQPullConsumer:113/126` 在构造函数里
    // 就置了 true），unitName 只影响动态取址的 URL。
    mqClient_.reset(new MQClientInstance(clientId_, nameServerAddrs_,
                                        /*connectTimeoutMillis=*/3000,
                                        consumerPullTimeoutMillis_,
                                        MQClientInstance::tlsEnabledFromEnv(), unitName_,
                                        pollNameServerIntervalMillis_));
    // 请求钩子（ACL 签名 / stream 的 `ReqT`）：必须在**实例 start() 之前**绑定 ——
    // Java 的 rpcHook 是在 MQClientAPIImpl 构造时传进去的（MQClientInstance:214 附近），
    // 也就是实例发出的第一笔报文就带着它；放到 start() 之后，start 期间的动态取址、
    // 首包路由就可能签不出 ReqT/AccessKey。
    std::shared_ptr<RPCHook> requestHook =
        composeRequestHooks(enableStreamRequestType_, rpcHook_);
    if (requestHook && !mqClient_->registerRPCHook(requestHook)) {
        logger_warn("pull consumer rpc hook ignored: MQClientInstance already has one (clientId="
                    + clientId_ + ")");
    }
    mqClient_->start();
    // 拉模式也要登记 topic，路由才会被周期刷新（对齐 Java registerTopicInUse）。
    // 心跳前的刷路由走同一个入口：`brokerAddrTable` 空着时心跳没有收件人。
    refreshRouteForHeartbeat();
    started_ = true;
    // 同步发一轮让 broker 立刻认识本组，然后交给常驻循环。顺序对齐 lite 拉取消费者
    // start（同一份理由），也贴合 Java 的 registerConsumer:746 → mQClientFactory.start():755
    // （实例级心跳任务首个周期在 1s 内发出）。
    try {
        sendHeartbeatToAllBroker();
    } catch (const std::exception& e) {
        logger_debug(std::string("initial heartbeat failed: ") + e.what());
    }
    startHeartbeatLoop();
}

void DefaultMQPullConsumer::shutdown() {
    if (!started_) return;
    started_ = false;
    heartbeatStop_.store(true);
    heartbeatCv_.notify_all();
    if (heartbeatThread_.joinable()) heartbeatThread_.join();
    if (mqClient_ != nullptr) {
        // 优雅注销（对齐 Java DefaultMQPullConsumerImpl.shutdown:689-692：
        // unregisterConsumer → mQClientFactory.shutdown）：立刻从各 broker 的
        // ConsumerManager 摘除本组，不必等心跳超时（默认 ~120s）。本端口没有 Java 的
        // 本地位点表，所以 `persistConsumerOffset()` 那一步无对应物（位点由调用方
        // updateConsumeOffset 直接写给 broker）。
        try {
            mqClient_->unregisterClientAllBrokers(clientId_, "", consumerGroup_);
        } catch (const std::exception& e) {
            logger_debug(std::string("unregister on shutdown failed: ") + e.what());
        }
        mqClient_->shutdown();
        mqClient_.reset();
    }
}

// ---------------------------------------------------------------- 队列
std::vector<MessageQueue> DefaultMQPullConsumer::fetchSubscribeMessageQueues(
    const std::string& topic) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    const std::string realTopic = NamespaceUtil::wrapNamespace(namespace_, topic);
    std::shared_ptr<TopicPublishInfo> publish = mqClient_->getTopicPublishInfo(realTopic);
    if (publish == nullptr || !publish->ok()) {
        // 对齐 Java：topic 不存在（拿不到路由）时直接抛，而不是返回空列表。
        throw MQClientException("the topic[" + topic + "] not exist");
    }
    return publish->msgQueueList;
}

// ---------------------------------------------------------------- 拉取
PullResult DefaultMQPullConsumer::pull(const MessageQueue& mq, const std::string& subExpression,
                                      int64_t offset, int32_t maxNums, int32_t timeoutMillis) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    const int32_t timeout = timeoutMillis > 0 ? timeoutMillis : consumerPullTimeoutMillis_;
    // 对齐 Java DefaultMQPullConsumerImpl.pullSyncImpl（:248）：
    //   sysFlag = PullSysFlag.buildSysFlag(false, block, true, false)
    // 即 commitOffset=false、suspend=block。**pull() 的 block=false** ——
    // 位点由调用方自己 updateConsumeOffset 提交，且这是**短轮询**不挂起。
    // ⚠ 曾在这里写成 suspend=true：broker 在队尾会挂起到 brokerSuspendMaxTimeMillis
    // （默认 20s），而客户端 5s 就超时 → RemotingTimeoutException（真机必现）。
    const int32_t sysFlag = PullSysFlag::buildSysFlag(/*commitOffset=*/false,
                                                      /*suspend=*/false,
                                                      /*subscription=*/true,
                                                      /*classFilter=*/false);
    MessageQueue real = mq;
    real.topic = NamespaceUtil::wrapNamespace(namespace_, mq.topic);
    const SubscriptionData sub = FilterAPI::buildSubscriptionData(real.topic, subExpression);
    // Java PullAPIWrapper#pullKernelImpl：按 pullFromWhichNodeTable 选主/从
    PullResult result = mqClient_->pullMessage(
        consumerGroup_, real, offset, maxNums, sysFlag, /*commitOffset=*/0,
        sub.subString.empty() ? std::string("*") : sub.subString,
        // Java：TAG 类型时 subVersion 传 0（isTagType ? 0L : subVersion）
        /*subVersion=*/0, ExpressionType::TAG, timeout, /*maxMsgBytes=*/-1,
        /*suspendTimeoutMillis=*/15000, /*addr=*/std::string(), /*requestSource=*/0,
        /*brokerId=*/recalculatePullFromWhichNode(real));
    updatePullFromWhichNode(real, result);
    return result;
}

PullResult DefaultMQPullConsumer::pullBlockIfNotFound(const MessageQueue& mq,
                                                      const std::string& subExpression,
                                                      int64_t offset, int32_t maxNums) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    // block=true：suspend=true 让 broker 挂起到有消息；超时用
    // consumerTimeoutMillisWhenSuspend（Java :250 的 `block ? ... : timeout`）。
    const int32_t sysFlag = PullSysFlag::buildSysFlag(/*commitOffset=*/false,
                                                      /*suspend=*/true,
                                                      /*subscription=*/true,
                                                      /*classFilter=*/false);
    MessageQueue real = mq;
    real.topic = NamespaceUtil::wrapNamespace(namespace_, mq.topic);
    const SubscriptionData sub = FilterAPI::buildSubscriptionData(real.topic, subExpression);
    PullResult result = mqClient_->pullMessage(
        consumerGroup_, real, offset, maxNums, sysFlag, /*commitOffset=*/0,
        sub.subString.empty() ? std::string("*") : sub.subString,
        /*subVersion=*/0, ExpressionType::TAG, consumerTimeoutMillisWhenSuspend_,
        /*maxMsgBytes=*/-1, brokerSuspendMaxTimeMillis_, /*addr=*/std::string(),
        /*requestSource=*/0, /*brokerId=*/recalculatePullFromWhichNode(real));
    updatePullFromWhichNode(real, result);
    return result;
}

// Java PullAPIWrapper#recalculatePullFromWhichNode：表里没有该队列时按 master=0。
int64_t DefaultMQPullConsumer::recalculatePullFromWhichNode(const MessageQueue& mq) const {
    auto it = pullFromWhichNode_.find(mq);
    return it != pullFromWhichNode_.end() ? it->second : MixAll::MASTER_ID;
}

// Java PullAPIWrapper#updatePullFromWhichNode:157-164：把应答头里的 suggestWhichBrokerId
// 写回表；缺省（老 broker 不带该字段）按 master=0 记账 —— 与 Java 的 long 原语口径一致，
// 所以不能把「没有该字段」当成"保留旧值"。
void DefaultMQPullConsumer::updatePullFromWhichNode(const MessageQueue& mq,
                                                    const PullResult& result) {
    pullFromWhichNode_[mq] = result.suggestWhichBrokerId.value_or(MixAll::MASTER_ID);
}

// ---------------------------------------------------------------- 位点管理
bool DefaultMQPullConsumer::fetchConsumeOffset(const MessageQueue& mq, int64_t& outOffset) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    return mqClient_->queryConsumerOffset(consumerGroup_, mq, outOffset);
}

void DefaultMQPullConsumer::updateConsumeOffset(const MessageQueue& mq, int64_t offset) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    mqClient_->updateConsumerOffset(consumerGroup_, mq, offset);
}

int64_t DefaultMQPullConsumer::searchOffset(const MessageQueue& mq, int64_t timestamp) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    return mqClient_->searchOffsetByTimestamp(mq, timestamp);
}

int64_t DefaultMQPullConsumer::maxOffset(const MessageQueue& mq) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    return mqClient_->getMaxOffset(mq);
}

int64_t DefaultMQPullConsumer::minOffset(const MessageQueue& mq) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    return mqClient_->getMinOffset(mq);
}

int64_t DefaultMQPullConsumer::earliestMsgStoreTime(const MessageQueue& mq) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    const std::string addr = mqClient_->brokerAddrForMq(mq);
    if (addr.empty()) {
        throw MQClientException("broker " + mq.brokerName + " not found");
    }
    PropertyMap ext;
    ext["topic"] = mq.topic;
    ext["queueId"] = std::to_string(mq.queueId);
    ext["brokerName"] = mq.brokerName;
    const RemotingCommand response = mqClient_->invokeSync(
        addr, RequestCode::GET_EARLIEST_MSG_STORETIME, ext, Bytes(), false, 5000);
    return extInt(response, "timestamp", 0);
}

// ---------------------------------------------------------------- 回投 / 建 topic
// 消息回投（对应 Java DefaultMQPullConsumer.sendMessageBack）。
// 注意两点（真机踩过）：
//   1. 地址靠 brokerAddrOf(msg.brokerName) 反查**路由表**，所以调用方必须先用本 consumer
//      访问过该 topic（Java 同理，走 findBrokerAddressInPublish 读 brokerAddrTable）。
//   2. 与 Java 的**有意差异**：Java 在失败时会吞掉异常、改用内部默认生产者把消息直接发到
//      %RETRY%group（DefaultMQPullConsumerImpl:666 的 catch 分支）。本实现不做这个兜底 ——
//      回投失败就抛，让调用方看见，而不是换一条路径静默重发。
void DefaultMQPullConsumer::sendMessageBack(const MessageExt& msg, int32_t delayLevel) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    const std::string addr = mqClient_->brokerAddrOf(msg.brokerName);
    if (addr.empty()) {
        throw MQClientException("broker " + msg.brokerName + " not found");
    }
    auto header = std::make_shared<ConsumerSendMsgBackRequestHeader>();
    header->offset = msg.commitLogOffset;
    header->group = consumerGroup_;
    header->delayLevel = delayLevel;
    header->originMsgId = msg.msgId;
    header->originTopic = msg.topic;
    // ⚠ 有意超出 Java：Java 的 MQClientAPIImpl#consumerSendMessageBack(:1684-1693) 只填
    // group/offset/delayLevel/originMsgId/originTopic/maxReconsumeTimes/brokerName，
    // 从不写 unitMode，所以字段恒为 false —— 单元化模式下回投自动建出的 %RETRY%group
    // 拿不到 UNIT_SUB 标记。broker 侧是读这个字段的
    // （AbstractSendMessageProcessor:135-138 → TopicSysFlag.buildSysFlag(false, true)），
    // 故这里按消费者配置如实上报。
    header->unitMode = unitMode_;
    // ⚠ 不照抄 Java 弃用的 DefaultMQPullConsumerImpl#sendMessageBack（它直接传
    // getMaxReconsumeTimes()，默认 -1）。客户端版本 ≥ V3_4_9 后 broker 无条件采信该
    // 字段（`AbstractSendMessageProcessor:172-179`），-1 会让 reconsumeTimes(0) >= -1
    // 成立、消息直接进 %DLQ%。留空交给订阅组的 retryMaxTimes 判定。
    header->maxReconsumeTimes = std::nullopt;
    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::CONSUMER_SEND_MSG_BACK, header);
    const RemotingCommand response = mqClient_->remotingClient().invokeSync(addr, request, 5000);
    if (response.code != ResponseCode::SUCCESS) {
        throw MQBrokerException(response.code, response.remark);
    }
}

void DefaultMQPullConsumer::createTopic(const std::string& key, const std::string& newTopic,
                                       int32_t queueNum) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    // C++ 的 createTopicInRoute 忽略 key 形参，统一走默认 topic（MixAll.DefaultTopic）建路由
    (void)key;
    const std::string real = NamespaceUtil::wrapNamespace(namespace_, newTopic);
    mqClient_->createTopicInRoute(real, queueNum, queueNum, /*perm=*/6);
}

// ---------------------------------------------------------------- 心跳
// 为什么拉模式也必须心跳：broker 的 `ConsumerManager.consumerTable` 是**按台**的，
// 只有心跳（或带订阅标志的拉取）才会建表。没有心跳时：
//   1. `consumerConnection`/`mqadmin consumerConnection`（`AdminBrokerProcessor:1971`）
//      看不到本组，`GET_CONSUMER_LIST_BY_GROUP(38)` 也是空的；
//   2. `isRejectPullConsumerEnabled=true` 的 broker 会给本组每次拉取都回
//      `the pull consumer is rejected by server`（`PullMessageProcessor:493-505`）。
// 而 `ClientManageProcessor:87-92` **跳过** ACTIVELY 类型心跳的订阅注册 ——
// 拉模式靠自带订阅标志的拉取走补偿分支（`PullMessageProcessor:397-412`），所以
// 缺心跳的失效是静默的：拉取照样成功，只是 broker 侧完全不知道本组存在。
void DefaultMQPullConsumer::refreshRouteForHeartbeat() {
    if (mqClient_ == nullptr) return;
    // Java 侧这条链路是间接的：`DefaultMQPullConsumerImpl.subscriptions():357-385`
    // 返回 registerTopics 构出的订阅集，实例的 `updateTopicRouteInfoFromNameServer`
    // 周期任务据此刷路由 → `brokerAddrTable` 有地址 → 心跳发得出去。本端口没有实例级
    // 路由任务（心跳循环在消费者内，同 push / lite），所以显式刷一遍并按 registerTopics
    // 登记「在用」，交给 MQClientInstance 的后台刷新任务保持新鲜。
    for (const std::string& t : registerTopics_) {
        const std::string real = NamespaceUtil::wrapNamespace(namespace_, t);
        mqClient_->registerTopicInUse(real);
        try {
            mqClient_->getTopicPublishInfo(real);
        } catch (const std::exception& e) {
            logger_debug("refresh route for " + real + " failed: " + e.what());
        }
    }
}

// Java `MQClientInstance#prepareHeartbeatData:1031-1045` 为拉模式消费者组出来的那一份：
// `consumeType()` 恒为 CONSUME_ACTIVELY（`DefaultMQPullConsumerImpl:348`）、
// `consumeFromWhere()` 恒为 CONSUME_FROM_LAST_OFFSET（:353）—— 与 push 消费者的
// PASSIVELY 是两个口径，broker 侧两项都不是摆设（见本节开头那段）。
// 订阅集取 `subscriptions():357-385`：逐条 `buildSubscriptionData(topic, "*")` 并显式
// `setSubVersion(0L)`（Java 源码如此）—— 拉模式没有"订阅版本"语义，带上 SubscriptionData
// 构造函数默认的当前时间戳会让 broker 每次心跳都认为订阅变了。
// ⚠ 本端口没有 Java 的 `registerSubscriptions` 入口，订阅集只从 registerTopics 来。
HeartbeatData DefaultMQPullConsumer::buildHeartbeat() const {
    HeartbeatData hb(clientId_);
    ConsumerData cd(consumerGroup_, ConsumeType::CONSUME_ACTIVELY, messageModel_,
                    ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET);
    // Java `MQClientInstance:1039` 心跳里带 consumerData.setUnitMode(tc.isUnitMode())：
    // broker 据此决定 %RETRY% topic 建出来带不带 UNIT_SUB 位。
    cd.unitMode = unitMode_;
    for (const std::string& t : registerTopics_) {
        // Java 是 `FilterAPI.buildSubscriptionData(t, SubscriptionData.SUB_ALL)`（即 "*"）
        SubscriptionData sub = FilterAPI::buildSubscriptionData(t, "*");
        sub.subVersion = 0;
        cd.addSubscriptionData(sub);
    }
    hb.addConsumerData(cd);
    return hb;
}

int32_t DefaultMQPullConsumer::sendHeartbeatToAllBroker() {
    if (mqClient_ == nullptr) {
        return 0;
    }
    std::vector<std::string> addrs;
    try {
        // 每台都发（主 + 从）：Java `MQClientInstance#sendHeartbeatToAllBroker`:732-750
        // 遍历 `brokerAddrTable` 的每个 brokerId，仅当 `consumerEmpty && id != MASTER_ID`
        // 才跳过；消费者心跳必带 ConsumerData，故从节点不跳。broker 的 ConsumerManager
        // 每台各自一份，从节点收不到心跳，指向自己的拉取就要走没有订阅表的补偿分支。
        addrs = mqClient_->getAllBrokerAddrs();
    } catch (const std::exception& e) {
        logger_warn("heartbeat: gather brokers failed: " + std::string(e.what()));
        return 0;
    }
    if (addrs.empty()) {
        return 0;
    }
    const HeartbeatData hb = buildHeartbeat();
    int32_t okCount = 0;
    for (const std::string& addr : addrs) {
        try {
            mqClient_->sendHeartbeat(addr, hb, 5000);
            ++okCount;
        } catch (const std::exception& e) {
            logger_warn("heartbeat to " + addr + " failed: " + e.what());
        }
    }
    if (okCount > 0) {
        heartbeatCount_.fetch_add(1);
    }
    return okCount;
}

void DefaultMQPullConsumer::startHeartbeatLoop() {
    // std::thread 一构造就跑：先备好停机标志位（构造前已置 false），线程起来即可用。
    heartbeatThread_ = std::thread([this]() {
        setThreadName("PullConsumerHeartbeatThread");
        heartbeatLoop();
    });
}

void DefaultMQPullConsumer::heartbeatLoop() {
    while (!heartbeatStop_.load()) {
        {
            std::unique_lock<std::mutex> lk(heartbeatLock_);
            // 停机时由 shutdown() 置位并 notify，最多等一个周期就醒。
            if (heartbeatCv_.wait_for(lk, std::chrono::milliseconds(heartbeatBrokerIntervalMillis_),
                                      [this] { return heartbeatStop_.load(); })) {
                break;
            }
        }
        if (!heartbeatEnabled_) {
            continue;
        }
        // 常驻循环的边界：线程里逃出异常 = std::terminate（整个进程陪葬）。心跳失败
        // 已在 sendHeartbeatToAllBroker 内部逐台接住，这里再兜一层（比如拼心跳时的异常）。
        try {
            sendHeartbeatToAllBroker();
        } catch (const std::exception& e) {
            logger_warn(std::string("heartbeat loop error: ") + e.what());
        }
    }
}

}  // namespace rocketmq
