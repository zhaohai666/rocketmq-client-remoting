#include "rocketmq/client/pull_consumer.h"

#include <algorithm>
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
    // （长度/字符表/DEFAULT_CONSUMER 保留名）在 start() 里走 Validators::checkGroup。
    // ⚠ 旧实现用 empty()，纯空白组名会漏过——改为 UtilAll::isBlank。
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
    if (topic.empty()) return;
    registerTopics_.insert(topic);
    if (listener != nullptr) {
        messageQueueListeners_[topic] = std::move(listener);
    }
}

// ---------------------------------------------------------------- 生命周期
void DefaultMQPullConsumer::start() {
    if (started_) return;
    if (!namespace_.empty()) {
        consumerGroup_ = NamespaceUtil::wrapNamespace(namespace_, consumerGroup_);
    }
    Validators::checkGroup(consumerGroup_);
    if (consumerGroup_ == MixAll::DEFAULT_CONSUMER_GROUP) {
        throw MQClientException(
            "consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.");
    }
    if (nameServerAddrs_.empty()) {
        throw MQClientException("name server address is not set");
    }
    if (!allocateStrategy_) {
        throw MQClientException("allocateMessageQueueStrategy is null");
    }
    if (messageModel_ == MessageModel::CLUSTERING) {
        instanceName_ = changeInstanceNameToPID(instanceName_);
    }
    if (clientId_.empty()) {
        clientId_ = buildClientId(instanceName_, unitName_, enableStreamRequestType_);
    }
    mqClient_.reset(new MQClientInstance(clientId_, nameServerAddrs_,
                                        /*connectTimeoutMillis=*/3000,
                                        consumerPullTimeoutMillis_,
                                        MQClientInstance::tlsEnabledFromEnv(), unitName_,
                                        pollNameServerIntervalMillis_));
    std::shared_ptr<RPCHook> requestHook =
        composeRequestHooks(enableStreamRequestType_, rpcHook_,
                            [this] { return namespaceV2_; });
    if (requestHook && !mqClient_->registerRPCHook(requestHook)) {
        logger_warn("pull consumer rpc hook ignored: MQClientInstance already has one (clientId="
                    + clientId_ + ")");
    }
    mqClient_->start();
    refreshRouteForHeartbeat();
    started_ = true;
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
    std::shared_ptr<TopicRouteData> route = mqClient_->getTopicRouteData(realTopic);
    if (route == nullptr) {
        throw MQClientException("the topic[" + topic + "] not exist");
    }
    return route->getAllSubscribeMessageQueue(realTopic);
}

// ---------------------------------------------------------------- 平衡视图
std::vector<MessageQueue> DefaultMQPullConsumer::fetchMessageQueuesInBalance(
    const std::string& topic) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    const std::string realTopic = NamespaceUtil::wrapNamespace(namespace_, topic);

    // 现有分配：本地拉过的那个 topic 的队列（键由 pull() 写入，topic 已是 realTopic）。
    std::vector<MessageQueue> pulled;
    for (const auto& kv : pullFromWhichNode_) {
        if (kv.first.topic == realTopic) pulled.push_back(kv.first);
    }
    std::sort(pulled.begin(), pulled.end());

    std::vector<MessageQueue> allocated;
    bool computed = false;
    std::shared_ptr<TopicRouteData> route = mqClient_->getTopicRouteData(realTopic);
    if (route != nullptr) {
        std::vector<MessageQueue> mqAll = route->getAllSubscribeMessageQueue(realTopic);
        std::sort(mqAll.begin(), mqAll.end());
        if (messageModel_ == MessageModel::BROADCASTING) {
            allocated = mqAll;
            computed = true;
        } else if (!mqAll.empty()) {
            std::vector<std::string> cidAll =
                mqClient_->getConsumerIdListByGroup(realTopic, consumerGroup_);
            if (!cidAll.empty()) {
                std::sort(cidAll.begin(), cidAll.end());
                try {
                    allocated = allocateStrategy_->allocate(consumerGroup_, clientId_, mqAll, cidAll);
                    computed = true;
                } catch (const std::exception& e) {
                    logger_error("allocate message queue exception. strategy name: "
                                 + allocateStrategy_->getName() + ", ex: " + e.what());
                }
            }
        }
    }
    if (!computed) {
        logger_debug("fetchMessageQueuesInBalance: no route/consumer list for " + consumerGroup_
                     + "/" + topic + ", keep current assignment");
        return pulled;
    }

    std::vector<MessageQueue> result;
    for (const MessageQueue& mq : allocated) {
        if (mq.topic != realTopic) continue;
        result.push_back(
            MessageQueue(NamespaceUtil::withoutNamespace(mq.topic, namespace_), mq.brokerName,
                         mq.queueId));
    }
    std::sort(result.begin(), result.end());
    return result;
}

// ---------------------------------------------------------------- 拉取
PullResult DefaultMQPullConsumer::pull(const MessageQueue& mq, const std::string& subExpression,
                                      int64_t offset, int32_t maxNums, int32_t timeoutMillis) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    const int32_t timeout = timeoutMillis > 0 ? timeoutMillis : consumerPullTimeoutMillis_;
    const int32_t sysFlag = PullSysFlag::buildSysFlag(/*commitOffset=*/false,
                                                      /*suspend=*/false,
                                                      /*subscription=*/true,
                                                      /*classFilter=*/false);
    MessageQueue real = mq;
    real.topic = NamespaceUtil::wrapNamespace(namespace_, mq.topic);
    const SubscriptionData sub = FilterAPI::buildSubscriptionData(real.topic, subExpression);
    PullResult result = mqClient_->pullMessage(
        consumerGroup_, real, offset, maxNums, sysFlag, /*commitOffset=*/0,
        sub.subString.empty() ? std::string("*") : sub.subString,
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

int64_t DefaultMQPullConsumer::recalculatePullFromWhichNode(const MessageQueue& mq) const {
    auto it = pullFromWhichNode_.find(mq);
    return it != pullFromWhichNode_.end() ? it->second : MixAll::MASTER_ID;
}

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
    const std::string addr = mqClient_->publishAddrFor(mq.brokerName, mq.topic);
    PropertyMap ext;
    ext["topic"] = mq.topic;
    ext["queueId"] = std::to_string(mq.queueId);
    ext["brokerName"] = mq.brokerName;
    const RemotingCommand response = mqClient_->invokeSync(
        addr, RequestCode::GET_EARLIEST_MSG_STORETIME, ext, Bytes(), false, 5000);
    return extInt(response, "timestamp", 0);
}

void DefaultMQPullConsumer::sendMessageBack(const MessageExt& msg, int32_t delayLevel) {
    if (mqClient_ == nullptr) {
        throw MQClientException("consumer not started, call start() first");
    }
    const std::string addr = mqClient_->findBrokerAddressInPublish(msg.brokerName);
    if (addr.empty()) {
        throw MQClientException("Broker[" + msg.brokerName + "] master node does not exist");
    }
    auto header = std::make_shared<ConsumerSendMsgBackRequestHeader>();
    header->offset = msg.commitLogOffset;
    header->group = consumerGroup_;
    header->delayLevel = delayLevel;
    header->originMsgId = msg.msgId;
    header->originTopic = msg.topic;
    header->unitMode = unitMode_;
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

HeartbeatData DefaultMQPullConsumer::buildHeartbeat() const {
    HeartbeatData hb(clientId_);
    ConsumerData cd(consumerGroup_, ConsumeType::CONSUME_ACTIVELY, messageModel_,
                    ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET);
    cd.unitMode = unitMode_;
    for (const std::string& t : registerTopics_) {
        const std::string subTopic = NamespaceUtil::wrapNamespace(namespace_, t);
        SubscriptionData sub = FilterAPI::buildSubscriptionData(subTopic, "*");
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
