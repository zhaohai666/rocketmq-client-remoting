#include "rocketmq/client/pull_consumer.h"

#include <exception>
#include <utility>

#include "rocketmq/client/exception.h"
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
    if (consumerGroup.empty()) {
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
    if (nameServerAddrs_.empty()) {
        throw MQClientException("name server address is not set");
    }
    if (clientId_.empty()) {
        clientId_ = buildClientId(instanceName_);
    }
    // 对齐 Java DefaultMQPullConsumer.start()：把消费组套上命名空间（ns%group），
    // 之后所有面向 broker 的组名（心跳 / 位点 / 回投）都用包装后的值。
    if (!namespace_.empty()) {
        consumerGroup_ = NamespaceUtil::wrapNamespace(namespace_, consumerGroup_);
    }
    mqClient_.reset(new MQClientInstance(clientId_, nameServerAddrs_,
                                        /*connectTimeoutMillis=*/3000,
                                        consumerPullTimeoutMillis_));
    mqClient_->start();
    // 拉模式也要登记 topic，路由才会被周期刷新（对齐 Java registerTopicInUse）。
    for (const std::string& t : registerTopics_) {
        mqClient_->registerTopicInUse(NamespaceUtil::wrapNamespace(namespace_, t));
    }
    // ACL 鉴权钩子：必须在首包（路由拉取 / 位点查询）发出之前绑定。
    if (rpcHook_ && !mqClient_->registerRPCHook(rpcHook_)) {
        logger_warn("pull consumer rpc hook ignored: MQClientInstance already has one (clientId="
                    + clientId_ + ")");
    }
    started_ = true;
}

void DefaultMQPullConsumer::shutdown() {
    if (!started_) return;
    started_ = false;
    if (mqClient_ != nullptr) {
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
    return mqClient_->pullMessage(
        consumerGroup_, real, offset, maxNums, sysFlag, /*commitOffset=*/0,
        sub.subString.empty() ? std::string("*") : sub.subString,
        // Java：TAG 类型时 subVersion 传 0（isTagType ? 0L : subVersion）
        /*subVersion=*/0, ExpressionType::TAG, timeout, /*maxMsgBytes=*/-1,
        /*suspendTimeoutMillis=*/15000);
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
    return mqClient_->pullMessage(
        consumerGroup_, real, offset, maxNums, sysFlag, /*commitOffset=*/0,
        sub.subString.empty() ? std::string("*") : sub.subString,
        /*subVersion=*/0, ExpressionType::TAG, consumerTimeoutMillisWhenSuspend_,
        /*maxMsgBytes=*/-1, brokerSuspendMaxTimeMillis_);
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
    header->unitMode = false;
    // Java：maxReconsumeTimes == -1 时按 16 传给 broker（超限由 broker 转 %DLQ%）
    header->maxReconsumeTimes = 16;
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

}  // namespace rocketmq
