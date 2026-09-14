// MQClientInstance 实现（对应 Python client/mq_client.py）。
#include "rocketmq/client/mq_client.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/exception.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/headers.h"

namespace rocketmq {

namespace {

// 拉取响应码 -> PullStatus（对应 Python pull_message 的映射）
bool mapPullStatus(int32_t code, PullStatus& out) {
    switch (code) {
        case ResponseCode::SUCCESS:
            out = PullStatus::FOUND;
            return true;
        case ResponseCode::PULL_NOT_FOUND:
            out = PullStatus::NO_NEW_MSG;
            return true;
        case ResponseCode::PULL_OFFSET_MOVED:
            out = PullStatus::OFFSET_ILLEGAL;
            return true;
        case ResponseCode::PULL_RETRY_IMMEDIATELY:
            out = PullStatus::NO_MATCHED_MSG;
            return true;
        default:
            return false;
    }
}

}  // namespace

// ---------------------------------------------------------------- clientId
std::string buildClientId(const std::string& instanceName) {
    static std::atomic<uint32_t> seq{0};
    std::string ts = UtilAll::timeToHumanString(UtilAll::currentTimeMillis(), "%Y%m%d%H%M%S");
    return instanceName + "@" + ts + "@" + std::to_string(UtilAll::pid()) + "@"
         + std::to_string(seq.fetch_add(1));
}

// ---------------------------------------------------------------- TopicPublishInfo
MessageQueue TopicPublishInfo::selectOneMessageQueue() {
    if (msgQueueList.empty()) {
        throw MQClientException("no message queue for publish info");
    }
    // 游标用原子自增：生产端可能被多线程并发调用，且游标是跨调用共享状态
    uint64_t idx = index_.fetch_add(1);
    return msgQueueList[static_cast<size_t>(idx % msgQueueList.size())];
}

MessageQueue TopicPublishInfo::selectOneMessageQueue(const std::string& lastBrokerName) {
    if (msgQueueList.empty()) {
        throw MQClientException("no message queue for publish info");
    }
    // 对应 Java：尽量避开上次失败的 broker；若全是同一 broker 则退化为轮询
    for (size_t i = 0; i < msgQueueList.size(); ++i) {
        uint64_t idx = index_.fetch_add(1);
        const MessageQueue& mq = msgQueueList[static_cast<size_t>(idx % msgQueueList.size())];
        if (mq.brokerName != lastBrokerName) {
            return mq;
        }
    }
    uint64_t idx = index_.fetch_add(1);
    return msgQueueList[static_cast<size_t>(idx % msgQueueList.size())];
}

// ---------------------------------------------------------------- 生命周期
MQClientInstance::MQClientInstance(const std::string& clientId,
                                  const std::vector<std::string>& nameServerAddrs,
                                  int32_t connectTimeoutMillis, int32_t invokeTimeoutMillis)
    : clientId_(clientId), nameServerAddrs_(nameServerAddrs),
      remotingClient_(new RemotingClient(connectTimeoutMillis, invokeTimeoutMillis)) {}

MQClientInstance::~MQClientInstance() { shutdown(); }

void MQClientInstance::start() {
    started_ = true;
    std::string ns;
    for (size_t i = 0; i < nameServerAddrs_.size(); ++i) {
        if (i) ns += ";";
        ns += nameServerAddrs_[i];
    }
    logger_info("MQClientInstance[" + clientId_ + "] started, namesrv=" + ns);
}

void MQClientInstance::shutdown() {
    started_ = false;
    if (remotingClient_) {
        remotingClient_->shutdown();
    }
}

std::vector<std::string> MQClientInstance::nameServerAddrs() const {
    return nameServerAddrs_;
}

void MQClientInstance::updateNameServerAddressList(const std::vector<std::string>& addrs) {
    if (!addrs.empty()) {
        nameServerAddrs_ = addrs;
    }
}

RemotingCommand MQClientInstance::invokeSyncOnAddr(const std::string& addr,
                                                  RemotingCommand& request,
                                                  int32_t timeoutMillis) {
    return remotingClient_->invokeSync(addr, request, timeoutMillis);
}

void MQClientInstance::checkResponseCode(const RemotingCommand& response) {
    if (response.code != ResponseCode::SUCCESS) {
        throw MQBrokerException(response.code, response.remark);
    }
}

// ---------------------------------------------------------------- 路由管理
bool MQClientInstance::updateTopicRouteInfoFromNameServer(const std::string& topic,
                                                         int32_t timeoutMillis) {
    if (nameServerAddrs_.empty()) {
        throw MQClientException("name server address list is empty");
    }

    auto fetch = [&](const std::string& t, TopicRouteData& out) -> bool {
        RemotingCommand request =
            RemotingCommand::createRequestCommand(RequestCode::GET_ROUTEINFO_BY_TOPIC, nullptr);
        request.extFields["topic"] = t;
        std::string lastError;
        for (const std::string& nsAddr : nameServerAddrs_) {
            try {
                RemotingCommand response = invokeSyncOnAddr(nsAddr, request, timeoutMillis);
                if (response.code == ResponseCode::SUCCESS && !response.body.empty()) {
                    return TopicRouteData::decode(response.body, out);
                }
                // 第一个可达的 NS 明确返回非 SUCCESS（如 TOPIC_NOT_EXIST）就停止轮询
                break;
            } catch (const RemotingException& e) {
                lastError = e.what();
                continue;
            }
        }
        (void)lastError;
        return false;
    };

    TopicRouteData route;
    bool ok = fetch(topic, route);
    if (!ok && topic != MixAll::DEFAULT_TOPIC) {
        // 5.x nameServer 不为未知 topic 合成默认路由（返回 TOPIC_NOT_EXIST），
        // 需像 Java 客户端那样回退到默认 topic（TBW102）来构造发布信息。
        // 新 topic 由 broker 用 defaultTopicQueueNums 创建队列，而默认 topic 自身
        // 可能配置了更多队列，这里按 broker 实际创建数裁剪，避免选中非法 queueId。
        TopicRouteData defaultRoute;
        if (fetch(MixAll::DEFAULT_TOPIC, defaultRoute)) {
            for (QueueData& qd : defaultRoute.queueDatas) {
                if (qd.writeQueueNums > MixAll::DEFAULT_TOPIC_QUEUE_NUMS) {
                    qd.writeQueueNums = MixAll::DEFAULT_TOPIC_QUEUE_NUMS;
                }
                if (qd.readQueueNums > MixAll::DEFAULT_TOPIC_QUEUE_NUMS) {
                    qd.readQueueNums = MixAll::DEFAULT_TOPIC_QUEUE_NUMS;
                }
            }
            route = defaultRoute;
            ok = true;
        }
    }
    if (!ok) {
        return false;
    }

    std::lock_guard<std::recursive_mutex> lk(routeLock_);
    topicRouteTable_[topic] = route;
    auto it = topicPublishInfoTable_.find(topic);
    if (it == topicPublishInfoTable_.end() || it->second == nullptr) {
        it = topicPublishInfoTable_.emplace(topic, std::make_shared<TopicPublishInfo>()).first;
    }
    TopicPublishInfo& publish = *it->second;
    publish.orderTopic = !route.orderTopicConf.empty();
    publish.topicRouteData = route;
    publish.msgQueueList = route.getAllMessageQueue(topic);
    if (topic.find("ORDER") != std::string::npos) {
        publish.orderTopic = true;
    }
    return true;
}

std::shared_ptr<TopicPublishInfo> MQClientInstance::getTopicPublishInfo(const std::string& topic) {
    {
        std::lock_guard<std::recursive_mutex> lk(routeLock_);
        auto it = topicPublishInfoTable_.find(topic);
        if (it != topicPublishInfoTable_.end() && it->second != nullptr && it->second->ok()) {
            return it->second;
        }
    }
    updateTopicRouteInfoFromNameServer(topic);
    std::lock_guard<std::recursive_mutex> lk(routeLock_);
    auto it = topicPublishInfoTable_.find(topic);
    if (it == topicPublishInfoTable_.end() || it->second == nullptr || !it->second->ok()) {
        throw MQClientException("Can not find Message Queue for topic: " + topic);
    }
    return it->second;
}

std::shared_ptr<TopicRouteData> MQClientInstance::getTopicRouteData(const std::string& topic) {
    {
        std::lock_guard<std::recursive_mutex> lk(routeLock_);
        auto it = topicRouteTable_.find(topic);
        if (it != topicRouteTable_.end()) {
            return std::make_shared<TopicRouteData>(it->second);
        }
    }
    try {
        updateTopicRouteInfoFromNameServer(topic);
    } catch (const std::exception&) {
        // 与 Python 一致：路由刷新失败不抛，交给下面的查表返回空
    }
    std::lock_guard<std::recursive_mutex> lk(routeLock_);
    auto it = topicRouteTable_.find(topic);
    if (it == topicRouteTable_.end()) {
        return nullptr;
    }
    return std::make_shared<TopicRouteData>(it->second);
}

std::string MQClientInstance::findBrokerAddrInRoute(const TopicRouteData& route,
                                                   const std::string& brokerName) {
    for (const BrokerData& bd : route.brokerDatas) {
        if (bd.brokerName == brokerName) {
            return bd.selectBrokerAddr();
        }
    }
    return std::string();
}

std::string MQClientInstance::brokerAddr(const MessageQueue& mq) {
    auto route = getTopicRouteData(mq.topic);
    if (route == nullptr) {
        throw MQClientNoRouteException(mq.topic);
    }
    std::string addr = findBrokerAddrInRoute(*route, mq.brokerName);
    if (addr.empty()) {
        throw MQClientException("Broker " + mq.brokerName + " not found in route of topic "
                                + mq.topic);
    }
    return addr;
}

std::string MQClientInstance::brokerAddrOf(const std::string& brokerName) {
    std::lock_guard<std::recursive_mutex> lk(routeLock_);
    for (const auto& kv : topicRouteTable_) {
        std::string addr = findBrokerAddrInRoute(kv.second, brokerName);
        if (!addr.empty()) {
            return addr;
        }
    }
    return std::string();
}

std::vector<std::string> MQClientInstance::getRouteOfAllBrokers() {
    std::vector<std::string> addrs;
    std::lock_guard<std::recursive_mutex> lk(routeLock_);
    for (const auto& kv : topicRouteTable_) {
        for (const BrokerData& bd : kv.second.brokerDatas) {
            std::string a = bd.selectBrokerAddr();
            if (!a.empty() && std::find(addrs.begin(), addrs.end(), a) == addrs.end()) {
                addrs.push_back(a);
            }
        }
    }
    return addrs;
}

std::vector<std::string> MQClientInstance::knownBrokerAddrs() { return getRouteOfAllBrokers(); }

// ---------------------------------------------------------------- 消息发送
SendResult MQClientInstance::sendMessage(const std::string& producerGroup, const Message& msg,
                                        const MessageQueue& mq, int32_t timeoutMillis,
                                        int32_t sysFlag) {
    const std::string addr = brokerAddr(mq);

    auto header = std::make_shared<SendMessageRequestHeaderV2>();
    header->producerGroup = producerGroup;
    header->topic = msg.topic;
    header->defaultTopic = MixAll::DEFAULT_TOPIC;
    header->defaultTopicQueueNums = MixAll::DEFAULT_TOPIC_QUEUE_NUMS;
    header->queueId = mq.queueId;
    header->sysFlag = sysFlag;
    header->bornTimestamp = UtilAll::currentTimeMillis();
    header->flag = msg.flag;
    header->properties = messagePropertiesToString(msg.properties);
    header->reconsumeTimes = 0;
    header->unitMode = false;
    header->maxReconsumeTimes = 0;
    header->batch = msg.isBatch;

    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE_V2, header);
    request.body = msg.body;
    request.hasBody = true;

    RemotingCommand response = invokeSyncOnAddr(addr, request, timeoutMillis);

    SendStatus status;
    switch (response.code) {
        case ResponseCode::SUCCESS: status = SendStatus::SEND_OK; break;
        case ResponseCode::FLUSH_DISK_TIMEOUT: status = SendStatus::FLUSH_DISK_TIMEOUT; break;
        case ResponseCode::FLUSH_SLAVE_TIMEOUT: status = SendStatus::FLUSH_SLAVE_TIMEOUT; break;
        case ResponseCode::SLAVE_NOT_AVAILABLE: status = SendStatus::SLAVE_NOT_AVAILABLE; break;
        default:
            throw MQBrokerException(response.code, response.remark);
    }

    SendMessageResponseHeader respHeader;
    respHeader.fromExtFields(response.extFields);
    SendResult result;
    result.sendStatus = status;
    result.msgId = respHeader.msgId.value_or("");
    result.offsetMsgId = respHeader.msgId.value_or("");
    result.messageQueue = MessageQueue(mq.topic, mq.brokerName,
                                       respHeader.queueId.value_or(mq.queueId));
    result.queueOffset = respHeader.queueOffset.value_or(0);
    result.transactionId = respHeader.transactionId.value_or("");
    return result;
}

void MQClientInstance::sendMessageOneway(const std::string& producerGroup, const Message& msg,
                                        const MessageQueue& mq, int32_t timeoutMillis,
                                        int32_t sysFlag) {
    const std::string addr = brokerAddr(mq);
    (void)timeoutMillis;

    auto header = std::make_shared<SendMessageRequestHeaderV2>();
    header->producerGroup = producerGroup;
    header->topic = msg.topic;
    header->defaultTopic = MixAll::DEFAULT_TOPIC;
    header->defaultTopicQueueNums = MixAll::DEFAULT_TOPIC_QUEUE_NUMS;
    header->queueId = mq.queueId;
    header->sysFlag = sysFlag;
    header->bornTimestamp = UtilAll::currentTimeMillis();
    header->flag = msg.flag;
    header->properties = messagePropertiesToString(msg.properties);
    header->reconsumeTimes = 0;
    header->unitMode = false;
    header->maxReconsumeTimes = 0;
    header->batch = msg.isBatch;

    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE_V2, header);
    request.body = msg.body;
    request.hasBody = true;
    request.markOnewayRpc();
    remotingClient_->invokeOneway(addr, request);
}

// ---------------------------------------------------------------- 消息拉取
PullResult MQClientInstance::pullMessage(const std::string& consumerGroup, const MessageQueue& mq,
                                        int64_t queueOffset, int32_t maxMsgNums, int32_t sysFlag,
                                        int64_t commitOffset, const std::string& subscription,
                                        int64_t subVersion, const std::string& expressionType,
                                        int32_t timeoutMillis, int32_t maxMsgBytes,
                                        int32_t suspendTimeoutMillis, const std::string& addrIn,
                                        int32_t requestSource) {
    std::string addr = addrIn;
    if (addr.empty()) {
        addr = brokerAddr(mq);
    }

    auto header = std::make_shared<PullMessageRequestHeader>();
    header->consumerGroup = consumerGroup;
    header->topic = mq.topic;
    header->queueId = mq.queueId;
    header->queueOffset = queueOffset;
    header->maxMsgNums = maxMsgNums;
    header->sysFlag = sysFlag;
    header->commitOffset = commitOffset;
    header->suspendTimeoutMillis = suspendTimeoutMillis;
    header->subscription = subscription;
    header->subVersion = subVersion;
    header->expressionType = expressionType;
    header->maxMsgBytes = maxMsgBytes;
    header->requestSource = requestSource;

    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::PULL_MESSAGE, header);
    RemotingCommand response = invokeSyncOnAddr(addr, request, timeoutMillis);

    PullStatus status = PullStatus::NO_NEW_MSG;
    if (!mapPullStatus(response.code, status)) {
        throw MQBrokerException(response.code, response.remark);
    }

    PullMessageResponseHeader respHeader;
    respHeader.fromExtFields(response.extFields);

    PullResult result;
    result.status = status;
    result.nextBeginOffset = respHeader.nextBeginOffset.value_or(0);
    result.minOffset = respHeader.minOffset.value_or(0);
    result.maxOffset = respHeader.maxOffset.value_or(0);
    if (!response.body.empty()) {
        result.msgFoundList = decodeMessages(response.body);
        for (MessageExt& m : result.msgFoundList) {
            m.brokerName = mq.brokerName;
            m.queueId = mq.queueId;
        }
    }
    return result;
}

// ---------------------------------------------------------------- 消费位点
bool MQClientInstance::queryConsumerOffset(const std::string& consumerGroup,
                                          const MessageQueue& mq, int64_t& outOffset,
                                          int32_t timeoutMillis, const std::string& addrIn,
                                          bool setZeroIfNotFound) {
    std::string addr = addrIn.empty() ? brokerAddr(mq) : addrIn;
    auto header = std::make_shared<QueryConsumerOffsetRequestHeader>();
    header->consumerGroup = consumerGroup;
    header->topic = mq.topic;
    header->queueId = mq.queueId;
    // setZeroIfNotFound 在 Java 里是 header 字段
    auto request = RemotingCommand::createRequestCommand(RequestCode::QUERY_CONSUMER_OFFSET, header);
    // QueryConsumerOffsetRequestHeader 无该字段时，Java 会把未找到当错误；这里按需附加
    if (setZeroIfNotFound) {
        request.extFields["setZeroIfNotFound"] = "true";
    }
    RemotingCommand response = invokeSyncOnAddr(addr, request, timeoutMillis);
    if (response.code == ResponseCode::QUERY_NOT_FOUND) {
        return false;
    }
    checkResponseCode(response);
    QueryConsumerOffsetResponseHeader respHeader;
    respHeader.fromExtFields(response.extFields);
    outOffset = respHeader.offset.value_or(0);
    return true;
}

void MQClientInstance::updateConsumerOffset(const std::string& consumerGroup,
                                           const MessageQueue& mq, int64_t commitOffset,
                                           int32_t timeoutMillis, const std::string& addrIn) {
    std::string addr = addrIn.empty() ? brokerAddr(mq) : addrIn;
    auto header = std::make_shared<UpdateConsumerOffsetRequestHeader>();
    header->consumerGroup = consumerGroup;
    header->topic = mq.topic;
    header->queueId = mq.queueId;
    header->commitOffset = commitOffset;
    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::UPDATE_CONSUMER_OFFSET, header);
    RemotingCommand response = invokeSyncOnAddr(addr, request, timeoutMillis);
    checkResponseCode(response);
}

int64_t MQClientInstance::getMaxOffset(const MessageQueue& mq, int32_t timeoutMillis,
                                      const std::string& addrIn) {
    std::string addr = addrIn.empty() ? brokerAddr(mq) : addrIn;
    auto header = std::make_shared<GetMaxOffsetRequestHeader>();
    header->topic = mq.topic;
    header->queueId = mq.queueId;
    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::GET_MAX_OFFSET, header);
    RemotingCommand response = invokeSyncOnAddr(addr, request, timeoutMillis);
    checkResponseCode(response);
    GetMaxOffsetResponseHeader respHeader;
    respHeader.fromExtFields(response.extFields);
    return respHeader.offset.value_or(0);
}

int64_t MQClientInstance::getMinOffset(const MessageQueue& mq, int32_t timeoutMillis,
                                      const std::string& addrIn) {
    std::string addr = addrIn.empty() ? brokerAddr(mq) : addrIn;
    auto header = std::make_shared<GetMinOffsetRequestHeader>();
    header->topic = mq.topic;
    header->queueId = mq.queueId;
    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::GET_MIN_OFFSET, header);
    RemotingCommand response = invokeSyncOnAddr(addr, request, timeoutMillis);
    checkResponseCode(response);
    GetMinOffsetResponseHeader respHeader;
    respHeader.fromExtFields(response.extFields);
    return respHeader.offset.value_or(0);
}

int64_t MQClientInstance::searchOffsetByTimestamp(const MessageQueue& mq, int64_t timestamp,
                                                 int32_t timeoutMillis,
                                                 const std::string& addrIn) {
    std::string addr = addrIn.empty() ? brokerAddr(mq) : addrIn;
    auto header = std::make_shared<SearchOffsetRequestHeader>();
    header->topic = mq.topic;
    header->queueId = mq.queueId;
    header->timestamp = timestamp;
    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::SEARCH_OFFSET_BY_TIMESTAMP, header);
    RemotingCommand response = invokeSyncOnAddr(addr, request, timeoutMillis);
    checkResponseCode(response);
    SearchOffsetResponseHeader respHeader;
    respHeader.fromExtFields(response.extFields);
    return respHeader.offset.value_or(0);
}

// ---------------------------------------------------------------- 按 Key 查询
bool MQClientInstance::queryMessage(const std::string& topic, const std::string& key,
                                   int32_t maxNum, int64_t beginTimestamp, int64_t endTimestamp,
                                   Bytes& outBody, int32_t timeoutMillis,
                                   const std::string& addrIn,
                                   const std::string& indexType, bool uniqKey) {
    std::string addr = addrIn;
    if (addr.empty()) {
        auto route = getTopicRouteData(topic);
        if (route == nullptr) {
            throw MQClientNoRouteException(topic);
        }
        if (route->brokerDatas.empty()) {
            throw MQClientException("no broker in route of topic " + topic);
        }
        addr = findBrokerAddrInRoute(*route, route->brokerDatas[0].brokerName);
    }
    auto header = std::make_shared<QueryMessageRequestHeader>();
    header->topic = topic;
    header->key = key;
    header->maxNum = maxNum;
    header->beginTimestamp = beginTimestamp;
    header->endTimestamp = endTimestamp;
    if (!indexType.empty()) header->indexType = indexType;
    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::QUERY_MESSAGE, header);
    if (uniqKey) {
        // Java MixAll.UNIQUE_MSG_QUERY_FLAG："_UNIQUE_KEY_QUERY"="true"。
        // 注意它是 extFields 的**键名**，不是标志位（早期实现写成数字键是错的）。
        request.extFields[MixAll::UNIQUE_MSG_QUERY_FLAG] = "true";
    }
    RemotingCommand response = invokeSyncOnAddr(addr, request, timeoutMillis);
    if (response.code == ResponseCode::QUERY_NOT_FOUND) {
        return false;
    }
    checkResponseCode(response);
    outBody = response.body;
    return true;
}

std::vector<MessageExt> MQClientInstance::queryMessageAllBrokers(
    const std::string& topic, const std::string& key, int32_t maxNum, int64_t beginTimestamp,
    int64_t endTimestamp, const std::string& indexType, bool uniqKey, int32_t timeoutMillis) {
    // 对应 Java MQAdminImpl.queryMessage：查该 topic **所有** broker 并合并去重后的消息。
    // Java 还会做客户端侧二次校验（uniqKey 命中要求 msgId == key；普通 key 命中要求
    // message.keys 拆分后含 key 且 topic 相同），这里保持一致。
    std::vector<MessageExt> messages;
    auto route = getTopicRouteData(topic);
    if (route == nullptr) {
        return messages;
    }
    for (const BrokerData& bd : route->brokerDatas) {
        std::string addr = bd.selectBrokerAddr();
        if (addr.empty()) continue;
        Bytes body;
        try {
            if (!queryMessage(topic, key, maxNum, beginTimestamp, endTimestamp, body,
                              timeoutMillis, addr, indexType, uniqKey)) {
                continue;
            }
        } catch (const std::exception&) {
            continue;
        }
        if (body.empty()) continue;
        std::vector<MessageExt> decoded = decodeMessages(body, true);
        for (MessageExt& m : decoded) {
            m.brokerName = bd.brokerName;
            if (uniqKey) {
                if (m.msgId == key) messages.push_back(m);
            } else {
                std::string keys = m.getKeys();
                if (!keys.empty()) {
                    // KEYS 以空格（MessageConst::KEY_SEPARATOR）分隔
                    size_t pos = 0;
                    bool hit = false;
                    while (pos <= keys.size()) {
                        size_t sp = keys.find(' ', pos);
                        std::string piece = (sp == std::string::npos)
                                                ? keys.substr(pos)
                                                : keys.substr(pos, sp - pos);
                        if (piece == key && m.topic == topic) {
                            hit = true;
                            break;
                        }
                        if (sp == std::string::npos) break;
                        pos = sp + 1;
                    }
                    if (hit) messages.push_back(m);
                }
            }
        }
    }
    std::sort(messages.begin(), messages.end(),
              [](const MessageExt& a, const MessageExt& b) { return a.queueOffset < b.queueOffset; });
    if (maxNum > 0 && messages.size() > static_cast<size_t>(maxNum)) {
        messages.resize(static_cast<size_t>(maxNum));
    }
    return messages;
}

// ---------------------------------------------------------------- 心跳 / 注销
void MQClientInstance::sendHeartbeat(const std::string& addr, const HeartbeatData& heartbeatData,
                                    int32_t timeoutMillis) {
    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT, nullptr);
    request.body = heartbeatData.encode();
    request.hasBody = true;
    RemotingCommand response = invokeSyncOnAddr(addr, request, timeoutMillis);
    checkResponseCode(response);
}

void MQClientInstance::unregisterClient(const std::string& addr, const std::string& clientId,
                                       const std::string& producerGroup,
                                       const std::string& consumerGroup, int32_t timeoutMillis) {
    auto header = std::make_shared<UnregisterClientRequestHeader>();
    header->clientId = clientId;
    header->producerGroup = producerGroup;
    header->consumerGroup = consumerGroup;
    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::UNREGISTER_CLIENT, header);
    RemotingCommand response = invokeSyncOnAddr(addr, request, timeoutMillis);
    checkResponseCode(response);
}

// ---------------------------------------------------------------- 通用同步调用
RemotingCommand MQClientInstance::invokeSyncRaw(const std::string& addr, int32_t code,
                                               const PropertyMap& extFields, const Bytes& body,
                                               bool hasBody, int32_t timeoutMillis,
                                               int32_t languageOverride) {
    RemotingCommand request = RemotingCommand::createRequestCommand(code, nullptr);
    for (const auto& kv : extFields) {
        request.extFields[kv.first] = kv.second;
    }
    if (hasBody) {
        request.body = body;
        request.hasBody = true;
    }
    if (languageOverride >= 0) {
        request.language = static_cast<uint8_t>(languageOverride);
    }
    return remotingClient_->invokeSync(addr, request, timeoutMillis);
}

RemotingCommand MQClientInstance::invokeSync(const std::string& addr, int32_t code,
                                            const PropertyMap& extFields, const Bytes& body,
                                            bool hasBody, int32_t timeoutMillis,
                                            int32_t languageOverride) {
    RemotingCommand response = invokeSyncRaw(addr, code, extFields, body, hasBody,
                                             timeoutMillis, languageOverride);
    checkResponseCode(response);
    return response;
}

// ---------------------------------------------------------------- 管理类
void MQClientInstance::createTopicInBroker(const std::string& brokerAddr,
                                          const std::string& defaultTopic,
                                          const std::string& topic, int32_t readQueueNums,
                                          int32_t writeQueueNums, int32_t perm,
                                          int32_t topicSysFlag,
                                          const std::string& topicFilterType,
                                          const std::string& attributes, bool force,
                                          int32_t timeoutMillis, int32_t retryTimes) {
    auto header = std::make_shared<CreateTopicRequestHeader>();
    header->topic = topic;
    header->defaultTopic = defaultTopic;
    header->readQueueNums = readQueueNums;
    header->writeQueueNums = writeQueueNums;
    header->perm = perm;
    // ⚠ 必须下发 topicFilterType：broker 的 CreateTopicRequestHeader.checkFields()
    // 会把它转成枚举，为空直接报 "topicFilterType = [null] value invalid"。
    // attributes 必须是 ""（Java AttributeParser.parseToString(空 map) 的结果）而非 null。
    // 这两条都是先在 Python 侧被真实 broker 打回、再回填到 C++ 的。
    header->topicFilterType = topicFilterType;
    header->topicSysFlag = topicSysFlag;
    header->order = false;
    header->attributes = attributes;
    header->force = force;
    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::UPDATE_AND_CREATE_TOPIC, header);

    // Java MQAdminImpl.createTopic 对每个 broker 重试 5 次（连接抖动容忍）
    std::string lastError;
    int attempts = retryTimes > 0 ? retryTimes : 1;
    for (int i = 0; i < attempts; ++i) {
        try {
            RemotingCommand response = invokeSyncOnAddr(brokerAddr, request, timeoutMillis);
            checkResponseCode(response);
            return;
        } catch (const MQBrokerException&) {
            // broker 明确拒绝（如 TOPIC_EXIST_ALREADY）不重试，直接上抛
            throw;
        } catch (const std::exception& e) {
            lastError = e.what();
        }
    }
    throw MQClientException("create topic [" + topic + "] in broker " + brokerAddr
                            + " failed after " + std::to_string(attempts)
                            + " attempts: " + lastError);
}

void MQClientInstance::createTopicInRoute(const std::string& topic, int32_t readQueueNums,
                                         int32_t writeQueueNums, int32_t perm,
                                         int32_t timeoutMillis) {
    auto route = getTopicRouteData(MixAll::DEFAULT_TOPIC);
    if (route == nullptr) {
        throw MQClientException("No route info of default topic " + std::string(MixAll::DEFAULT_TOPIC));
    }
    bool createdAtLeastOnce = false;
    std::string lastError;
    for (const BrokerData& bd : route->brokerDatas) {
        std::string addr = bd.selectBrokerAddr();
        if (addr.empty()) continue;
        try {
            createTopicInBroker(addr, MixAll::DEFAULT_TOPIC, topic, readQueueNums, writeQueueNums,
                                perm, 0, TopicFilterType::SINGLE_TAG, std::string(), false,
                                timeoutMillis);
            createdAtLeastOnce = true;
        } catch (const std::exception& e) {
            lastError = e.what();
        }
    }
    if (!createdAtLeastOnce) {
        throw MQClientException("create new topic failed: " + lastError);
    }
}

void MQClientInstance::deleteTopicInBroker(const std::string& brokerAddr,
                                          const std::string& topic, int32_t timeoutMillis) {
    PropertyMap ext;
    ext["topic"] = topic;
    invokeSync(brokerAddr, RequestCode::DELETE_TOPIC_IN_BROKER, ext, Bytes(), false, timeoutMillis);
}

void MQClientInstance::deleteTopicInNamesrv(const std::string& topic, int32_t timeoutMillis) {
    PropertyMap ext;
    ext["topic"] = topic;
    std::string lastError;
    for (const std::string& nsAddr : nameServerAddrs_) {
        try {
            invokeSync(nsAddr, RequestCode::DELETE_TOPIC_IN_NAMESRV, ext, Bytes(), false,
                       timeoutMillis);
            return;
        } catch (const std::exception& e) {
            lastError = e.what();
        }
    }
    throw MQClientException("Failed to delete topic " + topic + " in name server: " + lastError);
}

// ---------------------------------------------------------------- 集群 / Topic / 消费者列表
ClusterInfo MQClientInstance::getBrokerClusterInfo(int32_t timeoutMillis) {
    PropertyMap none;
    for (const std::string& nsAddr : nameServerAddrs_) {
        try {
            RemotingCommand response = invokeSyncRaw(
                nsAddr, RequestCode::GET_BROKER_CLUSTER_INFO, none, Bytes(), false, timeoutMillis);
            if (response.code == ResponseCode::SUCCESS && !response.body.empty()) {
                ClusterInfo ci;
                ClusterInfo::decode(response.body, ci);
                return ci;
            }
        } catch (const std::exception&) {
            continue;
        }
    }
    throw MQClientException("Failed to get broker cluster info from name server");
}

TopicList MQClientInstance::getAllTopicListFromNameServer(int32_t timeoutMillis) {
    PropertyMap none;
    for (const std::string& nsAddr : nameServerAddrs_) {
        try {
            RemotingCommand response = invokeSyncRaw(
                nsAddr, RequestCode::GET_ALL_TOPIC_LIST_FROM_NAMESERVER, none, Bytes(), false,
                timeoutMillis);
            if (response.code == ResponseCode::SUCCESS && !response.body.empty()) {
                TopicList tl;
                TopicList::decode(response.body, tl);
                return tl;
            }
        } catch (const std::exception&) {
            continue;
        }
    }
    throw MQClientException("Failed to get all topic list from name server");
}

GetConsumerListByGroupResponseBody MQClientInstance::getConsumerListByGroup(
    const std::string& consumerGroup, const std::string& addr, int32_t timeoutMillis) {
    auto header = std::make_shared<GetConsumerListByGroupRequestHeader>();
    header->consumerGroup = consumerGroup;
    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::GET_CONSUMER_LIST_BY_GROUP, header);
    RemotingCommand response = invokeSyncOnAddr(addr, request, timeoutMillis);
    checkResponseCode(response);
    GetConsumerListByGroupResponseBody out;
    if (!response.body.empty()) {
        GetConsumerListByGroupResponseBody::decode(response.body, out);
    }
    return out;
}

// ---------------------------------------------------------------- 工具
std::string MQClientInstance::brokerAddrForMq(const MessageQueue& mq) { return brokerAddr(mq); }

}  // namespace rocketmq
