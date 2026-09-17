// MQClientInstance 实现（对应 Python client/mq_client.py）。
#include "rocketmq/client/mq_client.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/request_reply.h"
#include "rocketmq/client/trace.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/exception.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/extra_info.h"
#include "rocketmq/remoting/protocol/headers.h"
#include "rocketmq/remoting/protocol/json.h"

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

std::optional<MessageQueue> TopicPublishInfo::selectOneMessageQueue(
    const std::function<bool(const MessageQueue&)>& filter,
    const std::function<bool(const MessageQueue&)>& brokerFilter) {
    if (msgQueueList.empty()) {
        throw MQClientException("no message queue for publish info");
    }
    // 对应 Python select_one_message_queue(*filters)：游标照常推进，
    // 一轮内全部不匹配返回 nullopt（由调用方退化选择，不在这里兜底）。
    const size_t n = msgQueueList.size();
    for (size_t i = 0; i < n; ++i) {
        uint64_t idx = index_.fetch_add(1);
        MessageQueue mq = msgQueueList[static_cast<size_t>(idx % n)];
        if (filter(mq) && brokerFilter(mq)) {
            return mq;
        }
    }
    return std::nullopt;
}

// ---------------------------------------------------------------- 生命周期
MQClientInstance::MQClientInstance(const std::string& clientId,
                                  const std::vector<std::string>& nameServerAddrs,
                                  int32_t connectTimeoutMillis, int32_t invokeTimeoutMillis)
    : clientId_(clientId), nameServerAddrs_(nameServerAddrs),
      remotingClient_(new RemotingClient(connectTimeoutMillis, invokeTimeoutMillis)) {
    // Request-Reply：broker 用 PUSH_REPLY_MESSAGE_TO_CLIENT(326) 把应答推回来。
    // 对应 Java MQClientAPIImpl 构造函数里的
    // registerProcessor(PUSH_REPLY_MESSAGE_TO_CLIENT, clientRemotingProcessor, null)
    // —— 它是**客户端实例级**的（与具体 producer 无关，应答按 clientId 推回），
    // 所以注册在构造函数里。处理器绝不能在回调里同步发请求（回调在读线程上执行）。
    remotingClient_->registerProcessor(
        RequestCode::PUSH_REPLY_MESSAGE_TO_CLIENT,
        [](const RemotingCommand& cmd, const std::string& addr) {
            return processReplyMessage(cmd, addr);
        });
}

MQClientInstance::~MQClientInstance() { shutdown(); }

void MQClientInstance::start() {
    started_ = true;
    // 动态 name server（Java MQClientInstance.start:344-348）：**当且仅当**没配置
    // 静态地址时先 fetch 一次；取不到直接报错（比 Java 更严格——Java 会让运行期各处
    // 各自失败，这里在 start 时给一个明确错误）。
    if (nameServerAddrs_.empty() && !topAddressing_.wsAddr().empty()) {
        fetchNameServerAddr();
        if (nameServerAddrs_.empty()) {
            throw MQClientException("name server address is not set and address server ("
                                    + topAddressing_.wsAddr() + ") returned none");
        }
        // 周期刷新（Java scheduleAtFixedRate(fetchNameServerAddr, 10s, 2min)）
        if (!namesrvRefreshThread_.joinable()) {
            namesrvRefreshStop_ = false;
            namesrvRefreshThread_ = std::thread([this]() {
                setThreadName("MQClientFactoryScheduledThread-NS");
                namesrvRefreshLoop();
            });
        }
    }
    std::string ns;
    for (size_t i = 0; i < nameServerAddrs_.size(); ++i) {
        if (i) ns += ";";
        ns += nameServerAddrs_[i];
    }
    logger_info("MQClientInstance[" + clientId_ + "] started, namesrv=" + ns);
    // 周期刷新在用 topic 路由（对应 Java MQClientInstance.startScheduledTask 用
    // pollNameServerInterval，默认 30s）。没有它，新 topic 被 broker 创建、队列扩容等
    // 变化只能等消费者自己的 rebalance 轮次或生产者的下次发送才被发现。
    if (!routeRefreshThread_.joinable()) {
        routeRefreshStop_ = false;
        routeRefreshThread_ = std::thread([this]() {
            setThreadName("MQClientFactoryScheduledThread");
            routeRefreshLoop();
        });
    }
}

void MQClientInstance::registerTopicInUse(const std::string& topic) {
    if (!topic.empty()) {
        std::lock_guard<std::recursive_mutex> lk(routeLock_);
        topicsInUse_.insert(topic);
    }
}

void MQClientInstance::routeRefreshLoop() {
    // 首次延迟 ~10ms 后再开始周期刷新（对应 Java 的 10ms initialDelay）
    if (routeRefreshStop_) return;
    for (int i = 0; i < 10 && !routeRefreshStop_; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    while (!routeRefreshStop_) {
        // 周期 30s（对应 Java pollNameServerInterval 默认 30000ms）
        for (int i = 0; i < 300 && !routeRefreshStop_; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (routeRefreshStop_) return;
        if (!started_) return;
        std::set<std::string> topics;
        {
            std::lock_guard<std::recursive_mutex> lk(routeLock_);
            topics = topicsInUse_;
        }
        for (const std::string& topic : topics) {
            try {
                updateTopicRouteInfoFromNameServer(topic);
            } catch (const std::exception& e) {
                logger_debug("route refresh failed for " + topic + ": " + e.what());
            }
        }
    }
}

void MQClientInstance::shutdown() {
    started_ = false;
    routeRefreshStop_ = true;
    namesrvRefreshStop_ = true;
    if (namesrvRefreshThread_.joinable()) {
        namesrvRefreshThread_.join();
    }
    if (routeRefreshThread_.joinable()) {
        routeRefreshThread_.join();
    }
    if (remotingClient_) {
        remotingClient_->shutdown();
    }
}

void MQClientInstance::fetchNameServerAddr() {
    // Java MQClientAPIImpl.fetchNameServerAddr：地址**变化才应用**（按 ';' 切分）
    std::string changed = topAddressing_.fetchAndApply();
    if (changed.empty()) return;
    std::vector<std::string> addrs;
    std::string item;
    std::istringstream iss(changed);
    while (std::getline(iss, item, ';')) {
        const std::string t = clearNewLine(item);
        if (!t.empty()) addrs.push_back(t);
    }
    updateNameServerAddressList(addrs);
}

void MQClientInstance::namesrvRefreshLoop() {
    // Java scheduleAtFixedRate(fetchNameServerAddr, 10s, 2min)：首次延迟 10s、周期 2min
    for (int i = 0; i < 100 && !namesrvRefreshStop_; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    while (!namesrvRefreshStop_) {
        if (!started_) return;
        try {
            fetchNameServerAddr();
        } catch (const std::exception& e) {
            logger_debug(std::string("fetchNameServerAddr exception: ") + e.what());
        }
        for (int i = 0; i < 1200 && !namesrvRefreshStop_; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
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
                                                         int32_t timeoutMillis,
                                                         bool isDefault) {
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
    if (!ok && isDefault && topic != MixAll::DEFAULT_TOPIC) {
        // 5.x nameServer 不为未知 topic 合成默认路由（返回 TOPIC_NOT_EXIST），
        // **生产者**需要像 Java 客户端那样回退到默认 topic（TBW102）来为该 topic
        // 构造发布信息。isDefault=false 时（消费者路径）不做这个兜底。
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

std::shared_ptr<TopicPublishInfo> MQClientInstance::getTopicPublishInfo(const std::string& topic,
                                                                      bool isDefault) {
    {
        std::lock_guard<std::recursive_mutex> lk(routeLock_);
        auto it = topicPublishInfoTable_.find(topic);
        if (it != topicPublishInfoTable_.end() && it->second != nullptr && it->second->ok()) {
            return it->second;
        }
    }
    updateTopicRouteInfoFromNameServer(topic, 5000, isDefault);
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

    // Request-Reply：MSG_TYPE == "reply" 的应答消息走 SEND_REPLY_MESSAGE_V2(325)，
    // 而不是普通的 SEND_MESSAGE_V2(314)。broker 只在 324/325 上注册了
    // ReplyMessageProcessor（它负责按 REPLY_TO_CLIENT 把应答推回请求方）。
    RemotingCommand request = RemotingCommand::createRequestCommand(
        isReplyMessage(msg) ? RequestCode::SEND_REPLY_MESSAGE_V2
                            : RequestCode::SEND_MESSAGE_V2,
        header);
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
    // msgId 对齐 Java processSendResponse：客户端 UNIQ_KEY（批量 = 逗号拼接）；
    // offsetMsgId 是 broker 的 offset 消息 ID（响应头的 msgId）。
    result.msgId = msg.getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    result.offsetMsgId = respHeader.msgId.value_or("");
    result.messageQueue = MessageQueue(mq.topic, mq.brokerName,
                                       respHeader.queueId.value_or(mq.queueId));
    result.queueOffset = respHeader.queueOffset.value_or(0);
    result.transactionId = respHeader.transactionId.value_or("");
    // MSG_REGION / TRACE_ON 来自响应头 extFields（对齐 Java processSendResponse）。
    // 缺省 region=DefaultRegion，traceOn=true（Java: !"false".equals(TRACE_ON)）。
    auto regionIt = response.extFields.find(MessageConst::PROPERTY_MSG_REGION);
    std::string region = (regionIt != response.extFields.end()) ? regionIt->second : std::string();
    result.regionId = region.empty() ? std::string(TraceConstants::DEFAULT_TRACE_REGION_ID) : region;
    auto traceIt = response.extFields.find(TraceConstants::PROPERTY_TRACE_SWITCH);
    std::string traceOn =
        (traceIt != response.extFields.end()) ? traceIt->second : std::string();
    result.traceOn = (traceOn != "false");
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

    RemotingCommand request = RemotingCommand::createRequestCommand(
        isReplyMessage(msg) ? RequestCode::SEND_REPLY_MESSAGE_V2
                            : RequestCode::SEND_MESSAGE_V2,
        header);
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

// ---------------------------------------------------------------- POP 模式

namespace {

// 从 topic 路由解析出 (brokerName, addr)。调用方可能已给定其中之一。
void resolveBrokerFromRoute(const TopicRouteData& route, const std::string& topic,
                            std::string& brokerName, std::string& addr) {
    if (brokerName.empty()) {
        if (route.brokerDatas.empty()) {
            throw MQClientException("No broker in route of topic: " + topic);
        }
        brokerName = route.brokerDatas.front().brokerName;
    }
    if (addr.empty()) {
        addr = MQClientInstance::findBrokerAddrInRoute(route, brokerName);
        if (addr.empty() && !route.brokerDatas.empty()) {
            addr = route.brokerDatas.front().selectBrokerAddr();
        }
        if (addr.empty()) {
            throw MQClientException("No available broker addr for topic: " + topic);
        }
    }
}

}  // namespace

// 给 POP 出来的消息反构 POP_CK 与 1ST_POP_TIME。
//
// **这是 POP 最容易踩的坑**：普通 topic 直连 POP 时 broker **不写** POP_CK 属性
// （只在 retry topic 重编码路径才写），必须由客户端用响应头的 startOffsetInfo /
// msgOffsetInfo 反构 —— 没有它就无法发 ACK。逐条对齐 Java
// MQClientAPIImpl.processPopResponse。
//
// 注意调用时机：必须在**改写消息 topic 之前**调用，因为 retryFlag 是从消息的
// 原始 topic 推出来的（broker 可能改写 topic）。
void MQClientInstance::stampPopCk(std::vector<MessageExt>& msgs, const std::string& brokerName,
                                  const PopMessageResponseHeader& respHeader) {
    using namespace extra_info;

    const int64_t popTime = respHeader.popTime.value_or(0);
    const int64_t invisibleTime = respHeader.invisibleTime.value_or(0);
    const int32_t reviveQid = respHeader.reviveQid.value_or(0);
    const std::string startOffsetInfo = respHeader.startOffsetInfo.value_or("");
    const std::string msgOffsetInfo = respHeader.msgOffsetInfo.value_or("");

    // Java 的查表 key：消息已带 POP_CK（= retry 消息）时 retryFlag 取自 POP_CK 第 5 段，
    // 否则由消息 topic 推断。
    auto queueMapKey = [](const MessageExt& m) -> std::string {
        auto it = m.properties.find(MessageConst::PROPERTY_POP_CK);
        if (it != m.properties.end() && !it->second.empty()) {
            return getRetry(split(it->second)) + "@" + std::to_string(m.queueId);
        }
        return getStartOffsetInfoMapKey(m.topic, m.queueId);
    };

    if (startOffsetInfo.empty()) {
        // Java 的 startOffsetInfo == null 分支：用消息自身 queueOffset 当 ckQueueOffset
        // 拼 7 段，再手工补一段凑成 8 段。
        std::map<std::string, std::string> perQueue;
        for (MessageExt& m : msgs) {
            std::string key = m.topic + std::to_string(m.queueId);
            auto it = perQueue.find(key);
            if (it == perQueue.end()) {
                std::string built = buildExtraInfo(m.queueOffset, popTime, invisibleTime, reviveQid,
                                                   m.topic, brokerName, m.queueId);
                it = perQueue.emplace(key, built).first;
            }
            m.properties[MessageConst::PROPERTY_POP_CK] =
                it->second + kKeySeparator + std::to_string(m.queueOffset);
        }
    } else {
        auto startMap = parseStartOffsetInfo(startOffsetInfo);
        auto msgMap = parseMsgOffsetInfo(msgOffsetInfo);

        // Java 先按队列收集 queueOffset 并排序，再用 indexOf 求下标去取
        // msgOffsetInfo 里对应的 msgQueueOffset。
        std::map<std::string, std::vector<int64_t>> sortedOffsets;
        for (const MessageExt& m : msgs) {
            sortedOffsets[queueMapKey(m)].push_back(m.queueOffset);
        }
        for (auto& kv : sortedOffsets) {
            std::sort(kv.second.begin(), kv.second.end());
        }

        for (MessageExt& m : msgs) {
            // retry topic 弹回来的消息 broker 已经写好 POP_CK，不能覆盖。
            if (m.properties.find(MessageConst::PROPERTY_POP_CK) != m.properties.end()) {
                continue;
            }
            const std::string key = queueMapKey(m);
            if (startMap == std::nullopt || msgMap == std::nullopt) {
                continue;
            }
            auto startIt = startMap->find(key);
            auto offIt = msgMap->find(key);
            if (startIt == startMap->end() || offIt == msgMap->end()) {
                continue;
            }
            auto sortedIt = sortedOffsets.find(key);
            if (sortedIt == sortedOffsets.end()) {
                continue;
            }
            const std::vector<int64_t>& ordered = sortedIt->second;
            auto pos = std::find(ordered.begin(), ordered.end(), m.queueOffset);
            if (pos == ordered.end()) {
                continue;
            }
            std::size_t index = static_cast<std::size_t>(pos - ordered.begin());
            if (index >= offIt->second.size()) {
                continue;
            }
            m.properties[MessageConst::PROPERTY_POP_CK] =
                buildExtraInfo(startIt->second, popTime, invisibleTime, reviveQid, m.topic,
                               brokerName, m.queueId, offIt->second[index]);
        }
    }

    // Java 用 computeIfAbsent：只在缺失时补。
    for (MessageExt& m : msgs) {
        if (m.properties.find(MessageConst::PROPERTY_FIRST_POP_TIME) == m.properties.end()) {
            m.properties[MessageConst::PROPERTY_FIRST_POP_TIME] = std::to_string(popTime);
        }
    }
}

PopResult MQClientInstance::popMessage(const std::string& consumerGroup, const std::string& topic,
                                       int32_t queueId, int32_t maxMsgNums, int64_t invisibleTime,
                                       int64_t pollTime, int32_t initMode,
                                       const std::string& expression,
                                       const std::string& expressionType, bool order,
                                       int32_t timeoutMillis, const std::string& brokerNameIn,
                                       const std::string& addrIn) {
    std::string brokerName = brokerNameIn;
    std::string addr = addrIn;
    if (brokerName.empty() || addr.empty()) {
        auto route = getTopicRouteData(topic);
        if (route == nullptr) {
            throw MQClientNoRouteException(topic);
        }
        resolveBrokerFromRoute(*route, topic, brokerName, addr);
    }

    auto header = std::make_shared<PopMessageRequestHeader>();
    header->consumerGroup = consumerGroup;
    header->topic = topic;
    header->queueId = queueId;
    header->maxMsgNums = maxMsgNums;
    header->invisibleTime = invisibleTime;
    header->pollTime = pollTime;
    // bornTime 填 0 会让 broker 判定"超时太久"直接回 POLLING_TIMEOUT(210)
    header->bornTime = UtilAll::currentTimeMillis();
    header->initMode = initMode;
    if (!expression.empty()) {
        header->exp = expression;
    }
    if (!expressionType.empty()) {
        header->expType = expressionType;
    }
    header->order = order;

    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::POP_MESSAGE, header);
    RemotingCommand response = invokeSyncOnAddr(addr, request, timeoutMillis);

    PopResult result;
    if (response.code == ResponseCode::SUCCESS) {
        result.status = PopStatus::FOUND;
    } else if (response.code == ResponseCode::POLLING_FULL) {
        result.status = PopStatus::POLLING_FULL;
    } else if (response.code == ResponseCode::POLLING_TIMEOUT) {
        result.status = PopStatus::POLLING_NOT_FOUND;
    } else if (response.code == ResponseCode::PULL_NOT_FOUND) {
        result.status = PopStatus::POLLING_NOT_FOUND;
    } else {
        throw MQBrokerException(response.code, response.remark);
    }

    PopMessageResponseHeader respHeader;
    respHeader.fromExtFields(response.extFields);
    result.restNum = respHeader.restNum.value_or(0);
    result.popTime = respHeader.popTime.value_or(0);
    result.invisibleTime = respHeader.invisibleTime.value_or(0);
    result.reviveQid = respHeader.reviveQid.value_or(0);
    result.startOffsetInfo = respHeader.startOffsetInfo.value_or("");
    result.msgOffsetInfo = respHeader.msgOffsetInfo.value_or("");
    result.orderCountInfo = respHeader.orderCountInfo.value_or("");

    if (result.status == PopStatus::FOUND && !response.body.empty()) {
        result.msgFoundList = decodeMessages(response.body);
        stampPopCk(result.msgFoundList, brokerName, respHeader);
    }
    // Java processPopResponse 收尾：统一盖 brokerName，并把 topic 还原成请求的 topic
    for (MessageExt& m : result.msgFoundList) {
        m.brokerName = brokerName;
        m.topic = topic;
    }
    return result;
}

int32_t MQClientInstance::ackMessage(const std::string& consumerGroup, const std::string& topic,
                                     int32_t queueId, const std::string& extraInfo, int64_t offset,
                                     int32_t timeoutMillis, const std::string& brokerNameIn,
                                     const std::string& addrIn) {
    std::string brokerName = brokerNameIn;
    if (brokerName.empty() && !extraInfo.empty()) {
        // 与 Java 一致：从 CK 串第 6 段取 brokerName（ACK 是靠它找地址的）
        brokerName = extra_info::getBrokerName(extra_info::split(extraInfo));
    }
    std::string addr = addrIn;
    if (addr.empty()) {
        auto route = getTopicRouteData(topic);
        if (route == nullptr) {
            throw MQClientNoRouteException(topic);
        }
        resolveBrokerFromRoute(*route, topic, brokerName, addr);
    }

    auto header = std::make_shared<AckMessageRequestHeader>();
    header->consumerGroup = consumerGroup;
    header->topic = topic;
    header->queueId = queueId;
    header->extraInfo = extraInfo;
    header->offset = offset;

    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::ACK_MESSAGE, header);
    RemotingCommand response = invokeSyncOnAddr(addr, request, timeoutMillis);
    return response.code;
}

ChangeInvisibleTimeResult MQClientInstance::changeInvisibleTime(
    const std::string& consumerGroup, const std::string& topic, int32_t queueId,
    const std::string& extraInfo, int64_t offset, int64_t invisibleTime, int32_t timeoutMillis,
    const std::string& brokerNameIn, const std::string& addrIn) {
    std::string brokerName = brokerNameIn;
    if (brokerName.empty() && !extraInfo.empty()) {
        brokerName = extra_info::getBrokerName(extra_info::split(extraInfo));
    }
    std::string addr = addrIn;
    if (addr.empty()) {
        auto route = getTopicRouteData(topic);
        if (route == nullptr) {
            throw MQClientNoRouteException(topic);
        }
        resolveBrokerFromRoute(*route, topic, brokerName, addr);
    }

    auto header = std::make_shared<ChangeInvisibleTimeRequestHeader>();
    header->consumerGroup = consumerGroup;
    header->topic = topic;
    header->queueId = queueId;
    header->extraInfo = extraInfo;
    header->offset = offset;
    header->invisibleTime = invisibleTime;

    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::CHANGE_MESSAGE_INVISIBLETIME, header);
    RemotingCommand response = invokeSyncOnAddr(addr, request, timeoutMillis);

    ChangeInvisibleTimeResponseHeader respHeader;
    respHeader.fromExtFields(response.extFields);

    ChangeInvisibleTimeResult result;
    result.responseCode = response.code;
    result.popTime = respHeader.popTime.value_or(0);
    result.invisibleTime = respHeader.invisibleTime.value_or(0);
    result.reviveQid = respHeader.reviveQid.value_or(0);
    if (response.code == ResponseCode::SUCCESS) {
        // 与 Java MQClientAPIImpl.changeInvisibleTimeAsync 一致：用**响应里的**新值
        // 重建 8 段 extraInfo，供后续 ACK 使用。
        result.extraInfo = extra_info::buildExtraInfo(offset, result.popTime, result.invisibleTime,
                                                      result.reviveQid, topic, brokerName, queueId,
                                                      offset);
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

// ---------------------------------------------------------------- 队列锁（顺序消费）
namespace {

// LockBatchRequestBody：{"consumerGroup":..,"clientId":..,"mqSet":[{"topic":..,
// "brokerName":..,"queueId":..}]}（对齐 Java LockBatchRequestBody）
JsonValue buildMqSetJson(const std::vector<MessageQueue>& mqs) {
    JsonValue arr = JsonValue::makeArray();
    for (const MessageQueue& mq : mqs) {
        JsonValue o = JsonValue::makeObject();
        o.set("topic", JsonValue::makeString(mq.topic));
        o.set("brokerName", JsonValue::makeString(mq.brokerName));
        o.set("queueId", JsonValue::makeInt(mq.queueId));
        arr.pushArray(o);
    }
    return arr;
}

}  // namespace

std::vector<MessageQueue> MQClientInstance::lockBatchMq(const std::string& consumerGroup,
                                                       const std::string& clientId,
                                                       const std::vector<MessageQueue>& mqs,
                                                       int32_t timeoutMillis) {
    std::vector<MessageQueue> lockOk;
    // 按 broker 分组（Java 按 brokerName 逐个发请求）
    std::map<std::string, std::vector<MessageQueue>> byBroker;
    for (const MessageQueue& mq : mqs) {
        byBroker[mq.brokerName].push_back(mq);
    }
    for (const auto& kv : byBroker) {
        std::string addr = brokerAddrOf(kv.first);
        if (addr.empty()) {
            continue;
        }
        JsonValue body = JsonValue::makeObject();
        body.set("consumerGroup", JsonValue::makeString(consumerGroup));
        body.set("clientId", JsonValue::makeString(clientId));
        body.set("mqSet", buildMqSetJson(kv.second));
        std::string bodyText = body.dump();
        Bytes payload(bodyText.begin(), bodyText.end());
        RemotingCommand response =
            invokeSyncRaw(addr, RequestCode::LOCK_BATCH_MQ, PropertyMap{}, payload,
                          /*hasBody=*/true, timeoutMillis);
        checkResponseCode(response);
        // LockBatchResponseBody：{"lockOKMQSet":[{topic,brokerName,queueId}]}
        std::string text(response.body.begin(), response.body.end());
        JsonValue root;
        std::string err;
        if (!jsonParse(text, root, &err)) {
            logger_warn("lockBatchMq: parse response failed: " + err);
            continue;
        }
        const JsonValue* okSet = root.find("lockOKMQSet");
        if (okSet == nullptr || !okSet->isArray()) {
            continue;
        }
        for (size_t i = 0; i < okSet->size(); ++i) {
            const JsonValue& o = okSet->at(i);
            const JsonValue* t = o.find("topic");
            const JsonValue* b = o.find("brokerName");
            const JsonValue* q = o.find("queueId");
            if (t == nullptr || b == nullptr || q == nullptr) continue;
            lockOk.emplace_back(t->stringValue(), b->stringValue(),
                                static_cast<int32_t>(q->intValue()));
        }
    }
    return lockOk;
}

void MQClientInstance::unlockBatchMq(const std::string& consumerGroup,
                                     const std::string& clientId,
                                     const std::vector<MessageQueue>& mqs,
                                     int32_t timeoutMillis) {
    std::map<std::string, std::vector<MessageQueue>> byBroker;
    for (const MessageQueue& mq : mqs) {
        byBroker[mq.brokerName].push_back(mq);
    }
    for (const auto& kv : byBroker) {
        std::string addr = brokerAddrOf(kv.first);
        if (addr.empty()) {
            continue;
        }
        JsonValue body = JsonValue::makeObject();
        body.set("consumerGroup", JsonValue::makeString(consumerGroup));
        body.set("clientId", JsonValue::makeString(clientId));
        body.set("mqSet", buildMqSetJson(kv.second));
        std::string bodyText = body.dump();
        Bytes payload(bodyText.begin(), bodyText.end());
        RemotingCommand response =
            invokeSyncRaw(addr, RequestCode::UNLOCK_BATCH_MQ, PropertyMap{}, payload,
                          /*hasBody=*/true, timeoutMillis);
        checkResponseCode(response);
    }
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

std::vector<std::string> MQClientInstance::getConsumerIdListByGroup(
    const std::string& topic, const std::string& consumerGroup, int32_t timeoutMillis) {
    // 取该 topic 路由里的 master broker 发 GET_CONSUMER_LIST_BY_GROUP(38)：
    // 所有客户端都会向集群内每台 broker 心跳注册，故任取一台即持有完整消费者列表。
    // 查不到（无路由 / 非 SUCCESS / 异常）返回空 vector；调用方按 Java 语义「保留当前分配」，
    // 不要回退成"自己独占全部队列"（那会让多实例互相重复消费）。
    std::string addr;
    {
        auto route = getTopicRouteData(topic);
        if (route == nullptr || route->brokerDatas.empty()) {
            return {};
        }
        addr = route->brokerDatas[0].selectBrokerAddr();
    }
    if (addr.empty()) {
        return {};
    }
    try {
        GetConsumerListByGroupResponseBody body =
            getConsumerListByGroup(consumerGroup, addr, timeoutMillis);
        return body.consumerIdList;
    } catch (const std::exception& e) {
        logger_debug("getConsumerIdListByGroup failed, topic=" + topic + " group=" + consumerGroup
                     + ": " + e.what());
        return {};
    }
}

void MQClientInstance::unregisterClientAllBrokers(const std::string& clientId,
                                                 const std::string& producerGroup,
                                                 const std::string& consumerGroup,
                                                 int32_t timeoutMillis) {
    // 向所有已知 broker 注销本 clientId（对应 Java MQClientInstance.unregisterClient）：
    // 生产者/消费者 shutdown 时逐台 broker 发 UNREGISTER_CLIENT(35)。不发的话 broker 端
    // Consumer/ProducerManager 只能等心跳超时（默认 ~120s）清理，期间事务回查、消费者
    // 变更通知仍可能发往已退出的实例。单台失败只记 debug——shutdown 路径不应因网络抖动抛异常。
    for (const std::string& addr : getRouteOfAllBrokers()) {
        try {
            unregisterClient(addr, clientId, producerGroup, consumerGroup, timeoutMillis);
        } catch (const std::exception& e) {
            logger_debug("unregisterClient failed, addr=" + addr + ": " + e.what());
        }
    }
}

// ---------------------------------------------------------------- 工具
std::string MQClientInstance::brokerAddrForMq(const MessageQueue& mq) { return brokerAddr(mq); }

}  // namespace rocketmq
