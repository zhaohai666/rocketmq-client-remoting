// MQClientInstance implementation: instance-level routing, send, pull, POP, offset and heartbeat plumbing.
#include "rocketmq/client/mq_client.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/request_reply.h"
#include "rocketmq/client/static_topic.h"
#include "rocketmq/client/trace.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"
#include "schedule_util.h"
#include "rocketmq/remoting/exception.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/extra_info.h"
#include "rocketmq/remoting/protocol/headers.h"
#include "rocketmq/remoting/protocol/json.h"

namespace rocketmq {

namespace {

// Pull response code -> PullStatus (the classic remoting protocol meaning of the PULL_* codes)
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

// Send request code: reply messages first, then batches. A reply must use 325 (the broker registers
// ReplyMessageProcessor only for 324/325); a batch uses SEND_BATCH_MESSAGE(320), decided by "is this
// message a batch" (`msg.isBatch`).
// The server handles 310/320 through the same path (the broker picks the write mode from header.batch,
// and proxy and auth list both codes in the same case), so this is not a bug fix: it only separates batch
// from single messages at the request-code layer so server-side per-code metrics and throttling do not count a batch as one message.
int32_t sendRequestCode(const Message& msg) {
    if (isReplyMessage(msg)) {
        return RequestCode::SEND_REPLY_MESSAGE_V2;
    }
    return msg.isBatch ? RequestCode::SEND_BATCH_MESSAGE : RequestCode::SEND_MESSAGE_V2;
}

}  // namespace

// ---------------------------------------------------------------- clientId
namespace {

// Monotonic nanosecond clock: the origin is arbitrary, it only guarantees uniqueness within the process (used to build the clientId).
uint64_t nanoTime() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

bool isBlank(const std::string& s) {
    for (char c : s) {
        if (!std::isspace(static_cast<unsigned char>(c))) return false;
    }
    return true;
}

}  // namespace

std::string changeInstanceNameToPID(const std::string& instanceName) {
    if (instanceName == MixAll::DEFAULT_INSTANCE_NAME) {
        return std::to_string(UtilAll::pid()) + "#" + std::to_string(nanoTime());
    }
    return instanceName;
}

std::string buildMqClientId(const std::string& clientIp, const std::string& instanceName,
                            const std::string& unitName, bool enableStreamRequestType) {
    // The clientId concatenation format required by the classic remoting protocol:
    //   clientIP + "@" + instanceName
    //   append "@" + unitName when unitName is not blank
    //   append "@" + "STREAM" when enableStreamRequestType is set
    // The trailing segment is the **request type name** ("STREAM"), not the code; the code only appears in
    // extFields ReqT.
    std::string id = clientIp + "@" + instanceName;
    if (!isBlank(unitName)) {
        id += "@" + unitName;
    }
    if (enableStreamRequestType) {
        id += "@" + std::string(MixAll::STREAM_REQUEST_TYPE);
    }
    return id;
}

std::string buildClientId(const std::string& instanceName, const std::string& unitName,
                          bool enableStreamRequestType) {
    return buildMqClientId(MixAll::cachedIpStr(), instanceName, unitName, enableStreamRequestType);
}

// ---------------------------------------------------------------- TopicPublishInfo
MessageQueue TopicPublishInfo::selectOneMessageQueue() {
    if (msgQueueList.empty()) {
        throw MQClientException("no message queue for publish info");
    }
    // The cursor uses an atomic increment: publishing may be called concurrently from several threads and the cursor is shared across calls
    uint64_t idx = index_.fetch_add(1);
    return msgQueueList[static_cast<size_t>(idx % msgQueueList.size())];
}

MessageQueue TopicPublishInfo::selectOneMessageQueue(const std::string& lastBrokerName) {
    if (msgQueueList.empty()) {
        throw MQClientException("no message queue for publish info");
    }
    // On retry, avoid the previously failed broker when possible; if every queue is the same broker, fall back to round-robin
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
    // Filtered selection: the cursor still advances,
    // and nullopt is returned when nothing in one full round matches (the caller picks the fallback; no fallback here).
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

// ---------------------------------------------------------------- Lifecycle
bool MQClientInstance::tlsEnabledFromEnv() {
    const char* v = std::getenv("ROCKETMQ_TLS_ENABLE");
    if (v == nullptr) return false;
    std::string s(v);
    // Simple normalisation: 1/true/yes (case-insensitive)
    for (auto& c : s) c = static_cast<char>(::tolower(static_cast<unsigned char>(c)));
    return s == "1" || s == "true" || s == "yes";
}

MQClientInstance::MQClientInstance(const std::string& clientId,
                                  const std::vector<std::string>& nameServerAddrs,
                                  int32_t connectTimeoutMillis, int32_t invokeTimeoutMillis,
                                  bool tlsEnable, const std::string& unitName,
                                  int32_t pollNameServerIntervalMillis)
    : clientId_(clientId), nameServerAddrs_(nameServerAddrs),
      remotingClient_(new RemotingClient(connectTimeoutMillis, invokeTimeoutMillis)),
      pollNameServerIntervalMillis_(pollNameServerIntervalMillis > 0 ? pollNameServerIntervalMillis
                                                                    : 30000),
      tlsEnable_(tlsEnable) {
    topAddressing_.configureFromEnv(unitName);
    remotingClient_->registerProcessor(
        RequestCode::PUSH_REPLY_MESSAGE_TO_CLIENT,
        [](const RemotingCommand& cmd, const std::string& addr) {
            return processReplyMessage(cmd, addr);
        });
    remotingClient_->registerProcessor(
        RequestCode::NOTIFY_CONSUMER_IDS_CHANGED,
        [this](const RemotingCommand& cmd, const std::string& addr) {
            this->processNotifyConsumerIdsChanged(cmd, addr);
            return std::optional<RemotingCommand>();
        });
}

MQClientInstance::~MQClientInstance() { shutdown(); }

void MQClientInstance::start() {
    started_ = true;
    // TLS: must be enabled before the first connection is established (every later connection performs the handshake)
    if (tlsEnable_) {
        remotingClient_->setTlsEnable(true);
    }
    consumerStats_.start();
    if (nameServerAddrs_.empty() && !topAddressing_.wsAddr().empty()) {
        fetchNameServerAddr();
        if (nameServerAddrs_.empty()) {
            throw MQClientException("name server address is not set and address server ("
                                    + topAddressing_.wsAddr() + ") returned none",
                                    ClientErrorCode::NO_NAME_SERVER_EXCEPTION);
        }
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
    auto next = std::chrono::steady_clock::now() + std::chrono::milliseconds(10);
    while (!routeRefreshStop_.load()) {
        if (!started_) return;
        sleepUntilDeadline(routeRefreshStop_, next);
        next += std::chrono::milliseconds(pollNameServerIntervalMillis_);
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
    consumerStats_.shutdown();
    if (remotingClient_) {
        // The code 40 callback captures this: it must be removed before the instance is destroyed, otherwise a reader thread could call back into a dangling object
        remotingClient_->unregisterProcessor(RequestCode::NOTIFY_CONSUMER_IDS_CHANGED);
        remotingClient_->shutdown();
    }
    {
        std::lock_guard<std::mutex> lk(rebalanceWakeupLock_);
        rebalanceWakeups_.clear();
    }
}

// ---------------------------------------------------------------- 40 NOTIFY_CONSUMER_IDS_CHANGED

void MQClientInstance::registerRebalanceWakeup(const std::string& group,
                                               std::function<void()> wakeup) {
    std::lock_guard<std::mutex> lk(rebalanceWakeupLock_);
    rebalanceWakeups_[group] = std::move(wakeup);
}

void MQClientInstance::unregisterRebalanceWakeup(const std::string& group) {
    std::lock_guard<std::mutex> lk(rebalanceWakeupLock_);
    rebalanceWakeups_.erase(group);
}

void MQClientInstance::rebalanceImmediately() {
    std::map<std::string, std::function<void()>> snapshot;
    {
        std::lock_guard<std::mutex> lk(rebalanceWakeupLock_);
        snapshot = rebalanceWakeups_;
    }
    for (const auto& entry : snapshot) {
        if (!entry.second) continue;
        try {
            entry.second();
        } catch (...) {
        }
    }
}

void MQClientInstance::processNotifyConsumerIdsChanged(const RemotingCommand& cmd,
                                                      const std::string& addr) {
    auto it = cmd.extFields.find("consumerGroup");
    const std::string group = it == cmd.extFields.end() ? std::string() : it->second;
    consumerIdsChangedCount_.fetch_add(1);
    logger_info("receive broker's notification[" + addr + "], the consumer group: " + group +
                " changed, rebalance immediately");
    rebalanceImmediately();
}

void MQClientInstance::fetchNameServerAddr() {
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
    auto next = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!namesrvRefreshStop_.load()) {
        if (!started_) return;
        sleepUntilDeadline(namesrvRefreshStop_, next);
        next += std::chrono::minutes(2);
        try {
            fetchNameServerAddr();
        } catch (const std::exception& e) {
            logger_debug(std::string("fetchNameServerAddr exception: ") + e.what());
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

std::string MQClientInstance::vipAddr(const std::string& addr) const {
    return MixAll::brokerVipChannel(vipChannelEnabled_, addr);
}

void MQClientInstance::checkResponseCode(const RemotingCommand& response) {
    if (response.code != ResponseCode::SUCCESS) {
        throw MQBrokerException(response.code, response.remark);
    }
}

// ---------------------------------------------------------------- Route management
bool MQClientInstance::updateTopicRouteInfoFromNameServer(const std::string& topic,
                                                         int32_t timeoutMillis,
                                                         bool isDefault) {
    if (nameServerAddrs_.empty()) {
        throw MQClientException("name server address list is empty",
                                ClientErrorCode::NO_NAME_SERVER_EXCEPTION);
    }

    // lastRouteError: the last failure reason when every name server is unreachable (TLS handshake CA
    // validation failed, connection refused...). Without surfacing it, getTopicPublishInfo can only report
    // "Can not find Message Queue" as a black box, sending users to look for a missing topic while the real fault is the link/certificate.
    std::string lastRouteError;
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
                // Stop polling as soon as a reachable NS explicitly returns non-SUCCESS (e.g. TOPIC_NOT_EXIST);
                // that is a real answer, not a link fault, so the leftover error string is cleared.
                lastError.clear();
                break;
            } catch (const RemotingException& e) {
                lastError = e.what();
                continue;
            }
        }
        lastRouteError = lastError;
        {
            std::lock_guard<std::mutex> lk(lastRouteErrorMutex_);
            lastRouteError_ = lastRouteError;
        }
        return false;
    };

    TopicRouteData route;
    bool ok = fetch(topic, route);
    if (!ok && isDefault && topic != MixAll::DEFAULT_TOPIC) {
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
    for (const BrokerData& bd : route.brokerDatas) {
        brokerAddrTable_[bd.brokerName] = bd.brokerAddrs;
    }
    {
        std::map<MessageQueue, std::string> endpoints;
        if (static_topic::topicRouteData2EndpointsForStaticTopic(topic, route, endpoints)) {
            topicEndPointsTable_[topic] = std::move(endpoints);
        } else {
            topicEndPointsTable_.erase(topic);
        }
    }
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
        // Include the real route-fetch failure reason (TLS/CA validation failed, connection refused...). With no
        // failure recorded (the NS was reachable and explicitly answered topic-not-found) the original text stays -- that is the "create the topic" meaning.
        std::string msg = "Can not find Message Queue for topic: " + topic;
        {
            std::lock_guard<std::mutex> lk(lastRouteErrorMutex_);
            if (!lastRouteError_.empty()) {
                msg += " (route fetch failed: " + lastRouteError_ + ")";
            }
        }
        throw MQClientException(msg);
    }
    return it->second;
}

std::vector<MessageQueue> MQClientInstance::getTopicSubscribeInfo(const std::string& topic) {
    std::shared_ptr<TopicRouteData> route = getTopicRouteData(topic);
    if (route == nullptr) {
        return {};
    }
    return route->getAllSubscribeMessageQueue(topic);
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
        // A failed route refresh does not throw; the table lookup below returns empty instead
    }
    std::lock_guard<std::recursive_mutex> lk(routeLock_);
    auto it = topicRouteTable_.find(topic);
    if (it == topicRouteTable_.end()) {
        return nullptr;
    }
    return std::make_shared<TopicRouteData>(it->second);
}

bool MQClientInstance::isTopicRouteCached(const std::string& topic) const {
    std::lock_guard<std::recursive_mutex> lk(routeLock_);
    return topicRouteTable_.find(topic) != topicRouteTable_.end();
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

std::pair<std::string, bool> MQClientInstance::findBrokerAddressInSubscribe(
    const std::map<int64_t, std::string>& brokerAddrs, int64_t brokerId, bool onlyThisBroker) {
    const int64_t masterId = MixAll::MASTER_ID;
    if (brokerAddrs.empty()) {
        return {std::string(), false};
    }
    auto hit = brokerAddrs.find(brokerId);
    if (hit != brokerAddrs.end()) {
        return {hit->second, brokerId != masterId};
    }
    if (brokerId != masterId) {
        auto next = brokerAddrs.find(brokerId + 1);
        if (next != brokerAddrs.end()) {
            return {next->second, true};
        }
    }
    if (!onlyThisBroker) {
        const auto& first = *brokerAddrs.begin();
        return {first.second, first.first != masterId};
    }
    return {std::string(), false};
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

std::string MQClientInstance::findBrokerAddressInPublish(const std::string& brokerName) {
    std::lock_guard<std::recursive_mutex> lk(routeLock_);
    auto it = brokerAddrTable_.find(brokerName);
    if (it == brokerAddrTable_.end()) {
        return std::string();
    }
    auto master = it->second.find(MixAll::MASTER_ID);
    if (master == it->second.end()) {
        return std::string();
    }
    return master->second;
}

std::string MQClientInstance::publishAddrFor(const std::string& brokerName,
                                             const std::string& topic) {
    std::string addr = findBrokerAddressInPublish(brokerName);
    if (addr.empty()) {
        updateTopicRouteInfoFromNameServer(topic, 5000, false);
        addr = findBrokerAddressInPublish(brokerName);
    }
    if (addr.empty()) {
        throw MQClientException("The broker[" + brokerName + "] not exist", -1);
    }
    return addr;
}

std::string MQClientInstance::publishAddrInAdmin(const MessageQueue& mq) {
    // Same shape as publishAddrFor; the extra layer only makes
    // offset-query call sites read as "resolve by mq".
    return publishAddrFor(mq.brokerName, mq.topic);
}

std::string MQClientInstance::resolvePopAddr(const std::string& topic, std::string& brokerName,
                                             const std::string& addrIn) {
    if (brokerName.empty()) {
        auto route = getTopicRouteData(topic);
        if (route == nullptr) {
            throw MQClientNoRouteException(topic);
        }
        if (route->brokerDatas.empty()) {
            throw MQClientException("No broker in route of topic: " + topic);
        }
        brokerName = route->brokerDatas.front().brokerName;
    }
    if (!addrIn.empty()) {
        return addrIn;  // The caller named the address: the route is only used to fill in brokerName, the address is untouched
    }
    return publishAddrFor(brokerName, topic);
}

std::string MQClientInstance::getBrokerNameFromMessageQueue(const MessageQueue& mq) {
    std::lock_guard<std::recursive_mutex> lk(routeLock_);
    auto tableIt = topicEndPointsTable_.find(mq.topic);
    if (tableIt != topicEndPointsTable_.end() && !tableIt->second.empty()) {
        auto it = tableIt->second.find(mq);
        if (it != tableIt->second.end()) {
            return it->second;
        }
    }
    return mq.brokerName;
}

std::string MQClientInstance::popAckAddrFor(const std::string& topic,
                                            const std::string& brokerName,
                                            int32_t queueId, const std::string& addrIn) {
    // The caller named the address: return it as is (same rule as resolvePopAddr).
    if (!addrIn.empty()) {
        return addrIn;
    }
    std::string name = brokerName;
    if (name.empty()) {
        // The same convenience rule as resolvePopAddr: pick one broker from the route, and only its master counts.
        auto route = getTopicRouteData(topic);
        if (route == nullptr) {
            throw MQClientNoRouteException(topic);
        }
        if (route->brokerDatas.empty()) {
            throw MQClientException("No broker in route of topic: " + topic);
        }
        name = route->brokerDatas.front().brokerName;
    }
    const auto resolveDes = [this, &topic, &name, queueId]() -> std::string {
        if (static_topic::isLogicalQueueMockBrokerName(name)) {
            return getBrokerNameFromMessageQueue(MessageQueue(topic, name, queueId));
        }
        return name;
    };
    std::string des = resolveDes();
    std::string addr = findBrokerAddressInPublish(des);
    if (addr.empty()) {
        updateTopicRouteInfoFromNameServer(topic, 5000, false);
        des = resolveDes();
        addr = findBrokerAddressInPublish(des);
    }
    if (addr.empty()) {
        throw MQClientException("Broker[" + des + "] master node does not exist", -1);
    }
    return addr;
}

std::string MQClientInstance::consumerOffsetAddr(const MessageQueue& mq) {
    // Differs from publishAddrInAdmin only in the last step: admin-side offset queries always hit the master and
    // fail when it is gone, while offset reading may fall back to a slave.
    std::string addr = findBrokerAddressInPublish(mq.brokerName);
    if (addr.empty()) {
        updateTopicRouteInfoFromNameServer(mq.topic, 5000, false);
        addr = brokerAddrOf(mq.brokerName);
    }
    if (addr.empty()) {
        throw MQClientException("The broker[" + mq.brokerName + "] not exist", -1);
    }
    return addr;
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

std::vector<std::string> MQClientInstance::getAllBrokerAddrs() {
    // The only difference from getRouteOfAllBrokers: no master selection, every brokerId counts as one.
    std::vector<std::string> addrs;
    std::lock_guard<std::recursive_mutex> lk(routeLock_);
    for (const auto& kv : topicRouteTable_) {
        for (const BrokerData& bd : kv.second.brokerDatas) {
            for (const auto& entry : bd.brokerAddrs) {
                if (!entry.second.empty() &&
                    std::find(addrs.begin(), addrs.end(), entry.second) == addrs.end()) {
                    addrs.push_back(entry.second);
                }
            }
        }
    }
    return addrs;
}

namespace {
std::optional<int32_t> propertyAsInt(const Message& msg, const char* key) {
    const std::string value = msg.getProperty(key);
    if (value.empty()) {
        return std::nullopt;
    }
    errno = 0;
    char* end = nullptr;
    const long long parsed = std::strtoll(value.c_str(), &end, 10);
    if (errno != 0 || end == value.c_str() || *end != '\0') {
        return std::nullopt;
    }
    if (parsed < INT32_MIN || parsed > INT32_MAX) {
        return std::nullopt;
    }
    return static_cast<int32_t>(parsed);
}
}  // namespace

RemotingCommand MQClientInstance::buildSendRequest(const std::string& producerGroup,
                                                   const Message& msg, const MessageQueue& mq,
                                                   int32_t sysFlag, bool unitMode,
                                                   const std::optional<std::string>& createTopicKey,
                                                   const std::optional<int32_t>& defaultTopicQueueNums) {
    auto header = std::make_shared<SendMessageRequestHeaderV2>();
    header->producerGroup = producerGroup;
    header->topic = msg.topic;
    header->defaultTopic = (createTopicKey && !createTopicKey->empty())
                               ? createTopicKey
                               : std::optional<std::string>(MixAll::DEFAULT_TOPIC);
    header->defaultTopicQueueNums =
        defaultTopicQueueNums.value_or(MixAll::DEFAULT_TOPIC_QUEUE_NUMS);
    header->queueId = mq.queueId;
    header->sysFlag = sysFlag;
    header->bornTimestamp = UtilAll::currentTimeMillis();
    header->flag = msg.flag;
    header->properties = messagePropertiesToString(msg.properties);
    header->reconsumeTimes = 0;
    header->unitMode = unitMode;
    header->maxReconsumeTimes = std::nullopt;
    if (MixAll::isRetryTopic(msg.topic)) {
        const std::optional<int32_t> reconsumeTimes =
            propertyAsInt(msg, MessageConst::PROPERTY_RECONSUME_TIME);
        if (reconsumeTimes) {
            header->reconsumeTimes = reconsumeTimes;
        }
        header->maxReconsumeTimes = propertyAsInt(msg, MessageConst::PROPERTY_MAX_RECONSUME_TIMES);
    }
    header->batch = msg.isBatch;
    header->brokerName =
        mq.brokerName.empty() ? std::nullopt : std::optional<std::string>(mq.brokerName);

    // Request-code selection (reply / batch / normal) is in sendRequestCode: a reply uses 325 and a batch uses 320,
    // otherwise plain SEND_MESSAGE_V2(310).
    RemotingCommand request = RemotingCommand::createRequestCommand(sendRequestCode(msg), header);
    request.body = msg.body;
    request.hasBody = true;
    return request;
}

SendResult MQClientInstance::parseSendResponse(const RemotingCommand& response, const Message& msg,
                                               const MessageQueue& mq) {
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
    const std::string clientUniqId =
        msg.getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    result.msgId = !clientUniqId.empty()
                       ? clientUniqId
                       : respHeader.batchUniqId.value_or(respHeader.msgId.value_or(""));
    result.offsetMsgId = respHeader.msgId.value_or("");
    result.messageQueue = MessageQueue(mq.topic, mq.brokerName,
                                       respHeader.queueId.value_or(mq.queueId));
    result.queueOffset = respHeader.queueOffset.value_or(0);
    result.transactionId = respHeader.transactionId.value_or("");
    result.recallHandle = respHeader.recallHandle;
    auto regionIt = response.extFields.find(MessageConst::PROPERTY_MSG_REGION);
    std::string region = (regionIt != response.extFields.end()) ? regionIt->second : std::string();
    result.regionId = region.empty() ? std::string(TraceConstants::DEFAULT_TRACE_REGION_ID) : region;
    auto traceIt = response.extFields.find(TraceConstants::PROPERTY_TRACE_SWITCH);
    std::string traceOn =
        (traceIt != response.extFields.end()) ? traceIt->second : std::string();
    result.traceOn = (traceOn != "false");
    return result;
}

SendResult MQClientInstance::sendMessage(const std::string& producerGroup, const Message& msg,
                                        const MessageQueue& mq, int32_t timeoutMillis,
                                        int32_t sysFlag, bool unitMode,
                                        const std::optional<std::string>& createTopicKey,
                                        const std::optional<int32_t>& defaultTopicQueueNums) {
    const std::string addr = publishAddrFor(mq.brokerName, mq.topic);
    RemotingCommand request =
        buildSendRequest(producerGroup, msg, mq, sysFlag, unitMode, createTopicKey,
                         defaultTopicQueueNums);
    RemotingCommand response = invokeSyncOnAddr(vipAddr(addr), request, timeoutMillis);
    return parseSendResponse(response, msg, mq);
}

void MQClientInstance::sendMessageAsync(const std::string& addr, RemotingCommand& request,
                                       const Message& msg, const MessageQueue& mq,
                                       int32_t timeoutMillis,
                                       std::function<void(const SendResult&, const InvokeError&)>
                                           onComplete) {
    remotingClient_->invokeAsync(
        vipAddr(addr), request,
        [onComplete, msg, mq](const RemotingCommand& response, const InvokeError& error) {
            if (!error.empty()) {
                onComplete(SendResult(), error);
                return;
            }
            try {
                onComplete(parseSendResponse(response, msg, mq), InvokeError());
            } catch (const MQBrokerException& e) {
                onComplete(SendResult(), InvokeError(InvokeError::Kind::RESPONSE_FAILED,
                                                     e.what(), e.responseCode));
            } catch (const std::exception& e) {
                onComplete(SendResult(),
                           InvokeError(InvokeError::Kind::RESPONSE_FAILED, e.what()));
            } catch (...) {
                onComplete(SendResult(), InvokeError(InvokeError::Kind::RESPONSE_FAILED,
                                                     "unknown error"));
            }
        },
        timeoutMillis);
}

std::string MQClientInstance::recallMessage(const std::string& addr,
                                            const RecallMessageRequestHeader& header,
                                            int32_t timeoutMillis) {
    RemotingCommand request = RemotingCommand::createRequestCommand(
        RequestCode::RECALL_MESSAGE, std::make_shared<RecallMessageRequestHeader>(header));
    RemotingCommand response = invokeSyncOnAddr(addr, request, timeoutMillis);
    checkResponseCode(response);
    RecallMessageResponseHeader respHeader;
    respHeader.fromExtFields(response.extFields);
    if (!respHeader.msgId.has_value() || respHeader.msgId->empty()) {
        throw MQBrokerException(response.code, "recall message response has no msgId");
    }
    return *respHeader.msgId;
}

void MQClientInstance::sendMessageOneway(const std::string& producerGroup, const Message& msg,
                                        const MessageQueue& mq, int32_t timeoutMillis,
                                        int32_t sysFlag, bool unitMode,
                                        const std::optional<std::string>& createTopicKey,
                                        const std::optional<int32_t>& defaultTopicQueueNums) {
    const std::string addr = publishAddrFor(mq.brokerName, mq.topic);
    (void)timeoutMillis;

    // Request-code selection (reply / batch / normal) is in sendRequestCode; the header-building details are shared with the sync/async paths
    RemotingCommand request =
        buildSendRequest(producerGroup, msg, mq, sysFlag, unitMode, createTopicKey,
                         defaultTopicQueueNums);
    request.markOnewayRpc();
    remotingClient_->invokeOneway(vipAddr(addr), request);
}

// ---------------------------------------------------------------- Message pull
PullResult MQClientInstance::pullMessage(const std::string& consumerGroup, const MessageQueue& mq,
                                        int64_t queueOffset, int32_t maxMsgNums, int32_t sysFlag,
                                        int64_t commitOffset, const std::string& subscription,
                                        int64_t subVersion, const std::string& expressionType,
                                        int32_t timeoutMillis, int32_t maxMsgBytes,
                                        int32_t suspendTimeoutMillis, const std::string& addrIn,
                                        int32_t requestSource,
                                        std::optional<int64_t> brokerId) {
    std::string addr = addrIn;
    if (addr.empty()) {
        if (!brokerId.has_value()) {
            addr = brokerAddr(mq);
        } else {
            auto route = getTopicRouteData(mq.topic);
            if (route == nullptr) {
                throw MQClientNoRouteException(mq.topic);
            }
            const BrokerData* brokerData = nullptr;
            for (const BrokerData& bd : route->brokerDatas) {
                if (bd.brokerName == mq.brokerName) {
                    brokerData = &bd;
                    break;
                }
            }
            if (brokerData == nullptr) {
                throw MQClientException("Broker " + mq.brokerName + " not exist");
            }
            auto [chosen, slave] =
                findBrokerAddressInSubscribe(brokerData->brokerAddrs, *brokerId);
            if (chosen.empty()) {
                throw MQClientException("Broker " + mq.brokerName + " not exist");
            }
            addr = chosen;
            if (slave) {
                sysFlag = PullSysFlag::clearCommitOffsetFlag(sysFlag);
            }
        }
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
    header->subscription =
        PullSysFlag::hasSubscriptionFlag(sysFlag) ? std::optional<std::string>(subscription)
                                                  : std::nullopt;
    header->subVersion = subVersion;
    header->expressionType = expressionType;
    header->maxMsgBytes = maxMsgBytes;
    header->requestSource = requestSource;

    const int32_t code = PullSysFlag::hasLitePullFlag(sysFlag) ? RequestCode::LITE_PULL_MESSAGE
                                                               : RequestCode::PULL_MESSAGE;
    RemotingCommand request = RemotingCommand::createRequestCommand(code, header);
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
    result.suggestWhichBrokerId = respHeader.suggestWhichBrokerId;
    if (!response.body.empty()) {
        result.msgFoundList = decodeMessages(response.body);
        for (MessageExt& m : result.msgFoundList) {
            m.brokerName = mq.brokerName;
            m.queueId = mq.queueId;
        }
    }
    return result;
}

// ---------------------------------------------------------------- POP mode

// Reconstruct POP_CK and 1ST_POP_TIME for POP-ed messages.
//
//
// Call-site requirement: it must run **before the message topic is rewritten**, because retryFlag is
// derived from the message's original topic (the broker may rewrite the topic).
void MQClientInstance::stampPopCk(std::vector<MessageExt>& msgs, const std::string& brokerName,
                                  const PopMessageResponseHeader& respHeader) {
    using namespace extra_info;

    const int64_t popTime = respHeader.popTime.value_or(0);
    const int64_t invisibleTime = respHeader.invisibleTime.value_or(0);
    const int32_t reviveQid = respHeader.reviveQid.value_or(0);
    const std::string startOffsetInfo = respHeader.startOffsetInfo.value_or("");
    const std::string msgOffsetInfo = respHeader.msgOffsetInfo.value_or("");

    auto queueMapKey = [](const MessageExt& m) -> std::string {
        auto it = m.properties.find(MessageConst::PROPERTY_POP_CK);
        if (it != m.properties.end() && !it->second.empty()) {
            return getRetry(split(it->second)) + "@" + std::to_string(m.queueId);
        }
        return getStartOffsetInfoMapKey(m.topic, m.queueId);
    };

    if (startOffsetInfo.empty()) {
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

        std::map<std::string, std::vector<int64_t>> sortedOffsets;
        for (const MessageExt& m : msgs) {
            sortedOffsets[queueMapKey(m)].push_back(m.queueOffset);
        }
        for (auto& kv : sortedOffsets) {
            std::sort(kv.second.begin(), kv.second.end());
        }

        for (MessageExt& m : msgs) {
            // Messages returned from a retry topic already carry the POP_CK written by the broker; it must not be overwritten.
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
    const std::string addr = resolvePopAddr(topic, brokerName, addrIn);

    auto header = std::make_shared<PopMessageRequestHeader>();
    header->consumerGroup = consumerGroup;
    header->topic = topic;
    header->queueId = queueId;
    header->maxMsgNums = maxMsgNums;
    header->invisibleTime = invisibleTime;
    header->pollTime = pollTime;
    // A bornTime of 0 makes the broker judge the request as far too old and answer POLLING_TIMEOUT(210) directly
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
        brokerName = extra_info::getBrokerName(extra_info::split(extraInfo));
    }
    const std::string addr = popAckAddrFor(topic, brokerName, queueId, addrIn);

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
    // The mock-name rewrite follows the ack path.
    const std::string addr = popAckAddrFor(topic, brokerName, queueId, addrIn);

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
        result.extraInfo = extra_info::buildExtraInfo(offset, result.popTime, result.invisibleTime,
                                                      result.reviveQid, topic, brokerName, queueId,
                                                      offset);
    }
    return result;
}

// ---------------------------------------------------------------- Consumer offset
bool MQClientInstance::queryConsumerOffset(const std::string& consumerGroup,
                                          const MessageQueue& mq, int64_t& outOffset,
                                          int32_t timeoutMillis, const std::string& addrIn,
                                          bool setZeroIfNotFound) {
    std::string addr = addrIn.empty() ? consumerOffsetAddr(mq) : addrIn;
    auto header = std::make_shared<QueryConsumerOffsetRequestHeader>();
    header->consumerGroup = consumerGroup;
    header->topic = mq.topic;
    header->queueId = mq.queueId;
    auto request = RemotingCommand::createRequestCommand(RequestCode::QUERY_CONSUMER_OFFSET, header);
    if (setZeroIfNotFound) {
        request.extFields["setZeroIfNotFound"] = "true";
    }
    RemotingCommand response = invokeSyncOnAddr(vipAddr(addr), request, timeoutMillis);
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
    RemotingCommand response = invokeSyncOnAddr(vipAddr(addr), request, timeoutMillis);
    checkResponseCode(response);
}

// ---------------------------------------------------------------- Queue locks (sequential consumption)
namespace {

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
    std::map<std::string, std::vector<MessageQueue>> byBroker;
    for (const MessageQueue& mq : mqs) {
        byBroker[mq.brokerName].push_back(mq);
    }
    for (const auto& kv : byBroker) {
        std::string addr = findBrokerAddressInPublish(kv.first);
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
            invokeSyncRaw(vipAddr(addr), RequestCode::LOCK_BATCH_MQ, PropertyMap{}, payload,
                          /*hasBody=*/true, timeoutMillis);
        checkResponseCode(response);
        // LockBatchResponseBody: {"lockOKMQSet":[{topic,brokerName,queueId}]}
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
        std::string addr = findBrokerAddressInPublish(kv.first);
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
            invokeSyncRaw(vipAddr(addr), RequestCode::UNLOCK_BATCH_MQ, PropertyMap{}, payload,
                          /*hasBody=*/true, timeoutMillis);
        checkResponseCode(response);
    }
}

int64_t MQClientInstance::getMaxOffset(const MessageQueue& mq, int32_t timeoutMillis,
                                      const std::string& addrIn) {
    std::string addr = addrIn.empty() ? publishAddrInAdmin(mq) : addrIn;
    auto header = std::make_shared<GetMaxOffsetRequestHeader>();
    header->topic = mq.topic;
    header->queueId = mq.queueId;
    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::GET_MAX_OFFSET, header);
    RemotingCommand response = invokeSyncOnAddr(vipAddr(addr), request, timeoutMillis);
    checkResponseCode(response);
    GetMaxOffsetResponseHeader respHeader;
    respHeader.fromExtFields(response.extFields);
    return respHeader.offset.value_or(0);
}

int64_t MQClientInstance::getMinOffset(const MessageQueue& mq, int32_t timeoutMillis,
                                      const std::string& addrIn) {
    std::string addr = addrIn.empty() ? publishAddrInAdmin(mq) : addrIn;
    auto header = std::make_shared<GetMinOffsetRequestHeader>();
    header->topic = mq.topic;
    header->queueId = mq.queueId;
    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::GET_MIN_OFFSET, header);
    RemotingCommand response = invokeSyncOnAddr(vipAddr(addr), request, timeoutMillis);
    checkResponseCode(response);
    GetMinOffsetResponseHeader respHeader;
    respHeader.fromExtFields(response.extFields);
    return respHeader.offset.value_or(0);
}

int64_t MQClientInstance::searchOffsetByTimestamp(const MessageQueue& mq, int64_t timestamp,
                                                 int32_t timeoutMillis,
                                                 const std::string& addrIn) {
    return searchOffsetByBoundary(mq, timestamp, BoundaryType::LOWER, timeoutMillis, addrIn);
}

int64_t MQClientInstance::searchOffsetByBoundary(const MessageQueue& mq, int64_t timestamp,
                                                const std::optional<BoundaryType>& boundaryType,
                                                int32_t timeoutMillis,
                                                const std::string& addrIn) {
    std::string addr = addrIn.empty() ? publishAddrInAdmin(mq) : addrIn;
    auto header = std::make_shared<SearchOffsetRequestHeader>();
    header->topic = mq.topic;
    header->queueId = mq.queueId;
    header->timestamp = timestamp;
    header->boundaryType = boundaryType;
    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::SEARCH_OFFSET_BY_TIMESTAMP, header);
    RemotingCommand response = invokeSyncOnAddr(vipAddr(addr), request, timeoutMillis);
    checkResponseCode(response);
    SearchOffsetResponseHeader respHeader;
    respHeader.fromExtFields(response.extFields);
    return respHeader.offset.value_or(0);
}

// ---------------------------------------------------------------- Query by key
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
        request.extFields[MixAll::UNIQUE_MSG_QUERY_FLAG] = "true";
    }
    RemotingCommand response = invokeSyncOnAddr(vipAddr(addr), request, timeoutMillis);
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
                    // KEYS is space separated (MessageConst::KEY_SEPARATOR)
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

// ---------------------------------------------------------------- Heartbeat / unregister
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
    header->producerGroup = UtilAll::isBlank(producerGroup)
        ? std::nullopt
        : std::optional<std::string>(producerGroup);
    header->consumerGroup = UtilAll::isBlank(consumerGroup)
        ? std::nullopt
        : std::optional<std::string>(consumerGroup);
    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::UNREGISTER_CLIENT, header);
    RemotingCommand response = invokeSyncOnAddr(addr, request, timeoutMillis);
    checkResponseCode(response);
}

// ---------------------------------------------------------------- Generic synchronous invocation
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

// ---------------------------------------------------------------- Administrative calls
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
    header->topicFilterType = topicFilterType;
    header->topicSysFlag = topicSysFlag;
    header->order = false;
    header->attributes = attributes;
    header->force = force;
    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::UPDATE_AND_CREATE_TOPIC, header);

    std::string lastError;
    int attempts = retryTimes > 0 ? retryTimes : 1;
    for (int i = 0; i < attempts; ++i) {
        try {
            RemotingCommand response = invokeSyncOnAddr(vipAddr(brokerAddr), request, timeoutMillis);
            checkResponseCode(response);
            return;
        } catch (const MQBrokerException&) {
            // An explicit broker rejection (e.g. TOPIC_EXIST_ALREADY) is not retried and propagates directly
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
    invokeSync(vipAddr(brokerAddr), RequestCode::DELETE_TOPIC_IN_BROKER, ext, Bytes(), false, timeoutMillis);
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

// ---------------------------------------------------------------- Cluster / topic / consumer lists
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
    RemotingCommand response = invokeSyncOnAddr(vipAddr(addr), request, timeoutMillis);
    checkResponseCode(response);
    GetConsumerListByGroupResponseBody out;
    if (!response.body.empty()) {
        GetConsumerListByGroupResponseBody::decode(response.body, out);
    }
    return out;
}

std::vector<std::string> MQClientInstance::getConsumerIdListByGroup(
    const std::string& topic, const std::string& consumerGroup, int32_t timeoutMillis) {
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
    for (const std::string& addr : getAllBrokerAddrs()) {
        try {
            unregisterClient(addr, clientId, producerGroup, consumerGroup, timeoutMillis);
        } catch (const std::exception& e) {
            logger_debug("unregisterClient failed, addr=" + addr + ": " + e.what());
        }
    }
}

// ---------------------------------------------------------------- CHECK_CLIENT_CONFIG(46)
std::string MQClientInstance::findBrokerAddrByTopic(const std::string& topic) {
    std::shared_ptr<TopicRouteData> route;
    {
        std::lock_guard<std::recursive_mutex> lk(routeLock_);
        auto it = topicRouteTable_.find(topic);
        if (it == topicRouteTable_.end()) return std::string();
        route = std::make_shared<TopicRouteData>(it->second);
    }
    const std::vector<BrokerData>& brokers = route->getBrokerDatas();
    if (brokers.empty()) return std::string();
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    std::uniform_int_distribution<size_t> dist(0, brokers.size() - 1);
    return brokers[dist(rng)].selectBrokerAddr();
}

void MQClientInstance::checkClientConfig(const std::string& brokerAddr,
                                         const std::string& consumerGroup,
                                         const std::string& clientId,
                                         const SubscriptionData& subscriptionData,
                                         int32_t timeoutMillis) {
    RemotingCommand request =
        RemotingCommand::createRequestCommand(RequestCode::CHECK_CLIENT_CONFIG, nullptr);
    CheckClientRequestBody body;
    body.clientId = clientId;
    body.group = consumerGroup;
    body.subscriptionData = subscriptionData;
    request.body = body.encode();
    request.hasBody = true;
    RemotingCommand response = invokeSyncOnAddr(vipAddr(brokerAddr), request, timeoutMillis);
    if (response.code != ResponseCode::SUCCESS) {
        throw MQClientException(response.remark, response.code);
    }
}

void MQClientInstance::checkSubscriptionsInBroker(const std::string& group,
                                                  const std::vector<SubscriptionData>& subs) {
    for (const SubscriptionData& sub : subs) {
        if (sub.expressionType.empty() || sub.expressionType == ExpressionType::TAG) continue;
        const std::string addr = findBrokerAddrByTopic(sub.topic);
        if (addr.empty()) continue;  // No route found -> skip, do not raise
        try {
            checkClientConfig(addr, group, clientId_, sub, kMqClientApiTimeoutMillis);
        } catch (const MQClientException&) {
            throw;  // Already carries a broker response code; propagate unchanged
        } catch (const std::exception& e) {
            logger_warn("checkClientConfig failed, addr=" + addr + ": " + e.what());
            throw MQClientException(
                std::string("Check client in broker error, maybe because you use ")
                + sub.expressionType
                + " to filter message, but server has not been upgraded to support!This error "
                  "would not affect the launch of consumer, but may has impact on message "
                  "receiving if you have use the new features which are not supported by server, "
                  "please check the log!");
        }
    }
}

// ---------------------------------------------------------------- Utilities
std::string MQClientInstance::brokerAddrForMq(const MessageQueue& mq) { return brokerAddr(mq); }

}  // namespace rocketmq
