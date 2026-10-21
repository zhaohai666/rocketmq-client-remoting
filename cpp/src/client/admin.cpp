// Administration client implementation.
#include "rocketmq/client/admin.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/validators.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/extra_info.h"
#include "rocketmq/remoting/protocol/serialize.h"

namespace rocketmq {

namespace {

std::string i64str(int64_t v) { return std::to_string(v); }

// Read an integer from the response extFields (returns fallback when missing)
int64_t extInt(const RemotingCommand& r, const std::string& key, int64_t fallback) {
    auto it = r.extFields.find(key);
    if (it == r.extFields.end() || it->second.empty()) return fallback;
    try {
        return std::stoll(it->second);
    } catch (...) {
        return fallback;
    }
}

// Equivalent text of a JsonValue (used to compare whether DataVersion changed)
std::string dvText(const JsonValue& v) { return v.isNull() ? std::string("null") : v.dump(); }

template <typename T>
AdminToolResult<T> adminSuccess(T data) {
    AdminToolResult<T> r;
    r.success = true;
    r.code = AdminToolResultCode::SUCCESS;
    r.errorMsg = "success";
    r.data = std::move(data);
    return r;
}

template <typename T>
AdminToolResult<T> adminFailure(int32_t code, std::string msg) {
    AdminToolResult<T> r;
    r.success = false;
    r.code = code;
    r.errorMsg = std::move(msg);
    return r;
}

template <typename T>
AdminToolResult<T> adminFailure(int32_t code, std::string msg, T data) {
    AdminToolResult<T> r;
    r.success = false;
    r.code = code;
    r.errorMsg = std::move(msg);
    r.data = std::move(data);
    return r;
}

}  // namespace

// ---------------------------------------------------------------- Lifecycle
DefaultMQAdminExt::DefaultMQAdminExt(const std::string& instanceName)
    : instanceName_(instanceName) {}

DefaultMQAdminExt::~DefaultMQAdminExt() { shutdown(); }

void DefaultMQAdminExt::setNamesrvAddr(const std::string& addr) {
    nameServerAddrs_.clear();
    size_t start = 0;
    while (start <= addr.size()) {
        size_t semi = addr.find(';', start);
        std::string piece = (semi == std::string::npos) ? addr.substr(start)
                                                        : addr.substr(start, semi - start);
        // Trim leading and trailing whitespace
        size_t b = 0, e = piece.size();
        while (b < e && (piece[b] == ' ' || piece[b] == '\t')) ++b;
        while (e > b && (piece[e - 1] == ' ' || piece[e - 1] == '\t')) --e;
        piece = piece.substr(b, e - b);
        if (!piece.empty()) nameServerAddrs_.push_back(piece);
        if (semi == std::string::npos) break;
        start = semi + 1;
    }
}

void DefaultMQAdminExt::setNameServerAddresses(const std::vector<std::string>& addrs) {
    nameServerAddrs_ = addrs;
}

std::string DefaultMQAdminExt::getNamesrvAddr() const {
    std::string s;
    for (size_t i = 0; i < nameServerAddrs_.size(); ++i) {
        if (i) s += ";";
        s += nameServerAddrs_[i];
    }
    return s;
}

void DefaultMQAdminExt::start() {
    if (started_) return;
    if (nameServerAddrs_.empty()) {
        throw MQClientException("name server address is not set");
    }
    instanceName_ = changeInstanceNameToPID(instanceName_);
    if (clientId_.empty()) {
        clientId_ = buildClientId(instanceName_, unitName_, enableStreamRequestType_);
    }
    mqClient_.reset(new MQClientInstance(clientId_, nameServerAddrs_,
                                        3000, 15000, MQClientInstance::tlsEnabledFromEnv(),
                                        unitName_, pollNameServerIntervalMillis_));
    // The instance answers the delegated offset / consumer-list family, and it applies the
    // VIP translation itself -- so the admin's switch has to be handed over before start().
    mqClient_->setVipChannelEnabled(vipChannelEnabled_);
    mqClient_->setMaxPageSizeInGetMetadata(maxPageSizeInGetMetadata_);
    std::shared_ptr<RPCHook> requestHook =
        composeRequestHooks(enableStreamRequestType_, rpcHook_,
                            [this] { return namespaceV2_; });
    if (requestHook && !mqClient_->registerRPCHook(requestHook)) {
        logger_warn("admin rpc hook ignored: MQClientInstance already has one (clientId="
                    + clientId_ + ")");
    }
    mqClient_->start();
    started_ = true;
}

void DefaultMQAdminExt::shutdown() {
    if (!started_) return;
    started_ = false;
    if (mqClient_) {
        mqClient_->shutdown();
    }
}

MQClientInstance& DefaultMQAdminExt::requireClient() {
    if (!started_ || !mqClient_) {
        throw MQClientException("admin not started, call start() first");
    }
    return *mqClient_;
}

MQClientInstance& DefaultMQAdminExt::getMQClientInstance() { return requireClient(); }

// ---------------------------------------------------------------- Internal utilities
std::string DefaultMQAdminExt::firstBrokerAddr(MQClientInstance& client) {
    ClusterInfo ci = client.getBrokerClusterInfo();
    std::vector<std::string> addrs = ci.getBrokerAddrs();
    if (!addrs.empty()) return addrs[0];
    throw MQClientException("no broker address available");
}

std::string DefaultMQAdminExt::findFirstBrokerAddr(MQClientInstance& client) {
    return firstBrokerAddr(client);
}

std::string DefaultMQAdminExt::brokerAddrForMq(MQClientInstance& client, const MessageQueue& mq) {
    return client.brokerAddrForMq(mq);
}

std::vector<std::string> DefaultMQAdminExt::brokerAddrsOfCluster(MQClientInstance& client,
                                                                const std::string& clusterName) {
    ClusterInfo ci = client.getBrokerClusterInfo();
    if (clusterName.empty()) return ci.getBrokerAddrs();
    std::vector<std::string> addrs = ci.getBrokerAddrsOfCluster(clusterName);
    if (addrs.empty()) return ci.getBrokerAddrs();
    return addrs;
}

// ---------------------------------------------------------------- Topic management
void DefaultMQAdminExt::createTopic(const std::string& /*key*/, const std::string& newTopic,
                                    int32_t queueNum, int32_t /*topicSysFlag*/) {
    Validators::checkTopic(newTopic);
    Validators::isSystemTopic(newTopic);
    requireClient().createTopicInRoute(newTopic, queueNum, queueNum, MixAll::READ_PERM_BY_DEFAULT);
}

void DefaultMQAdminExt::createAndUpdateTopicConfig(const std::string& addr,
                                                  const TopicConfig& config) {
    requireClient().createTopicInBroker(addr, MixAll::DEFAULT_TOPIC, config.topicName,
                                        config.readQueueNums, config.writeQueueNums, config.perm,
                                        config.topicSysFlag, config.topicFilterType,
                                        MixAll::properties2String(config.attributes));
}

void DefaultMQAdminExt::createTopicInBroker(const std::string& brokerAddr,
                                           const std::string& topic, int32_t readQueueNums,
                                           int32_t writeQueueNums, int32_t perm) {
    requireClient().createTopicInBroker(brokerAddr, MixAll::DEFAULT_TOPIC, topic, readQueueNums,
                                        writeQueueNums, perm);
}

void DefaultMQAdminExt::deleteTopicInBroker(const std::string& brokerAddr,
                                           const std::string& topic) {
    requireClient().deleteTopicInBroker(brokerAddr, topic);
}

void DefaultMQAdminExt::deleteTopicInNameServer(const std::vector<std::string>& addrs,
                                               const std::string& topic) {
    MQClientInstance& client = requireClient();
    std::vector<std::string> targets = addrs.empty() ? client.nameServerAddrs() : addrs;
    for (const std::string& ns : targets) {
        PropertyMap ext;
        ext["topic"] = topic;
        client.invokeSync(ns, RequestCode::DELETE_TOPIC_IN_NAMESRV, ext);
    }
}

void DefaultMQAdminExt::deleteTopicInNamesrv(const std::string& topic) {
    requireClient().deleteTopicInNamesrv(topic);
}

void DefaultMQAdminExt::deleteTopic(const std::string& topic, const std::string& clusterName) {
    MQClientInstance& client = requireClient();
    for (const std::string& broker : brokerAddrsOfCluster(client, clusterName)) {
        try {
            client.deleteTopicInBroker(broker, topic);
        } catch (const std::exception& e) {
            logger_warn("delete topic " + topic + " in broker " + broker
                           + " failed: " + std::string(e.what()));
        }
    }
    try {
        client.deleteTopicInNamesrv(topic);
    } catch (const std::exception& e) {
        logger_warn("delete topic " + topic + " in name server failed: " + std::string(e.what()));
    }
    for (const std::string& ns : kvNamespaceToDeleteList_) {
        try {
            deleteKvConfig(ns, topic);
        } catch (const std::exception& e) {
            logger_warn("delete kv config " + ns + "/" + topic + " failed: "
                           + std::string(e.what()));
        }
    }
}

TopicList DefaultMQAdminExt::fetchAllTopicList() {
    return requireClient().getAllTopicListFromNameServer();
}

std::set<std::string> DefaultMQAdminExt::fetchTopicsByCluster(const std::string& clusterName) {
    MQClientInstance& client = requireClient();
    PropertyMap ext;
    ext["clusterName"] = clusterName;
    std::set<std::string> topics;
    std::string lastError;
    for (const std::string& ns : client.nameServerAddrs()) {
        try {
            RemotingCommand response = client.invokeSync(
                ns, RequestCode::GET_TOPICS_BY_CLUSTER, ext, Bytes(), false, timeoutMillis_);
            if (!response.body.empty()) {
                JsonValue v;
                if (RemotingSerializable::decode(response.body, v)) {
                    const JsonValue* arr = v.find("topicList");
                    if (arr != nullptr && arr->isArray()) {
                        for (size_t i = 0; i < arr->size(); ++i) {
                            if (arr->at(i).isString()) topics.insert(arr->at(i).stringValue());
                        }
                    }
                }
            }
            return topics;
        } catch (const std::exception& e) {
            lastError = e.what();
        }
    }
    throw MQClientException("all name servers unreachable: " + lastError);
}

std::set<std::string> DefaultMQAdminExt::getClusterList(const std::string& topic) {
    MQClientInstance& client = requireClient();
    ClusterInfo ci = client.getBrokerClusterInfo();
    TopicRouteData route = examineTopicRoute(topic);
    std::set<std::string> brokerNames;
    for (const BrokerData& bd : route.brokerDatas) brokerNames.insert(bd.brokerName);

    std::set<std::string> clusters;
    for (const auto& kv : ci.clusterAddrTable) {
        for (const std::string& n : kv.second) {
            if (brokerNames.count(n) > 0) {
                clusters.insert(kv.first);
                break;
            }
        }
    }
    return clusters;
}

std::vector<TopicRouteData> DefaultMQAdminExt::fetchAllTopicRoute() {
    MQClientInstance& client = requireClient();
    std::vector<TopicRouteData> result;
    TopicList topics = client.getAllTopicListFromNameServer();
    for (const std::string& topic : topics.topicList) {
        try {
            auto route = client.getTopicRouteData(topic);
            if (route != nullptr) result.push_back(*route);
        } catch (const std::exception&) {
            continue;
        }
    }
    return result;
}

TopicRouteData DefaultMQAdminExt::examineTopicRoute(const std::string& topic) {
    auto route = requireClient().getTopicRouteData(topic);
    if (route == nullptr) {
        throw MQClientException("topic " + topic + " not exist");
    }
    return *route;
}

TopicConfig DefaultMQAdminExt::examineTopicConfig(const std::string& addr,
                                                 const std::string& topic) {
    PropertyMap ext;
    ext["topic"] = topic;
    ext["lo"] = "true";
    RemotingCommand response = requireClient().invokeSync(
        vipAddr(addr), RequestCode::GET_TOPIC_CONFIG, ext, Bytes(), false, timeoutMillis_);
    if (response.body.empty()) {
        throw MQBrokerException(ResponseCode::SYSTEM_ERROR, "empty topic config for " + topic);
    }
    TopicConfig cfg;
    if (!TopicConfig::decode(response.body, cfg)) {
        throw MQBrokerException(ResponseCode::SYSTEM_ERROR, "bad topic config body for " + topic);
    }
    return cfg;
}

TopicConfigSerializeWrapper DefaultMQAdminExt::getAllTopicConfig(const std::string& brokerAddr,
                                                                int32_t timeoutMillis) {
    MQClientInstance& client = requireClient();
    const int32_t timeout = timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis;
    const int64_t begin = UtilAll::currentTimeMillis();
    const int32_t pageSize = client.maxPageSizeInGetMetadata();

    TopicConfigSerializeWrapper result;
    int64_t topicSeq = 0;
    JsonValue currentDataVersion = JsonValue::makeNull();
    bool haveVersion = false;

    while (true) {
        const int64_t left = timeout - (UtilAll::currentTimeMillis() - begin);
        if (left < 0) {
            throw MQClientException("invokeSync call timeout");
        }
        PropertyMap ext;
        ext["topicSeq"] = i64str(topicSeq);
        ext["maxTopicNum"] = i64str(pageSize);
        // The key is always present; its first-page value is the empty string.
        ext["dataVersion"] = haveVersion ? dvText(currentDataVersion) : std::string();

        RemotingCommand response = client.invokeSyncRaw(
            vipAddr(brokerAddr), RequestCode::GET_ALL_TOPIC_CONFIG, ext, Bytes(), false,
            static_cast<int32_t>(left));
        if (response.code != ResponseCode::SUCCESS) {
            throw MQBrokerException(response.code, response.remark);
        }
        TopicConfigSerializeWrapper page;
        if (!response.body.empty()) TopicConfigSerializeWrapper::decode(response.body, page);
        for (const auto& kv : page.topicConfigTable) {
            result.topicConfigTable[kv.first] = kv.second;
        }

        auto totalIt = response.extFields.find("totalTopicNum");
        if (totalIt == response.extFields.end() || totalIt->second.empty()) {
            // Old broker: the single response already contains the whole table.
            if (!haveVersion) currentDataVersion = page.dataVersion;
            break;
        }
        int64_t total = 0;
        try {
            total = std::stoll(totalIt->second);
        } catch (...) {
            if (!haveVersion) currentDataVersion = page.dataVersion;
            break;
        }

        const JsonValue newVersion = page.dataVersion;
        if (!haveVersion) {
            currentDataVersion = newVersion;
            haveVersion = true;
        } else if (dvText(currentDataVersion) != dvText(newVersion)) {
            // The table changed underneath the paging walk: restart from the first page with
            // the version just observed and discard the pages merged so far.
            logger_warn("topic config dataVersion changed, restart paging");
            currentDataVersion = newVersion;
            topicSeq = 0;
            result.topicConfigTable.clear();
            continue;
        }

        topicSeq += pageSize;
        if (topicSeq >= total - 1) break;
    }

    result.dataVersion = currentDataVersion;
    return result;
}

TopicConfigSerializeWrapper DefaultMQAdminExt::getUserTopicConfig(
    const std::string& brokerAddr, bool specialTopic, int32_t timeoutMillis) {
    TopicConfigSerializeWrapper wrapper = getAllTopicConfig(brokerAddr, timeoutMillis);
    TopicList sysTopics = getSystemTopicListFromBroker(brokerAddr, timeoutMillis);
    std::set<std::string> sysSet(sysTopics.topicList.begin(), sysTopics.topicList.end());

    std::map<std::string, TopicConfig> kept;
    for (const auto& kv : wrapper.topicConfigTable) {
        const std::string& name = kv.first;
        if (sysSet.count(name) > 0) continue;
        if (MixAll::isSysTopic(name)) continue;
        bool isRetryOrDlq = name.rfind(MixAll::RETRY_GROUP_TOPIC_PREFIX, 0) == 0
                         || name.rfind(MixAll::DLQ_GROUP_TOPIC_PREFIX, 0) == 0;
        if (!specialTopic && isRetryOrDlq) continue;
        kept[name] = kv.second;
    }
    wrapper.topicConfigTable = kept;
    return wrapper;
}

TopicList DefaultMQAdminExt::getSystemTopicListFromBroker(const std::string& brokerAddr,
                                                         int32_t timeoutMillis) {
    RemotingCommand response = requireClient().invokeSync(
        vipAddr(brokerAddr), RequestCode::GET_SYSTEM_TOPIC_LIST_FROM_BROKER, PropertyMap(), Bytes(), false,
        timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis);
    TopicList tl;
    if (!response.body.empty()) TopicList::decode(response.body, tl);
    return tl;
}

TopicStatsTable DefaultMQAdminExt::examineTopicStats(const std::string& topic) {
    TopicRouteData route = examineTopicRoute(topic);
    TopicStatsTable merged;
    for (const BrokerData& bd : route.brokerDatas) {
        std::string addr = bd.selectBrokerAddr();
        if (addr.empty()) continue;
        try {
            TopicStatsTable part = examineTopicStatsByBroker(addr, topic);
            for (const auto& kv : part.offsetTable) merged.offsetTable[kv.first] = kv.second;
            merged.topicPutTps += part.topicPutTps;
        } catch (const std::exception& e) {
            logger_warn("getTopicStatsInfo error. topic=" + topic + " broker=" + addr + ": "
                           + std::string(e.what()));
        }
    }
    if (merged.offsetTable.empty()) {
        throw MQClientException("Not found the topic stats info");
    }
    return merged;
}

TopicStatsTable DefaultMQAdminExt::examineTopicStatsByBroker(const std::string& brokerAddr,
                                                            const std::string& topic) {
    PropertyMap ext;
    ext["topic"] = topic;
    RemotingCommand response = requireClient().invokeSync(
        vipAddr(brokerAddr), RequestCode::GET_TOPIC_STATS_INFO, ext, Bytes(), false, timeoutMillis_);
    TopicStatsTable t;
    if (!response.body.empty()) TopicStatsTable::decode(response.body, t);
    return t;
}

// ---------------------------------------- Batch configuration / unitization
// GroupForbidden: response body of UPDATE_AND_GET_GROUP_FORBIDDEN(353)
bool GroupForbidden::decode(const Bytes& data, GroupForbidden& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v) || !v.isObject()) return false;
    v.tryGetString("topic", out.topic);
    v.tryGetString("group", out.group);
    v.tryGetBool("readable", out.readable);
    return true;
}

void DefaultMQAdminExt::createAndUpdateTopicConfigList(const std::string& brokerAddr,
                                                      const std::vector<TopicConfig>& configs) {
    // UPDATE_AND_CREATE_TOPIC_LIST(18): the custom header is **empty** (the topic name acts as the
    // authorized resource in the body), and the body is CreateTopicListRequestBody JSON, i.e. {"topicConfigList":[...]}.
    // The list is sent as received -- an empty one is not filtered out here.
    JsonValue rows = JsonValue::makeArray();
    for (const TopicConfig& config : configs) {
        rows.pushArray(config.toJson());
    }
    JsonValue body = JsonValue::makeObject();
    body.set("topicConfigList", rows);
    RemotingCommand response = requireClient().invokeSyncRaw(
        vipAddr(brokerAddr), RequestCode::UPDATE_AND_CREATE_TOPIC_LIST, PropertyMap(),
        RemotingSerializable::encode(body), true, timeoutMillis_);
    if (response.code != ResponseCode::SUCCESS) {
        throw MQClientException(response.remark, response.code);
    }
}

void DefaultMQAdminExt::createAndUpdateSubscriptionGroupConfigList(
    const std::string& brokerAddr, const std::vector<SubscriptionGroupConfig>& configs) {
    // UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST(225): same shape as the topic-list one, but the body key is
    // `groupConfigList` (the single-group version 200 has a bare SubscriptionGroupConfig object as its body).
    JsonValue rows = JsonValue::makeArray();
    for (const SubscriptionGroupConfig& config : configs) {
        rows.pushArray(config.toJson());
    }
    JsonValue body = JsonValue::makeObject();
    body.set("groupConfigList", rows);
    RemotingCommand response = requireClient().invokeSyncRaw(
        vipAddr(brokerAddr), RequestCode::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST, PropertyMap(),
        RemotingSerializable::encode(body), true, timeoutMillis_);
    if (response.code != ResponseCode::SUCCESS) {
        throw MQClientException(response.remark, response.code);
    }
}

void DefaultMQAdminExt::createStaticTopic(const std::string& brokerAddr,
                                         const std::string& defaultTopic,
                                         const TopicConfig& config,
                                         const JsonValue& mappingDetail, bool force) {
    // UPDATE_AND_CREATE_STATIC_TOPIC(513): the header reuses CreateTopicRequestHeader fields
    // (bool rendered as lowercase "true"/"false" strings), and the body is the TopicQueueMappingDetail JSON document
    // (the mapping information of a unitized static topic).
    PropertyMap ext;
    ext["topic"] = config.topicName;
    ext["defaultTopic"] = defaultTopic;
    ext["readQueueNums"] = i64str(config.readQueueNums);
    ext["writeQueueNums"] = i64str(config.writeQueueNums);
    ext["perm"] = i64str(config.perm);
    ext["topicFilterType"] =
        config.topicFilterType.empty() ? TopicFilterType::SINGLE_TAG : config.topicFilterType;
    ext["topicSysFlag"] = i64str(config.topicSysFlag);
    ext["order"] = config.order ? "true" : "false";
    ext["force"] = force ? "true" : "false";
    requireClient().invokeSync(vipAddr(brokerAddr), RequestCode::UPDATE_AND_CREATE_STATIC_TOPIC,
                               ext, RemotingSerializable::encode(mappingDetail), true,
                               timeoutMillis_);
}

GroupForbidden DefaultMQAdminExt::updateAndGetGroupReadForbidden(
    const std::string& brokerAddr, const std::string& group, const std::string& topic,
    const std::optional<bool>& readable) {
    if (group.empty() || topic.empty()) {
        throw MQClientException("updateAndGetGroupReadForbidden: group/topic required");
    }
    PropertyMap ext;
    ext["group"] = group;
    ext["topic"] = topic;
    if (readable.has_value()) {
        ext["readable"] = *readable ? "true" : "false";
    }
    RemotingCommand response = requireClient().invokeSync(
        vipAddr(brokerAddr), RequestCode::UPDATE_AND_GET_GROUP_FORBIDDEN, ext, Bytes(), false,
        timeoutMillis_);
    if (response.body.empty()) {
        throw MQClientException("updateAndGetGroupReadForbidden: empty response body");
    }
    GroupForbidden out;
    if (!GroupForbidden::decode(response.body, out)) {
        throw MQClientException("updateAndGetGroupReadForbidden: bad GroupForbidden body");
    }
    return out;
}

bool DefaultMQAdminExt::resumeCheckHalfMessage(const std::string& brokerAddr,
                                              const std::string& topic,
                                              const std::string& msgId) {
    if (topic.empty()) {
        throw MQClientException("resumeCheckHalfMessage: topic required");
    }
    PropertyMap ext;
    ext["topic"] = topic;
    if (!msgId.empty()) ext["msgId"] = msgId;
    RemotingCommand response = requireClient().invokeSyncRaw(
        vipAddr(brokerAddr), RequestCode::RESUME_CHECK_HALF_MESSAGE, ext, Bytes(), false,
        timeoutMillis_);
    return response.code == ResponseCode::SUCCESS;
}

void DefaultMQAdminExt::createOrUpdateOrderConf(const std::string& key,
                                               const std::string& value, bool isCluster) {
    // **Not a separate request**: a read-modify-write on the NameServer KV namespace=ORDER_TOPIC_CONFIG.
    // Cluster mode writes the value verbatim; non-cluster mode treats the stored value as a ";" separated "key:value" list,
    // replaces the entry whose key matches and writes the whole list back (one topic at a time, not the entire cluster).
    if (key.empty() || value.empty()) {
        throw MQClientException("createOrUpdateOrderConf: key/value required");
    }
    if (isCluster) {
        putKvConfig(MixAll::NAMESPACE_ORDER_TOPIC_CONFIG, key, value);
        return;
    }
    std::string oldValue;
    try {
        getKvConfig(MixAll::NAMESPACE_ORDER_TOPIC_CONFIG, key, oldValue);
    } catch (const std::exception&) {
        oldValue.clear();
    }
    std::map<std::string, std::string> entries;
    size_t start = 0;
    while (start <= oldValue.size()) {
        size_t semi = oldValue.find(';', start);
        std::string entry = (semi == std::string::npos) ? oldValue.substr(start)
                                                        : oldValue.substr(start, semi - start);
        // Trim leading and trailing whitespace
        size_t b = 0, e = entry.size();
        while (b < e && (entry[b] == ' ' || entry[b] == '\t')) ++b;
        while (e > b && (entry[e - 1] == ' ' || entry[e - 1] == '\t')) --e;
        entry = entry.substr(b, e - b);
        if (!entry.empty()) {
            std::string entryKey = entry;
            size_t colon = entry.find(':');
            if (colon != std::string::npos) entryKey = entry.substr(0, colon);
            entries[entryKey] = entry;
        }
        if (semi == std::string::npos) break;
        start = semi + 1;
    }
    std::string newKey = value;
    size_t colon = value.find(':');
    if (colon != std::string::npos) newKey = value.substr(0, colon);
    if (newKey.empty()) {
        throw MQClientException("createOrUpdateOrderConf: value must start with a key");
    }
    entries[newKey] = value;
    std::string merged;
    for (const auto& kv : entries) {
        if (!merged.empty()) merged += ";";
        merged += kv.second;
    }
    putKvConfig(MixAll::NAMESPACE_ORDER_TOPIC_CONFIG, key, merged);
}

// ---------------------------------------- Operational cleanup
void DefaultMQAdminExt::cleanExpiredConsumerQueue(const std::string& brokerAddr,
                                                 int32_t timeHours) {
    // CLEAN_EXPIRED_CONSUMEQUEUE(306): the broker drops consume-queue entries older than `time` hours.
    PropertyMap ext;
    ext["time"] = i64str(timeHours);
    requireClient().invokeSync(vipAddr(brokerAddr), RequestCode::CLEAN_EXPIRED_CONSUMEQUEUE, ext,
                               Bytes(), false, timeoutMillis_);
}

std::vector<std::string> DefaultMQAdminExt::cleanExpiredConsumerQueueByAddr(
    const std::vector<std::string>& addrs, int32_t timeHours) {
    std::vector<std::string> failed;
    for (const std::string& addr : addrs) {
        try {
            cleanExpiredConsumerQueue(addr, timeHours);
        } catch (const std::exception&) {
            failed.push_back(addr);
        }
    }
    return failed;
}

void DefaultMQAdminExt::deleteExpiredCommitLog(const std::string& brokerAddr, int32_t timeHours) {
    // DELETE_EXPIRED_COMMITLOG(329): the broker deletes commit-log files older than `time` hours.
    PropertyMap ext;
    ext["time"] = i64str(timeHours);
    requireClient().invokeSync(vipAddr(brokerAddr), RequestCode::DELETE_EXPIRED_COMMITLOG, ext,
                               Bytes(), false, timeoutMillis_);
}

std::vector<std::string> DefaultMQAdminExt::deleteExpiredCommitLogByAddr(
    const std::vector<std::string>& addrs, int32_t timeHours) {
    std::vector<std::string> failed;
    for (const std::string& addr : addrs) {
        try {
            deleteExpiredCommitLog(addr, timeHours);
        } catch (const std::exception&) {
            failed.push_back(addr);
        }
    }
    return failed;
}

void DefaultMQAdminExt::cleanUnusedTopicByAddr(const std::string& brokerAddr) {
    // CLEAN_UNUSED_TOPIC(316): a **single request**; the broker cleans unused topics itself. The client must never
    // iterate the topic table and delete one by one -- broker-created BenchmarkTest and retry/DLQ topics are rejected by the broker
    // with SYSTEM_ERROR.
    requireClient().invokeSync(vipAddr(brokerAddr), RequestCode::CLEAN_UNUSED_TOPIC, PropertyMap(),
                               Bytes(), false, timeoutMillis_);
}

JsonValue DefaultMQAdminExt::queryConsumeTimeSpan(const std::string& topic,
                                                 const std::string& group) {
    // QUERY_CONSUME_TIME_SPAN(303): fans out to every master along the topic route and aggregates the
    // consumeTimeSpanSet JSON array from the response bodies.
    TopicRouteData route = examineTopicRoute(topic);
    JsonValue spans = JsonValue::makeArray();
    for (const BrokerData& bd : route.brokerDatas) {
        std::string addr = bd.selectBrokerAddr();
        if (addr.empty()) continue;
        PropertyMap ext;
        ext["topic"] = topic;
        ext["group"] = group;
        RemotingCommand response = requireClient().invokeSync(
            vipAddr(addr), RequestCode::QUERY_CONSUME_TIME_SPAN, ext, Bytes(), false,
            timeoutMillis_);
        if (response.body.empty()) continue;
        JsonValue v;
        if (!RemotingSerializable::decode(response.body, v)) continue;
        const JsonValue* arr = v.find("consumeTimeSpanSet");
        if (arr != nullptr && arr->isArray()) {
            for (size_t i = 0; i < arr->size(); ++i) spans.pushArray(arr->at(i));
        }
    }
    return spans;
}

std::set<std::string> DefaultMQAdminExt::getTopicClusterList(const std::string& topic) {
    return getClusterList(topic);
}

void DefaultMQAdminExt::setMessageRequestMode(const std::string& brokerAddr,
                                             const std::string& topic,
                                             const std::string& consumerGroup,
                                             const std::string& mode, int32_t popShareQueueNum) {
    // SET_MESSAGE_REQUEST_MODE(401): switches a consumer group between POP and Pull modes (unitized scenario).
    PropertyMap ext;
    ext["topic"] = topic;
    ext["consumerGroup"] = consumerGroup;
    ext["mode"] = mode;
    if (popShareQueueNum > 0) ext["popShareQueueNum"] = i64str(popShareQueueNum);
    requireClient().invokeSync(vipAddr(brokerAddr), RequestCode::SET_MESSAGE_REQUEST_MODE, ext,
                               Bytes(), false, timeoutMillis_);
}

// ---------------------------------------- NameServer configuration (318/319)
void DefaultMQAdminExt::updateNameServerConfig(const PropertyMap& properties,
                                              int32_t timeoutMillis) {
    std::string text = MixAll::properties2String(properties);
    if (text.empty()) return;
    MQClientInstance& client = requireClient();
    int32_t timeout = timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis;
    bool anyFailed = false;
    int32_t lastCode = 0;
    std::string lastRemark;
    for (const std::string& ns : client.nameServerAddrs()) {
        RemotingCommand response = client.invokeSyncRaw(
            ns, RequestCode::UPDATE_NAMESRV_CONFIG, PropertyMap(), text, true, timeout);
        if (response.code != ResponseCode::SUCCESS) {
            anyFailed = true;
            lastCode = response.code;
            lastRemark = response.remark;
        }
    }
    if (anyFailed) {
        throw MQClientException(
            lastRemark.empty() ? "update name server config failed" : lastRemark, lastCode);
    }
}

std::map<std::string, PropertyMap> DefaultMQAdminExt::getNameServerConfig(
    const std::vector<std::string>& namesrvAddrs, int32_t timeoutMillis) {
    // GET_NAMESRV_CONFIG(319): queried against each NameServer, the body is properties text;
    // returning {address: properties dictionary}.
    MQClientInstance& client = requireClient();
    std::vector<std::string> targets =
        namesrvAddrs.empty() ? client.nameServerAddrs() : namesrvAddrs;
    int32_t timeout = timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis;
    std::map<std::string, PropertyMap> result;
    for (const std::string& ns : targets) {
        RemotingCommand response =
            client.invokeSync(ns, RequestCode::GET_NAMESRV_CONFIG, PropertyMap(), Bytes(), false,
                              timeout);
        result[ns] = MixAll::string2Properties(response.body);
    }
    return result;
}

// ---------------------------------------------------------------- Cluster / Broker
ClusterInfo DefaultMQAdminExt::fetchBrokerClusterInfo() {
    return requireClient().getBrokerClusterInfo();
}

KVTable DefaultMQAdminExt::fetchBrokerRuntimeStats(const std::string& brokerAddr,
                                                  int32_t timeoutMillis) {
    RemotingCommand response = requireClient().invokeSync(
        vipAddr(brokerAddr), RequestCode::GET_BROKER_RUNTIME_INFO, PropertyMap(), Bytes(), false,
        timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis);
    KVTable t;
    if (!response.body.empty()) KVTable::decode(response.body, t);
    return t;
}

PropertyMap DefaultMQAdminExt::getBrokerConfig(const std::string& brokerAddr,
                                               int32_t timeoutMillis) {
    // ⚠ The response body is **properties text** ("k=v\n"), not JSON/KVTable.
    // The early implementation parsed it as KVTable JSON, which always fails against a real broker.
    // Reading a broker's own configuration is the one admin RPC that always uses the listening
    // port: the fast channel serves the other admin RPCs, not this one.
    RemotingCommand response = requireClient().invokeSync(
        brokerAddr, RequestCode::GET_BROKER_CONFIG, PropertyMap(), Bytes(), false,
        timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis);
    return MixAll::string2Properties(response.body);
}

void DefaultMQAdminExt::updateBrokerConfig(const std::string& brokerAddr,
                                          const PropertyMap& properties,
                                          int32_t timeoutMillis) {
    auto it = properties.find("brokerPermission");
    if (it != properties.end()) {
        int32_t perm = -1;
        try {
            perm = static_cast<int32_t>(std::stol(it->second));
        } catch (...) {
            perm = -1;
        }
        if (!PermName::isValid(perm)) {
            throw MQClientException("brokerPermission value: " + it->second + " is invalid.",
                                    ResponseCode::NO_PERMISSION);
        }
    }
    std::string text = MixAll::properties2String(properties);
    if (text.empty()) return;
    requireClient().invokeSync(vipAddr(brokerAddr), RequestCode::UPDATE_BROKER_CONFIG, PropertyMap(),
                               text, true, timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis);
}

int32_t DefaultMQAdminExt::wipeWritePermOfBroker(const std::string& namesrvAddr,
                                                const std::string& brokerName) {
    PropertyMap ext;
    ext["brokerName"] = brokerName;
    RemotingCommand response = requireClient().invokeSync(
        namesrvAddr, RequestCode::WIPE_WRITE_PERM_OF_BROKER, ext);
    return static_cast<int32_t>(extInt(response, "wipeTopicCount", 0));
}

int32_t DefaultMQAdminExt::addWritePermOfBroker(const std::string& namesrvAddr,
                                               const std::string& brokerName) {
    PropertyMap ext;
    ext["brokerName"] = brokerName;
    RemotingCommand response = requireClient().invokeSync(
        namesrvAddr, RequestCode::ADD_WRITE_PERM_OF_BROKER, ext);
    return static_cast<int32_t>(extInt(response, "addTopicCount", 0));
}

bool DefaultMQAdminExt::cleanUnusedTopic(const std::string& clusterName,
                                        const std::string& /*topic*/) {
    MQClientInstance& client = requireClient();
    bool ok = true;
    for (const std::string& addr : brokerAddrsOfCluster(client, clusterName)) {
        try {
            client.invokeSync(vipAddr(addr), RequestCode::CLEAN_UNUSED_TOPIC, PropertyMap(), Bytes(),
                              false, timeoutMillis_);
        } catch (const std::exception& e) {
            logger_warn("cleanUnusedTopic on " + addr + " failed: " + std::string(e.what()));
            ok = false;
        }
    }
    return ok;
}

JsonValue DefaultMQAdminExt::viewBrokerStatsData(const std::string& brokerAddr,
                                                const std::string& statsName,
                                                const std::string& statsKey) {
    PropertyMap ext;
    ext["statsName"] = statsName;
    ext["statsKey"] = statsKey;
    RemotingCommand response = requireClient().invokeSync(
        vipAddr(brokerAddr), RequestCode::VIEW_BROKER_STATS_DATA, ext);
    JsonValue v;
    if (response.body.empty()) return JsonValue::makeNull();
    RemotingSerializable::decode(response.body, v);
    return v;
}

// ---------------------------------------------------------------- NameServer KV
void DefaultMQAdminExt::createAndUpdateKvConfig(const std::string& ns, const std::string& key,
                                               const std::string& value) {
    MQClientInstance& client = requireClient();
    PropertyMap ext;
    ext["namespace"] = ns;
    ext["key"] = key;
    ext["value"] = value;
    std::string lastError;
    bool anyFailed = false;
    for (const std::string& addr : client.nameServerAddrs()) {
        RemotingCommand response = client.invokeSyncRaw(
            addr, RequestCode::PUT_KV_CONFIG, ext, Bytes(), false, timeoutMillis_);
        if (response.code != ResponseCode::SUCCESS) {
            anyFailed = true;
            lastError = response.remark.empty() ? "put kv config failed" : response.remark;
        }
    }
    if (anyFailed) throw MQClientException(lastError);
}

bool DefaultMQAdminExt::getKvConfig(const std::string& ns, const std::string& key,
                                   std::string& outValue) {
    MQClientInstance& client = requireClient();
    PropertyMap ext;
    ext["namespace"] = ns;
    ext["key"] = key;
    std::string lastError;
    for (const std::string& addr : client.nameServerAddrs()) {
        try {
            RemotingCommand response = client.invokeSyncRaw(
                addr, RequestCode::GET_KV_CONFIG, ext, Bytes(), false, timeoutMillis_);
            if (response.code == ResponseCode::SUCCESS) {
                auto it = response.extFields.find("value");
                if (it != response.extFields.end()) {
                    outValue = it->second;
                    return true;
                }
                outValue.clear();
                return false;
            }
            // This NS explicitly answered "none": return false right away (do not try the next one)
            return false;
        } catch (const std::exception& e) {
            lastError = e.what();
        }
    }
    throw MQClientException("all name servers unreachable: " + lastError);
}

void DefaultMQAdminExt::deleteKvConfig(const std::string& ns, const std::string& key) {
    MQClientInstance& client = requireClient();
    PropertyMap ext;
    ext["namespace"] = ns;
    ext["key"] = key;
    std::string lastError;
    bool anyFailed = false;
    for (const std::string& addr : client.nameServerAddrs()) {
        RemotingCommand response = client.invokeSyncRaw(
            addr, RequestCode::DELETE_KV_CONFIG, ext, Bytes(), false, timeoutMillis_);
        if (response.code != ResponseCode::SUCCESS) {
            anyFailed = true;
            lastError = response.remark.empty() ? "delete kv config failed" : response.remark;
        }
    }
    if (anyFailed) throw MQClientException(lastError);
}

KVTable DefaultMQAdminExt::getKvListByNamespace(const std::string& ns) {
    MQClientInstance& client = requireClient();
    PropertyMap ext;
    ext["namespace"] = ns;
    std::string lastError;
    for (const std::string& addr : client.nameServerAddrs()) {
        try {
            RemotingCommand response = client.invokeSync(
                addr, RequestCode::GET_KVLIST_BY_NAMESPACE, ext, Bytes(), false, timeoutMillis_);
            KVTable t;
            if (!response.body.empty()) KVTable::decode(response.body, t);
            return t;
        } catch (const std::exception& e) {
            lastError = e.what();
        }
    }
    throw MQClientException("all name servers unreachable: " + lastError);
}

// ---------------------------------------------------------------- Subscription group management
void DefaultMQAdminExt::createAndUpdateSubscriptionGroupConfig(
    const std::string& addr, const SubscriptionGroupConfig& config) {
    Bytes body = config.encode();
    requireClient().invokeSync(vipAddr(addr), RequestCode::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP,
                               PropertyMap(), body, true, timeoutMillis_);
}

bool DefaultMQAdminExt::examineSubscriptionGroupConfig(const std::string& addr,
                                                      const std::string& group,
                                                      SubscriptionGroupConfig& out) {
    SubscriptionGroupWrapper wrapper = getAllSubscriptionGroup(addr);
    auto it = wrapper.subscriptionGroupTable.find(group);
    if (it == wrapper.subscriptionGroupTable.end()) return false;
    out = it->second;
    return true;
}

bool DefaultMQAdminExt::getSubscriptionGroupConfig(const std::string& addr,
                                                  const std::string& group,
                                                  SubscriptionGroupConfig& out) {
    PropertyMap ext;
    ext["group"] = group;
    RemotingCommand response = requireClient().invokeSync(
        vipAddr(addr), RequestCode::GET_SUBSCRIPTIONGROUP_CONFIG, ext, Bytes(), false,
        timeoutMillis_);
    if (response.body.empty()) return false;
    return SubscriptionGroupConfig::decode(response.body, out);
}

SubscriptionGroupWrapper DefaultMQAdminExt::getAllSubscriptionGroup(const std::string& brokerAddr,
                                                                   int32_t timeoutMillis) {
    MQClientInstance& client = requireClient();
    int32_t timeout = timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis;
    int64_t begin = UtilAll::currentTimeMillis();
    const int32_t pageSize = client.maxPageSizeInGetMetadata();

    JsonValue currentDataVersion = JsonValue::makeNull();
    int64_t groupSeq = 0;
    bool haveVersion = false;
    std::map<std::string, SubscriptionGroupConfig> table;
    JsonValue forbidden = JsonValue::makeObject();

    while (true) {
        int64_t left = timeout - (UtilAll::currentTimeMillis() - begin);
        if (left < 0) {
            throw MQClientException("invokeSync call timeout");
        }
        PropertyMap ext;
        ext["groupSeq"] = i64str(groupSeq);
        ext["maxGroupNum"] = i64str(pageSize);
        // The key is always present; its first-page value is the empty string.
        ext["dataVersion"] = haveVersion ? dvText(currentDataVersion) : std::string();

        RemotingCommand response = client.invokeSyncRaw(
            vipAddr(brokerAddr), RequestCode::GET_ALL_SUBSCRIPTIONGROUP_CONFIG, ext, Bytes(), false,
            static_cast<int32_t>(left));
        if (response.code != ResponseCode::SUCCESS) {
            throw MQBrokerException(response.code, response.remark);
        }
        SubscriptionGroupWrapper wrapper;
        if (!response.body.empty()) SubscriptionGroupWrapper::decode(response.body, wrapper);
        for (const auto& kv : wrapper.subscriptionGroupTable) table[kv.first] = kv.second;
        if (wrapper.forbiddenTable.isObject()) {
            for (const auto& kv : wrapper.forbiddenTable.objectItems()) {
                forbidden.set(kv.first, kv.second);
            }
        }

        auto totalIt = response.extFields.find("totalGroupNum");
        if (totalIt == response.extFields.end() || totalIt->second.empty()) {
            // Old broker: everything in one response
            if (!haveVersion) currentDataVersion = wrapper.dataVersion;
            break;
        }
        int64_t total = 0;
        try {
            total = std::stoll(totalIt->second);
        } catch (...) {
            if (!haveVersion) currentDataVersion = wrapper.dataVersion;
            break;
        }

        JsonValue newVersion = wrapper.dataVersion;
        if (!haveVersion) {
            currentDataVersion = newVersion;
            haveVersion = true;
        } else if (dvText(currentDataVersion) != dvText(newVersion)) {
            logger_warn("subscription group dataVersion changed, restart paging");
            currentDataVersion = newVersion;
            groupSeq = 0;
            table.clear();
            forbidden = JsonValue::makeObject();
            continue;
        }

        groupSeq += pageSize;
        if (groupSeq >= total - 1) break;
    }

    SubscriptionGroupWrapper result;
    result.subscriptionGroupTable = table;
    result.forbiddenTable = forbidden;
    result.dataVersion = currentDataVersion;
    return result;
}

SubscriptionGroupWrapper DefaultMQAdminExt::getUserSubscriptionGroup(const std::string& brokerAddr,
                                                                    int32_t timeoutMillis) {
    SubscriptionGroupWrapper wrapper = getAllSubscriptionGroup(brokerAddr, timeoutMillis);
    std::map<std::string, SubscriptionGroupConfig> kept;
    for (const auto& kv : wrapper.subscriptionGroupTable) {
        if (MixAll::isSysConsumerGroup(kv.first)) continue;
        if (MixAll::isPredefinedGroup(kv.first)) continue;
        kept[kv.first] = kv.second;
    }
    wrapper.subscriptionGroupTable = kept;
    return wrapper;
}

void DefaultMQAdminExt::deleteSubscriptionGroup(const std::string& addr,
                                               const std::string& groupName,
                                               bool removeOffset) {
    PropertyMap ext;
    ext["groupName"] = groupName;
    ext["cleanOffset"] = removeOffset ? "true" : "false";
    requireClient().invokeSync(vipAddr(addr), RequestCode::DELETE_SUBSCRIPTIONGROUP, ext, Bytes(),
                               false, timeoutMillis_);
}

// ---------------------------------------------------------------- Connection information
ConsumerConnection DefaultMQAdminExt::examineConsumerConnectionInfo(
    const std::string& consumerGroup, const std::string& brokerAddr) {
    MQClientInstance& client = requireClient();
    std::string addr = brokerAddr.empty() ? findFirstBrokerAddr(client) : brokerAddr;
    PropertyMap ext;
    ext["consumerGroup"] = consumerGroup;
    RemotingCommand response = client.invokeSync(vipAddr(addr), RequestCode::GET_CONSUMER_CONNECTION_LIST,
                                                 ext, Bytes(), false, timeoutMillis_);
    ConsumerConnection cc;
    if (!response.body.empty()) ConsumerConnection::decode(response.body, cc);
    if (cc.connectionSet.empty()) {
        throw MQClientException("Not found the consumer group connection",
                                ResponseCode::CONSUMER_NOT_ONLINE);
    }
    return cc;
}

ProducerConnection DefaultMQAdminExt::examineProducerConnectionInfo(
    const std::string& producerGroup, const std::string& brokerAddr) {
    MQClientInstance& client = requireClient();
    std::string addr = brokerAddr.empty() ? findFirstBrokerAddr(client) : brokerAddr;
    PropertyMap ext;
    ext["producerGroup"] = producerGroup;
    RemotingCommand response = client.invokeSync(vipAddr(addr), RequestCode::GET_PRODUCER_CONNECTION_LIST,
                                                 ext, Bytes(), false, timeoutMillis_);
    ProducerConnection pc;
    if (!response.body.empty()) ProducerConnection::decode(response.body, pc);
    return pc;
}

ConsumerRunningInfo DefaultMQAdminExt::examineConsumerRunningInfo(
    const std::string& consumerGroup, const std::string& clientId, bool jstack,
    const std::string& brokerAddr) {
    MQClientInstance& client = requireClient();
    std::string addr = brokerAddr.empty() ? findFirstBrokerAddr(client) : brokerAddr;
    PropertyMap ext;
    ext["consumerGroup"] = consumerGroup;
    ext["clientId"] = clientId;
    ext["jstackEnable"] = jstack ? "true" : "false";
    RemotingCommand response = client.invokeSync(vipAddr(addr), RequestCode::GET_CONSUMER_RUNNING_INFO,
                                                 ext, Bytes(), false, timeoutMillis_);
    if (response.body.empty()) {
        throw MQClientException("no running info for client " + clientId);
    }
    ConsumerRunningInfo ri;
    ConsumerRunningInfo::decode(response.body, ri);
    return ri;
}

GetConsumerListByGroupResponseBody DefaultMQAdminExt::getConsumerListByGroup(
    const std::string& consumerGroup, const std::string& brokerAddr) {
    MQClientInstance& client = requireClient();
    std::string addr = brokerAddr.empty() ? findFirstBrokerAddr(client) : brokerAddr;
    return client.getConsumerListByGroup(consumerGroup, addr);
}

// ---------------------------------------------------------------- Consumer statistics
ConsumeStats DefaultMQAdminExt::examineConsumeStats(const std::string& brokerAddr,
                                                    const std::string& consumerGroup,
                                                    const std::string& topic,
                                                    const std::vector<std::string>& topicList) {
    PropertyMap ext;
    ext["consumerGroup"] = consumerGroup;
    if (!topic.empty()) ext["topic"] = topic;
    if (!topicList.empty()) {
        std::string joined;
        for (size_t i = 0; i < topicList.size(); ++i) {
            if (i) joined += ";";
            joined += topicList[i];
        }
        ext["topicList"] = joined;
    }
    RemotingCommand response = requireClient().invokeSync(
        vipAddr(brokerAddr), RequestCode::GET_CONSUME_STATS, ext, Bytes(), false, timeoutMillis_);
    ConsumeStats cs;
    if (!response.body.empty()) ConsumeStats::decode(response.body, cs);
    return cs;
}

ConsumeStatsList DefaultMQAdminExt::fetchConsumeStatsInBroker(const std::string& brokerAddr,
                                                             bool isOrder,
                                                             int32_t timeoutMillis) {
    PropertyMap ext;
    ext["isOrder"] = isOrder ? "true" : "false";
    RemotingCommand response = requireClient().invokeSync(
        vipAddr(brokerAddr), RequestCode::GET_BROKER_CONSUME_STATS, ext, Bytes(), false,
        timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis);
    ConsumeStatsList sl;
    if (!response.body.empty()) ConsumeStatsList::decode(response.body, sl);
    return sl;
}

std::set<std::string> DefaultMQAdminExt::queryTopicConsumeByWho(const std::string& brokerAddr,
                                                               const std::string& topic) {
    PropertyMap ext;
    ext["topic"] = topic;
    RemotingCommand response = requireClient().invokeSync(
        vipAddr(brokerAddr), RequestCode::QUERY_TOPIC_CONSUME_BY_WHO, ext, Bytes(), false, timeoutMillis_);
    std::set<std::string> groups;
    if (response.body.empty()) return groups;
    JsonValue v;
    if (!RemotingSerializable::decode(response.body, v)) return groups;
    const JsonValue* arr = v.find("groupList");
    if (arr != nullptr && arr->isArray()) {
        for (size_t i = 0; i < arr->size(); ++i) {
            if (arr->at(i).isString()) groups.insert(arr->at(i).stringValue());
        }
    }
    return groups;
}

ConsumeStats DefaultMQAdminExt::examineConsumeStatsGroup(const std::string& consumerGroup,
                                                        const std::string& topic) {
    TopicRouteData route = examineTopicRoute(MixAll::getRetryTopic(consumerGroup));
    ConsumeStats result;
    for (const BrokerData& bd : route.brokerDatas) {
        std::string addr = bd.selectBrokerAddr();
        if (addr.empty()) continue;
        ConsumeStats part = examineConsumeStats(addr, consumerGroup, topic);
        for (const auto& kv : part.offsetTable) result.offsetTable[kv.first] = kv.second;
        result.consumeTps += part.consumeTps;
    }
    if (result.offsetTable.empty()) {
        throw MQClientException("no consume stats for group " + consumerGroup,
                                ResponseCode::CONSUMER_NOT_ONLINE);
    }
    return result;
}

bool DefaultMQAdminExt::consumed(const MessageExt& msg, const std::string& consumerGroup) {
    ConsumeStats cstats = examineConsumeStatsGroup(consumerGroup);
    ClusterInfo ci = examineBrokerClusterInfo();
    std::string storeHost = msg.getStoreHostString();
    for (const auto& kv : cstats.offsetTable) {
        const MessageQueue& mq = kv.first;
        if (mq.topic != msg.topic || mq.queueId != msg.queueId) continue;
        auto bdIt = ci.brokerAddrTable.find(mq.brokerName);
        if (bdIt == ci.brokerAddrTable.end()) continue;
        auto addrIt = bdIt->second.brokerAddrs.find(MixAll::MASTER_ID);
        if (addrIt == bdIt->second.brokerAddrs.end()) continue;
        if (storeHost.empty() || addrIt->second != storeHost) continue;
        if (kv.second.consumerOffset > msg.getQueueOffset()) return true;
    }
    return false;
}

const char* trackTypeName(TrackType type) {
    switch (type) {
        case TrackType::CONSUMED: return "CONSUMED";
        case TrackType::CONSUMED_BUT_FILTERED: return "CONSUMED_BUT_FILTERED";
        case TrackType::PULL: return "PULL";
        case TrackType::NOT_CONSUME_YET: return "NOT_CONSUME_YET";
        case TrackType::NOT_ONLINE: return "NOT_ONLINE";
        case TrackType::CONSUME_BROADCASTING: return "CONSUME_BROADCASTING";
        default: return "UNKNOWN";
    }
}

namespace {

std::string codeDesc(int32_t code, const std::string& msg) {
    return "CODE:" + std::to_string(code) + " DESC:" + msg;
}

}  // namespace

MessageTrack DefaultMQAdminExt::trackMessageGroup(const MessageExt& msg, const std::string& group,
                                                  bool mapBroadcast) {
    // One verdict per group, shared by the single and the concurrent track form. The connection
    // probe failure keeps the verdict UNKNOWN unless the broker answered CONSUMER_NOT_ONLINE; the
    // consumed() probe failure maps NOT_ONLINE, and (single form only) the broadcast verdict.
    MessageTrack mt;
    mt.consumerGroup = group;
    ConsumerConnection cc;
    try {
        cc = examineConsumerConnectionInfo(group);
    } catch (const MQBrokerException& e) {
        if (e.getResponseCode() == ResponseCode::CONSUMER_NOT_ONLINE) {
            mt.trackType = TrackType::NOT_ONLINE;
        }
        mt.exceptionDesc = codeDesc(e.getResponseCode(), e.getResponseMessage());
        return mt;
    } catch (const std::exception& e) {
        mt.exceptionDesc = e.what();
        return mt;
    }

    if (cc.consumeType == "CONSUME_ACTIVELY") {
        mt.trackType = TrackType::PULL;
        return mt;
    }
    if (cc.consumeType != "CONSUME_PASSIVELY") {
        return mt;
    }
    bool ifConsumed = false;
    try {
        ifConsumed = consumed(msg, group);
    } catch (const MQBrokerException& e) {
        if (e.getResponseCode() == ResponseCode::CONSUMER_NOT_ONLINE) {
            mt.trackType = TrackType::NOT_ONLINE;
            mt.exceptionDesc = codeDesc(e.getResponseCode(), e.getResponseMessage());
        }
        if (mapBroadcast && e.getResponseCode() == ResponseCode::BROADCAST_CONSUMPTION) {
            mt.trackType = TrackType::CONSUME_BROADCASTING;
        }
        return mt;
    } catch (const MQClientException& e) {
        if (e.getResponseCode() == ResponseCode::CONSUMER_NOT_ONLINE) {
            mt.trackType = TrackType::NOT_ONLINE;
            mt.exceptionDesc = codeDesc(e.getResponseCode(), e.what());
        }
        if (mapBroadcast && e.getResponseCode() == ResponseCode::BROADCAST_CONSUMPTION) {
            mt.trackType = TrackType::CONSUME_BROADCASTING;
        }
        return mt;
    } catch (const std::exception& e) {
        mt.exceptionDesc = e.what();
        return mt;
    }

    if (ifConsumed) {
        mt.trackType = TrackType::CONSUMED;
        const JsonValue* sub = cc.subscriptionTable.find(msg.topic);
        if (sub != nullptr && sub->isObject()) {
            const JsonValue* tags = sub->find("tagsSet");
            std::set<std::string> tagsSet;
            if (tags != nullptr && tags->isArray()) {
                for (size_t i = 0; i < tags->size(); ++i) {
                    if (tags->at(i).isString()) {
                        tagsSet.insert(tags->at(i).stringValue());
                    }
                }
            }
            if (!tagsSet.empty() && tagsSet.count("*") == 0
                && tagsSet.count(msg.getTags()) == 0) {
                mt.trackType = TrackType::CONSUMED_BUT_FILTERED;
            }
        }
    } else {
        mt.trackType = TrackType::NOT_CONSUME_YET;
    }
    return mt;
}

std::vector<MessageTrack> DefaultMQAdminExt::messageTrackDetail(const MessageExt& msg) {
    std::vector<MessageTrack> result;
    TopicRouteData route = examineTopicRoute(msg.topic);
    std::string brokerAddr;
    for (const BrokerData& bd : route.brokerDatas) {
        brokerAddr = bd.selectBrokerAddr();
        if (!brokerAddr.empty()) break;
    }
    if (brokerAddr.empty()) return result;
    std::set<std::string> groups = queryTopicConsumeByWho(brokerAddr, msg.topic);
    for (const std::string& group : groups) {
        result.push_back(trackMessageGroup(msg, group, true));
    }
    return result;
}

TopicList DefaultMQAdminExt::queryTopicsByConsumerToBroker(const std::string& brokerAddr,
                                                          const std::string& group) {
    PropertyMap ext;
    ext["group"] = group;
    RemotingCommand response = requireClient().invokeSync(
        vipAddr(brokerAddr), RequestCode::QUERY_TOPICS_BY_CONSUMER, ext, Bytes(), false, timeoutMillis_);
    TopicList tl;
    if (!response.body.empty()) TopicList::decode(response.body, tl);
    return tl;
}

TopicList DefaultMQAdminExt::queryTopicsByConsumer(const std::string& group) {
    TopicRouteData route = examineTopicRoute(MixAll::getRetryTopic(group));
    TopicList merged;
    for (const BrokerData& bd : route.brokerDatas) {
        std::string addr = bd.selectBrokerAddr();
        if (addr.empty()) continue;
        TopicList part = queryTopicsByConsumerToBroker(addr, group);
        for (const std::string& topic : part.topicList) {
            if (merged.contains(topic)) continue;
            merged.topicList.push_back(topic);
        }
    }
    return merged;
}

JsonValue DefaultMQAdminExt::querySubscription(const std::string& brokerAddr,
                                              const std::string& group,
                                              const std::string& topic) {
    PropertyMap ext;
    ext["group"] = group;
    ext["topic"] = topic;
    RemotingCommand response = requireClient().invokeSync(
        vipAddr(brokerAddr), RequestCode::QUERY_SUBSCRIPTION_BY_CONSUMER, ext, Bytes(), false,
        timeoutMillis_);
    JsonValue v;
    if (response.body.empty()) return JsonValue::makeNull();
    RemotingSerializable::decode(response.body, v);
    return v;
}

JsonValue DefaultMQAdminExt::getConsumeStatus(const std::string& brokerAddr,
                                             const std::string& topic, const std::string& group,
                                             const std::string& clientAddr) {
    PropertyMap ext;
    ext["topic"] = topic;
    ext["group"] = group;
    ext["clientAddr"] = clientAddr;
    RemotingCommand response = requireClient().invokeSync(
        vipAddr(brokerAddr), RequestCode::INVOKE_BROKER_TO_GET_CONSUMER_STATUS, ext, Bytes(), false,
        timeoutMillis_);
    JsonValue v;
    if (response.body.empty()) return JsonValue::makeNull();
    if (!RemotingSerializable::decode(response.body, v)) return JsonValue::makeNull();
    return v.get("consumerTable");
}

void DefaultMQAdminExt::cloneGroupOffset(const std::string& brokerAddr,
                                        const std::string& srcGroup,
                                        const std::string& destGroup, const std::string& topic,
                                        bool offline) {
    PropertyMap ext;
    ext["srcGroup"] = srcGroup;
    ext["destGroup"] = destGroup;
    ext["topic"] = topic;
    ext["offline"] = offline ? "true" : "false";
    requireClient().invokeSync(vipAddr(brokerAddr), RequestCode::CLONE_GROUP_OFFSET, ext, Bytes(),
                               false, timeoutMillis_);
}

// ---------------------------------------------------------------- Offset management
int64_t DefaultMQAdminExt::maxOffset(const MessageQueue& mq) {
    return requireClient().getMaxOffset(mq);
}

int64_t DefaultMQAdminExt::minOffset(const MessageQueue& mq) {
    return requireClient().getMinOffset(mq);
}

int64_t DefaultMQAdminExt::searchOffset(const MessageQueue& mq, int64_t timestamp) {
    return searchLowerBoundaryOffset(mq, timestamp);
}

int64_t DefaultMQAdminExt::searchLowerBoundaryOffset(const MessageQueue& mq, int64_t timestamp) {
    return requireClient().searchOffsetByBoundary(mq, timestamp, BoundaryType::LOWER);
}

int64_t DefaultMQAdminExt::searchUpperBoundaryOffset(const MessageQueue& mq, int64_t timestamp) {
    return requireClient().searchOffsetByBoundary(mq, timestamp, BoundaryType::UPPER);
}

int64_t DefaultMQAdminExt::earliestMsgStoreTime(const MessageQueue& mq) {
    MQClientInstance& client = requireClient();
    std::string addr = client.publishAddrFor(mq.brokerName, mq.topic);
    PropertyMap ext;
    ext["topic"] = mq.topic;
    ext["queueId"] = i64str(mq.queueId);
    ext["brokerName"] = mq.brokerName;
    RemotingCommand response = client.invokeSync(vipAddr(addr), RequestCode::GET_EARLIEST_MSG_STORETIME,
                                                 ext, Bytes(), false, timeoutMillis_);
    return extInt(response, "timestamp", 0);
}

bool DefaultMQAdminExt::examineConsumerOffset(const std::string& consumerGroup,
                                             const MessageQueue& mq, int64_t& outOffset) {
    return requireClient().queryConsumerOffset(consumerGroup, mq, outOffset);
}

void DefaultMQAdminExt::updateConsumerOffset(const std::string& consumerGroup,
                                            const MessageQueue& mq, int64_t offset) {
    requireClient().updateConsumerOffset(consumerGroup, mq, offset);
}

void DefaultMQAdminExt::updateConsumerOffsetToBroker(const std::string& brokerAddr,
                                                    const std::string& consumerGroup,
                                                    const MessageQueue& mq, int64_t offset) {
    requireClient().updateConsumerOffset(consumerGroup, mq, offset, 5000, brokerAddr);
}

PropertyMap DefaultMQAdminExt::buildResetOffsetExtFields(
    const std::string& topic, const std::string& group, int64_t timestamp, bool isForce,
    int32_t queueId, int64_t offset) {
    PropertyMap ext;
    ext["topic"] = topic;
    ext["group"] = group;
    ext["timestamp"] = i64str(timestamp);
    ext["isForce"] = isForce ? "true" : "false";
    ext["offset"] = i64str(offset);
    if (queueId >= 0) ext["queueId"] = i64str(queueId);
    return ext;
}

std::map<MessageQueue, int64_t> DefaultMQAdminExt::invokeBrokerToResetOffset(
    const std::string& brokerAddr, const std::string& topic, const std::string& group,
    int64_t timestamp, bool isForce, bool isCpp, int32_t queueId, int64_t offset) {
    MQClientInstance& client = requireClient();
    PropertyMap ext = buildResetOffsetExtFields(topic, group, timestamp, isForce, queueId, offset);
    RemotingCommand response = client.invokeSyncRaw(
        vipAddr(brokerAddr), RequestCode::INVOKE_BROKER_TO_RESET_OFFSET, ext, Bytes(), false,
        timeoutMillis_,
        isCpp ? LanguageCode::CPP : -1);
    if (response.code != ResponseCode::SUCCESS) {
        throw MQClientException(response.remark.empty() ? "reset offset failed" : response.remark,
                                response.code);
    }
    std::map<MessageQueue, int64_t> offsets;
    if (!response.body.empty()) {
        ResetOffsetBody body;
        if (ResetOffsetBody::decode(response.body, body)) {
            for (const auto& kv : body.offsetTable) offsets[kv.first] = kv.second;
        }
    }
    return offsets;
}

std::map<MessageQueue, int64_t> DefaultMQAdminExt::resetOffsetByTimestamp(
    const std::string& topic, const std::string& group, int64_t timestamp, bool isForce,
    const std::string& clusterName, bool isCpp) {
    // This does **not** use the old local implementation of "per-queue searchOffset + updateConsumerOffset" --
    // that would neither sync online consumers nor run broker-side consistency validation.
    std::string routeTopic = topic;
    std::string wheelTimer = std::string(MixAll::SYSTEM_TOPIC_PREFIX) + "wheel_timer";
    if (!topic.empty() && (MixAll::isLmq(topic) || topic == wheelTimer) && !clusterName.empty()) {
        routeTopic = clusterName;
    }
    TopicRouteData route = examineTopicRoute(routeTopic);

    std::map<MessageQueue, int64_t> allOffsets;
    for (const BrokerData& bd : route.brokerDatas) {
        std::string addr = bd.selectBrokerAddr();
        if (addr.empty()) continue;
        for (const auto& kv : invokeBrokerToResetOffset(
                 addr, topic, group, timestamp, isForce, isCpp, -1, -1)) {
            allOffsets[kv.first] = kv.second;
        }
    }
    if (allOffsets.empty()) {
        throw MQClientException("reset offset failed, no broker returned offset table");
    }
    return allOffsets;
}

std::map<MessageQueue, int64_t> DefaultMQAdminExt::resetOffsetByQueueId(
    const std::string& brokerAddr, const std::string& consumerGroup, const std::string& topic,
    int32_t queueId, int64_t resetOffset) {
    updateConsumerOffsetToBroker(brokerAddr, consumerGroup,
                                 MessageQueue(topic, "", queueId), resetOffset);
    return invokeBrokerToResetOffset(brokerAddr, topic, consumerGroup, 0, false, false,
                                     queueId, resetOffset);
}

void DefaultMQAdminExt::resetOffsetNew(const std::string& consumerGroup,
                                      const std::string& topic, int64_t timestamp) {
    try {
        resetOffsetByTimestamp(topic, consumerGroup, timestamp, true);
    } catch (const MQClientException& e) {
        if (e.getResponseCode() == ResponseCode::CONSUMER_NOT_ONLINE) {
            resetOffsetByTimestampOld(consumerGroup, topic, timestamp, true);
            return;
        }
        throw;
    }
}

std::map<MessageQueue, int64_t> DefaultMQAdminExt::resetOffsetByTimestampOld(
    const std::string& consumerGroup, const std::string& topic, int64_t timestamp, bool force) {
    MQClientInstance& client = requireClient();
    TopicRouteData route = examineTopicRoute(topic);
    std::map<MessageQueue, int64_t> result;
    for (const BrokerData& bd : route.brokerDatas) {
        std::string addr = bd.selectBrokerAddr();
        if (addr.empty()) continue;
        for (const QueueData& qd : route.queueDatas) {
            if (qd.brokerName != bd.brokerName) continue;
            for (int32_t queueId = 0; queueId < qd.readQueueNums; ++queueId) {
                MessageQueue mq(topic, bd.brokerName, queueId);
                int64_t consumerOffset = 0;
                try {
                    client.queryConsumerOffset(consumerGroup, mq, consumerOffset, 5000, addr);
                } catch (const std::exception&) {
                    consumerOffset = 0;
                }
                int64_t resetOffset = 0;
                if (timestamp == -1) {
                    resetOffset = client.getMaxOffset(mq, 5000, addr);
                } else {
                    resetOffset = client.searchOffsetByTimestamp(mq, timestamp, 5000, addr);
                }
                if (force || resetOffset <= consumerOffset) {
                    client.updateConsumerOffset(consumerGroup, mq, resetOffset, 5000, addr);
                    result[mq] = resetOffset;
                }
            }
        }
    }
    return result;
}

// ---------------------------------------------------------------- Message query
std::vector<MessageExt> DefaultMQAdminExt::queryMessage(const std::string& topic,
                                                        const std::string& key, int32_t maxNum,
                                                        int64_t begin, int64_t end) {
    return requireClient().queryMessageAllBrokers(topic, key, maxNum, begin, end,
                                                  MessageConst::INDEX_KEY_TYPE, false);
}

bool DefaultMQAdminExt::queryMessageByUniqKey(const std::string& topic,
                                             const std::string& uniqKey, MessageExt& out) {
    int64_t now = UtilAll::currentTimeMillis();
    std::vector<MessageExt> msgs = requireClient().queryMessageAllBrokers(
        topic, uniqKey, 32, 0, now + 3600 * 1000, MessageConst::INDEX_UNIQUE_TYPE, true);
    if (msgs.empty()) return false;
    out = msgs[0];
    return true;
}

std::vector<MessageExt> DefaultMQAdminExt::queryMessageByKey(const std::string& topic,
                                                            const std::string& key,
                                                            int32_t maxNum) {
    int64_t now = UtilAll::currentTimeMillis();
    return requireClient().queryMessageAllBrokers(topic, key, maxNum, 0, now + 3600 * 1000,
                                                  MessageConst::INDEX_KEY_TYPE, false);
}

MessageExt DefaultMQAdminExt::viewMessage(const std::string& topic, const std::string& msgId) {
    std::string byIdError;
    try {
        std::string ip;
        int32_t port = 0;
        int64_t offset = 0;
        if (!decodeMessageId(msgId, ip, port, offset) || port <= 0 || port > 65535) {
            throw MQClientException("not a valid offset msgId: " + msgId,
                                    ResponseCode::NO_MESSAGE);
        }
        std::string addr = ip + ":" + std::to_string(port);
        PropertyMap ext;
        ext["topic"] = topic;
        ext["offset"] = i64str(offset);
        RemotingCommand response = requireClient().invokeSync(
            vipAddr(addr), RequestCode::VIEW_MESSAGE_BY_ID, ext, Bytes(), false, timeoutMillis_);
        if (response.body.empty()) {
            throw MQBrokerException(ResponseCode::NO_MESSAGE, "message not found: " + msgId);
        }
        std::vector<MessageExt> msgs = decodeMessages(response.body, true);
        if (msgs.empty()) {
            throw MQBrokerException(ResponseCode::NO_MESSAGE, "message not found: " + msgId);
        }
        return msgs[0];
    } catch (const std::exception& e) {
        byIdError = e.what();
    }

    MessageExt found;
    if (queryMessageByUniqKey(topic, msgId, found)) {
        return found;
    }
    throw MQClientException(
        "viewMessage failed: neither offset msgId nor uniq key matched message " + msgId
        + " of " + topic + ", last error: " + byIdError, ResponseCode::NO_MESSAGE);
}

QueryConsumeQueueResponseBody DefaultMQAdminExt::queryConsumeQueue(
    const std::string& brokerAddr, const std::string& topic, int32_t queueId, int64_t index,
    int32_t count, const std::string& consumerGroup) {
    PropertyMap ext;
    ext["topic"] = topic;
    ext["queueId"] = i64str(queueId);
    ext["index"] = i64str(index);
    ext["count"] = i64str(count);
    ext["consumerGroup"] = consumerGroup;
    RemotingCommand response = requireClient().invokeSync(
        vipAddr(brokerAddr), RequestCode::QUERY_CONSUME_QUEUE, ext, Bytes(), false, timeoutMillis_);
    QueryConsumeQueueResponseBody body;
    if (!response.body.empty()) QueryConsumeQueueResponseBody::decode(response.body, body);
    return body;
}

// ---------------------------------------- Direct consume probe (309)
PropertyMap DefaultMQAdminExt::buildConsumeMessageDirectlyExtFields(
    const std::string& topic, const std::string& consumerGroup, const std::string& clientId,
    const std::string& msgId) {
    PropertyMap ext;
    ext["topic"] = topic;
    ext["consumerGroup"] = consumerGroup;
    ext["clientId"] = clientId;
    ext["msgId"] = msgId;
    return ext;
}

ConsumeMessageDirectlyResult DefaultMQAdminExt::consumeMessageDirectly(
    const std::string& brokerAddr, const std::string& consumerGroup, const std::string& clientId,
    const std::string& topic, const std::string& msgId, int32_t timeoutMillis) {
    if (consumerGroup.empty()) {
        throw MQClientException("consumeMessageDirectly: consumerGroup required");
    }
    PropertyMap ext = buildConsumeMessageDirectlyExtFields(topic, consumerGroup, clientId, msgId);
    RemotingCommand response = requireClient().invokeSyncRaw(
        vipAddr(brokerAddr), RequestCode::CONSUME_MESSAGE_DIRECTLY, ext, Bytes(), false,
        timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis);
    if (response.code != ResponseCode::SUCCESS) {
        throw MQClientException(
            response.remark.empty() ? "consume message directly failed" : response.remark,
            response.code);
    }
    if (response.body.empty()) {
        throw MQClientException("no consume result for client " + clientId);
    }
    ConsumeMessageDirectlyResult result;
    if (!ConsumeMessageDirectlyResult::decode(response.body, result)) {
        throw MQClientException("consumeMessageDirectly: bad ConsumeMessageDirectlyResult body");
    }
    return result;
}

ConsumeMessageDirectlyResult DefaultMQAdminExt::consumeMessageDirectly(
    const std::string& consumerGroup, const std::string& clientId, const std::string& topic,
    const std::string& msgId) {
    MessageExt msg = viewMessage(topic, msgId);
    std::string outMsgId = msgId;
    if (!msg.getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX).empty()
        && !msg.getOffsetMsgId().empty()) {
        outMsgId = msg.getOffsetMsgId();
    }
    return consumeMessageDirectly(msg.getStoreHostString(), consumerGroup, clientId, topic,
                                  outMsgId);
}

// ---------------------------------------- AUTH user management (3001-3005)
// Deliberately not routed through vipAddr: the caller-supplied address is used verbatim.
void DefaultMQAdminExt::createUser(const std::string& brokerAddr, const UserInfo& user) {
    if (user.username.empty()) {
        throw MQClientException("createUser: username required");
    }
    PropertyMap ext;
    ext["username"] = user.username;
    requireClient().invokeSync(brokerAddr, RequestCode::AUTH_CREATE_USER, ext,
                               RemotingSerializable::encode(user.toJson()), true, timeoutMillis_);
}

void DefaultMQAdminExt::createUser(const std::string& brokerAddr, const std::string& username,
                                   const std::string& password, const std::string& userType) {
    UserInfo user;
    user.username = username;
    user.password = password;
    user.hasPassword = true;
    user.userType = userType;
    user.hasUserType = true;
    createUser(brokerAddr, user);
}

void DefaultMQAdminExt::updateUser(const std::string& brokerAddr, const UserInfo& user) {
    if (user.username.empty()) {
        throw MQClientException("updateUser: username required");
    }
    PropertyMap ext;
    ext["username"] = user.username;
    requireClient().invokeSync(brokerAddr, RequestCode::AUTH_UPDATE_USER, ext,
                               RemotingSerializable::encode(user.toJson()), true, timeoutMillis_);
}

void DefaultMQAdminExt::updateUser(const std::string& brokerAddr, const std::string& username,
                                   const std::string& password, const std::string& userType) {
    UserInfo user;
    user.username = username;
    user.password = password;
    user.hasPassword = true;
    user.userType = userType;
    user.hasUserType = true;
    updateUser(brokerAddr, user);
}

void DefaultMQAdminExt::deleteUser(const std::string& brokerAddr, const std::string& username) {
    if (username.empty()) {
        throw MQClientException("deleteUser: username required");
    }
    PropertyMap ext;
    ext["username"] = username;
    requireClient().invokeSync(brokerAddr, RequestCode::AUTH_DELETE_USER, ext, Bytes(), false,
                               timeoutMillis_);
}

UserInfo DefaultMQAdminExt::getUser(const std::string& brokerAddr, const std::string& username) {
    if (username.empty()) {
        throw MQClientException("getUser: username required");
    }
    PropertyMap ext;
    ext["username"] = username;
    RemotingCommand response = requireClient().invokeSync(
        brokerAddr, RequestCode::AUTH_GET_USER, ext, Bytes(), false, timeoutMillis_);
    UserInfo out;
    if (!response.body.empty()) UserInfo::decode(response.body, out);
    return out;
}

std::vector<UserInfo> DefaultMQAdminExt::listUser(const std::string& brokerAddr,
                                                  const std::optional<std::string>& filter) {
    PropertyMap ext;
    if (filter.has_value()) ext["filter"] = *filter;
    RemotingCommand response = requireClient().invokeSync(
        brokerAddr, RequestCode::AUTH_LIST_USER, ext, Bytes(), false, timeoutMillis_);
    std::vector<UserInfo> out;
    if (!response.body.empty()) {
        JsonValue v;
        if (RemotingSerializable::decode(response.body, v)) out = UserInfo::listFromJson(v);
    }
    return out;
}

// ---------------------------------------- AUTH ACL management (3006-3010)
namespace {

// One policy, policyType left unset, one entry per resource repeating the shared grant fields
AclInfo buildSinglePolicyAcl(const std::string& subject, const std::vector<std::string>& resources,
                             const std::vector<std::string>& actions,
                             const std::vector<std::string>& sourceIps,
                             const std::string& decision) {
    AclInfo acl;
    acl.subject = subject;
    AclPolicy policy;
    for (const std::string& resource : resources) {
        AclEntry entry;
        entry.resource = resource;
        entry.actions = actions;
        entry.hasActions = true;
        entry.sourceIps = sourceIps;
        entry.hasSourceIps = true;
        entry.decision = decision;
        entry.hasDecision = true;
        policy.entries.push_back(entry);
    }
    acl.policies.push_back(policy);
    return acl;
}

}  // namespace

void DefaultMQAdminExt::createAcl(const std::string& brokerAddr, const AclInfo& acl) {
    if (acl.subject.empty()) {
        throw MQClientException("createAcl: subject required");
    }
    PropertyMap ext;
    ext["subject"] = acl.subject;
    requireClient().invokeSync(brokerAddr, RequestCode::AUTH_CREATE_ACL, ext,
                               RemotingSerializable::encode(acl.toJson()), true, timeoutMillis_);
}

void DefaultMQAdminExt::createAcl(const std::string& brokerAddr, const std::string& subject,
                                  const std::vector<std::string>& resources,
                                  const std::vector<std::string>& actions,
                                  const std::vector<std::string>& sourceIps,
                                  const std::string& decision) {
    createAcl(brokerAddr, buildSinglePolicyAcl(subject, resources, actions, sourceIps, decision));
}

void DefaultMQAdminExt::updateAcl(const std::string& brokerAddr, const AclInfo& acl) {
    if (acl.subject.empty()) {
        throw MQClientException("updateAcl: subject required");
    }
    PropertyMap ext;
    ext["subject"] = acl.subject;
    requireClient().invokeSync(brokerAddr, RequestCode::AUTH_UPDATE_ACL, ext,
                               RemotingSerializable::encode(acl.toJson()), true, timeoutMillis_);
}

void DefaultMQAdminExt::updateAcl(const std::string& brokerAddr, const std::string& subject,
                                  const std::vector<std::string>& resources,
                                  const std::vector<std::string>& actions,
                                  const std::vector<std::string>& sourceIps,
                                  const std::string& decision) {
    updateAcl(brokerAddr, buildSinglePolicyAcl(subject, resources, actions, sourceIps, decision));
}

void DefaultMQAdminExt::deleteAcl(const std::string& brokerAddr, const std::string& subject,
                                  const std::string& resource) {
    if (subject.empty()) {
        throw MQClientException("deleteAcl: subject required");
    }
    PropertyMap ext;
    ext["subject"] = subject;
    if (!resource.empty()) ext["resource"] = resource;
    requireClient().invokeSync(brokerAddr, RequestCode::AUTH_DELETE_ACL, ext, Bytes(), false,
                               timeoutMillis_);
}

AclInfo DefaultMQAdminExt::getAcl(const std::string& brokerAddr, const std::string& subject) {
    if (subject.empty()) {
        throw MQClientException("getAcl: subject required");
    }
    PropertyMap ext;
    ext["subject"] = subject;
    RemotingCommand response = requireClient().invokeSync(
        brokerAddr, RequestCode::AUTH_GET_ACL, ext, Bytes(), false, timeoutMillis_);
    AclInfo out;
    if (!response.body.empty()) AclInfo::decode(response.body, out);
    return out;
}

std::vector<AclInfo> DefaultMQAdminExt::listAcl(const std::string& brokerAddr,
                                                const std::optional<std::string>& subjectFilter,
                                                const std::optional<std::string>& resourceFilter) {
    PropertyMap ext;
    if (subjectFilter.has_value()) ext["subjectFilter"] = *subjectFilter;
    if (resourceFilter.has_value()) ext["resourceFilter"] = *resourceFilter;
    RemotingCommand response = requireClient().invokeSync(
        brokerAddr, RequestCode::AUTH_LIST_ACL, ext, Bytes(), false, timeoutMillis_);
    std::vector<AclInfo> out;
    if (!response.body.empty()) {
        JsonValue v;
        if (RemotingSerializable::decode(response.body, v)) out = AclInfo::listFromJson(v);
    }
    return out;
}

// ---------------------------------------------------------------- Container management (902/903)
void DefaultMQAdminExt::addBroker(const std::string& containerAddr, const std::string& configPath) {
    // The container address travels verbatim (no VIP rewrite) with the admin default timeout.
    PropertyMap ext;
    if (!configPath.empty()) ext["configPath"] = configPath;
    RemotingCommand response = requireClient().invokeSyncRaw(
        containerAddr, RequestCode::ADD_BROKER, ext, Bytes(), false, DEFAULT_TIMEOUT);
    if (response.code != ResponseCode::SUCCESS) {
        throw MQBrokerException(response.code, response.remark);
    }
}

void DefaultMQAdminExt::removeBroker(const std::string& containerAddr,
                                     const std::string& brokerClusterName,
                                     const std::string& brokerName, int64_t brokerId) {
    PropertyMap ext;
    ext["brokerName"] = brokerName;
    ext["brokerClusterName"] = brokerClusterName;
    ext["brokerId"] = i64str(brokerId);
    RemotingCommand response = requireClient().invokeSyncRaw(
        containerAddr, RequestCode::REMOVE_BROKER, ext, Bytes(), false, DEFAULT_TIMEOUT);
    if (response.code != ResponseCode::SUCCESS) {
        throw MQBrokerException(response.code, response.remark);
    }
}

// ---------------------------------------------------------------- Controller administration
namespace {

std::string extStr(const RemotingCommand& r, const char* key) {
    auto it = r.extFields.find(key);
    return it == r.extFields.end() ? std::string() : it->second;
}

}  // namespace

ControllerMetaData DefaultMQAdminExt::getControllerMetaData(const std::string& controllerAddr) {
    RemotingCommand response = requireClient().invokeSyncRaw(
        controllerAddr, RequestCode::CONTROLLER_GET_METADATA_INFO, PropertyMap(), Bytes(), false,
        3000);
    if (response.code != ResponseCode::SUCCESS) {
        throw MQBrokerException(response.code, response.remark);
    }
    ControllerMetaData meta;
    meta.group = extStr(response, "group");
    meta.controllerLeaderId = extStr(response, "controllerLeaderId");
    meta.controllerLeaderAddress = extStr(response, "controllerLeaderAddress");
    meta.isLeader = extStr(response, "isLeader") == "true";
    meta.peers = extStr(response, "peers");
    return meta;
}

BrokerReplicasInfo DefaultMQAdminExt::getInSyncStateData(
    const std::string& controllerAddr, const std::vector<std::string>& brokers) {
    // Two-phase: ask any controller node for the metadata first, then the sync state from the leader.
    ControllerMetaData meta = getControllerMetaData(controllerAddr);
    if (meta.controllerLeaderAddress.empty()) {
        throw MQClientException("controller leader address is not available");
    }
    JsonValue arr = JsonValue::makeArray();
    for (const std::string& b : brokers) arr.pushArray(JsonValue::makeString(b));
    RemotingCommand response = requireClient().invokeSyncRaw(
        meta.controllerLeaderAddress, RequestCode::CONTROLLER_GET_SYNC_STATE_DATA, PropertyMap(),
        RemotingSerializable::encode(arr), true, 3000);
    if (response.code != ResponseCode::SUCCESS) {
        throw MQBrokerException(response.code, response.remark);
    }
    BrokerReplicasInfo info;
    if (!response.body.empty()) BrokerReplicasInfo::decode(response.body, info);
    return info;
}

std::map<std::string, PropertyMap> DefaultMQAdminExt::getControllerConfig(
    const std::vector<std::string>& controllerServers, int32_t timeoutMillis) {
    int32_t wait = timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis;
    std::vector<std::string> targets = controllerServers;
    if (targets.empty()) targets = requireClient().nameServerAddrs();
    if (targets.empty()) return {};
    std::map<std::string, PropertyMap> configMap;
    for (const std::string& controller : targets) {
        RemotingCommand response = requireClient().invokeSyncRaw(
            controller, RequestCode::GET_CONTROLLER_CONFIG, PropertyMap(), Bytes(), false, wait);
        if (response.code != ResponseCode::SUCCESS) {
            throw MQClientException(response.remark, response.code);
        }
        configMap[controller] = MixAll::string2Properties(
            std::string(response.body.begin(), response.body.end()));
    }
    return configMap;
}

void DefaultMQAdminExt::updateControllerConfig(const PropertyMap& properties,
                                               const std::vector<std::string>& controllers,
                                               int32_t timeoutMillis) {
    int32_t wait = timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis;
    std::string text = MixAll::properties2String(properties);
    if (text.empty() || controllers.empty()) return;
    bool hasError = false;
    int32_t errCode = 0;
    std::string errRemark;
    for (const std::string& controller : controllers) {
        RemotingCommand response = requireClient().invokeSyncRaw(
            controller, RequestCode::UPDATE_CONTROLLER_CONFIG, PropertyMap(),
            Bytes(text.begin(), text.end()), true, wait);
        if (response.code != ResponseCode::SUCCESS) {
            // every server gets the request; the last rejected one decides the exception
            hasError = true;
            errCode = response.code;
            errRemark = response.remark;
        }
    }
    if (hasError) throw MQClientException(errRemark, errCode);
}

void DefaultMQAdminExt::cleanBrokerData(const std::string& controllerAddr,
                                        const std::string& clusterName,
                                        const std::string& brokerName,
                                        const std::string& brokerControllerIdsToClean,
                                        bool isCleanLivingBroker) {
    // Two-phase like the sync-state read: metadata first, then the clean request to the leader.
    ControllerMetaData meta = getControllerMetaData(controllerAddr);
    if (meta.controllerLeaderAddress.empty()) {
        throw MQClientException("controller leader address is not available");
    }
    PropertyMap ext;
    if (!clusterName.empty()) ext["clusterName"] = clusterName;
    ext["brokerName"] = brokerName;
    if (!brokerControllerIdsToClean.empty()) {
        ext["brokerControllerIdsToClean"] = brokerControllerIdsToClean;
    }
    ext["isCleanLivingBroker"] = isCleanLivingBroker ? "true" : "false";
    ext["invokeTime"] = i64str(UtilAll::currentTimeMillis());
    RemotingCommand response = requireClient().invokeSyncRaw(
        meta.controllerLeaderAddress, RequestCode::CLEAN_BROKER_DATA, ext, Bytes(), false, 3000);
    if (response.code != ResponseCode::SUCCESS) {
        throw MQBrokerException(response.code, response.remark);
    }
}

// ---------------------------------------------------------------- Cold-data flow control
void DefaultMQAdminExt::updateColdDataFlowCtrGroupConfig(const std::string& brokerAddr,
                                                         const PropertyMap& properties) {
    std::string text = MixAll::properties2String(properties);
    if (text.empty()) return;
    RemotingCommand response = requireClient().invokeSyncRaw(
        vipAddr(brokerAddr), RequestCode::UPDATE_COLD_DATA_FLOW_CTR_CONFIG, PropertyMap(),
        Bytes(text.begin(), text.end()), true, timeoutMillis_);
    if (response.code != ResponseCode::SUCCESS) {
        throw MQBrokerException(response.code, response.remark);
    }
}

void DefaultMQAdminExt::removeColdDataFlowCtrGroupConfig(const std::string& brokerAddr,
                                                         const std::string& consumerGroup) {
    if (consumerGroup.empty()) return;
    RemotingCommand response = requireClient().invokeSyncRaw(
        vipAddr(brokerAddr), RequestCode::REMOVE_COLD_DATA_FLOW_CTR_CONFIG, PropertyMap(),
        Bytes(consumerGroup.begin(), consumerGroup.end()), true, timeoutMillis_);
    if (response.code != ResponseCode::SUCCESS) {
        throw MQBrokerException(response.code, response.remark);
    }
}

std::string DefaultMQAdminExt::getColdDataFlowCtrInfo(const std::string& brokerAddr) {
    RemotingCommand response = requireClient().invokeSyncRaw(
        brokerAddr, RequestCode::GET_COLD_DATA_FLOW_CTR_INFO, PropertyMap(), Bytes(), false,
        timeoutMillis_);
    if (response.code != ResponseCode::SUCCESS) {
        throw MQBrokerException(response.code, response.remark);
    }
    return std::string(response.body.begin(), response.body.end());
}

// ---------------------------------------------------------------- CommitLog read-ahead / pop rollback
std::string DefaultMQAdminExt::setCommitLogReadAheadMode(const std::string& brokerAddr,
                                                         const std::string& mode) {
    PropertyMap ext;
    ext["READ_AHEAD_MODE"] = mode;
    RemotingCommand response = requireClient().invokeSyncRaw(
        brokerAddr, RequestCode::SET_COMMITLOG_READ_MODE, ext, Bytes(), false, timeoutMillis_);
    if (response.code != ResponseCode::SUCCESS) {
        throw MQBrokerException(response.code, response.remark);
    }
    return response.remark;
}

void DefaultMQAdminExt::exportPopRecords(const std::string& brokerAddr, int32_t timeoutMillis) {
    RemotingCommand response = requireClient().invokeSyncRaw(
        brokerAddr, RequestCode::POP_ROLLBACK, PropertyMap(), Bytes(), false, timeoutMillis);
    if (response.code != ResponseCode::SUCCESS) {
        throw MQBrokerException(response.code, response.remark);
    }
}

// ---------------------------------------------------------------- RocksDB maintenance (354/355)
CheckRocksdbCqWriteResult DefaultMQAdminExt::checkRocksdbCqWriteProgress(
    const std::string& brokerAddr, const std::string& topic, int64_t checkStoreTime) {
    PropertyMap ext;
    ext["topic"] = topic;
    ext["checkStoreTime"] = i64str(checkStoreTime);
    RemotingCommand response = requireClient().invokeSyncRaw(
        brokerAddr, RequestCode::CHECK_ROCKSDB_CQ_WRITE_PROGRESS, ext, Bytes(), false,
        timeoutMillis_);
    if (response.code != ResponseCode::SUCCESS) {
        throw MQClientException(response.remark, response.code);
    }
    CheckRocksdbCqWriteResult result;
    if (!response.body.empty()) CheckRocksdbCqWriteResult::decode(response.body, result);
    return result;
}

void DefaultMQAdminExt::exportRocksDBConfigToJson(const std::string& brokerAddr,
                                                  const std::vector<std::string>& configTypes) {
    // the separator is written after every entry, so a single type still carries a trailing ';'
    std::string configType;
    for (const std::string& t : configTypes) configType += t + ";";
    PropertyMap ext;
    ext["configType"] = configType;
    RemotingCommand response = requireClient().invokeSyncRaw(
        brokerAddr, RequestCode::EXPORT_ROCKSDB_CONFIG_TO_JSON, ext, Bytes(), false,
        timeoutMillis_);
    if (response.code != ResponseCode::SUCCESS) {
        throw MQClientException(response.remark, response.code);
    }
}

// ---------------------------------------------------------------- Lite topic administration
JsonValue DefaultMQAdminExt::getBrokerLiteInfo(const std::string& brokerAddr) {
    return invokeBrokerMethod(brokerAddr, RequestCode::GET_BROKER_LITE_INFO, PropertyMap(),
                              timeoutMillis_);
}

JsonValue DefaultMQAdminExt::getParentTopicInfo(const std::string& brokerAddr,
                                                const std::string& topic) {
    PropertyMap ext;
    ext["topic"] = topic;
    return invokeBrokerMethod(brokerAddr, RequestCode::GET_PARENT_TOPIC_INFO, ext, timeoutMillis_);
}

JsonValue DefaultMQAdminExt::getLiteTopicInfo(const std::string& brokerAddr,
                                              const std::string& parentTopic,
                                              const std::string& liteTopic) {
    PropertyMap ext;
    ext["parentTopic"] = parentTopic;
    if (!liteTopic.empty()) ext["liteTopic"] = liteTopic;
    return invokeBrokerMethod(brokerAddr, RequestCode::GET_LITE_TOPIC_INFO, ext, timeoutMillis_);
}

JsonValue DefaultMQAdminExt::getLiteClientInfo(const std::string& brokerAddr,
                                               const std::string& parentTopic,
                                               const std::string& group,
                                               const std::string& clientId, int32_t maxCount) {
    PropertyMap ext;
    ext["parentTopic"] = parentTopic;
    ext["group"] = group;
    if (!clientId.empty()) ext["clientId"] = clientId;
    ext["maxCount"] = i64str(maxCount);
    return invokeBrokerMethod(brokerAddr, RequestCode::GET_LITE_CLIENT_INFO, ext, timeoutMillis_);
}

JsonValue DefaultMQAdminExt::getLiteGroupInfo(const std::string& brokerAddr,
                                              const std::string& group,
                                              const std::string& liteTopic, int32_t topK) {
    PropertyMap ext;
    ext["group"] = group;
    if (!liteTopic.empty()) ext["liteTopic"] = liteTopic;
    ext["topK"] = i64str(topK);
    return invokeBrokerMethod(brokerAddr, RequestCode::GET_LITE_GROUP_INFO, ext, timeoutMillis_);
}

void DefaultMQAdminExt::triggerLiteDispatch(const std::string& brokerAddr,
                                            const std::string& group,
                                            const std::string& clientId) {
    PropertyMap ext;
    ext["group"] = group;
    if (!clientId.empty()) ext["clientId"] = clientId;
    invokeBrokerMethod(brokerAddr, RequestCode::TRIGGER_LITE_DISPATCH, ext, timeoutMillis_);
}

// ---------------------------------------------------------------- Concurrent fan-out forms
AdminToolResult<TopicStatsTable> DefaultMQAdminExt::examineTopicStatsConcurrent(
    const std::string& topic, int32_t timeoutMillis) {
    int32_t wait = timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis;
    return adminToolExecute<TopicStatsTable>(
        [&]() -> AdminToolResult<TopicStatsTable> {
            TopicRouteData route;
            if (!examineTopicRouteOrNull(topic, route) || route.brokerDatas.empty()) {
                return adminSuccess<TopicStatsTable>(TopicStatsTable());
            }
            TopicStatsTable merged;
            std::mutex mergeMutex;
            runBoundedFanOut(route.brokerDatas.size(), wait, [&](size_t i) {
                try {
                    const std::string addr = route.brokerDatas[i].selectBrokerAddr();
                    if (addr.empty()) return;
                    PropertyMap ext;
                    ext["topic"] = topic;
                    RemotingCommand response = requireClient().invokeSync(
                        vipAddr(addr), RequestCode::GET_TOPIC_STATS_INFO, ext, Bytes(), false,
                        wait);
                    TopicStatsTable part;
                    if (!response.body.empty()) TopicStatsTable::decode(response.body, part);
                    std::lock_guard<std::mutex> lk(mergeMutex);
                    for (const auto& kv : part.offsetTable) {
                        merged.offsetTable[kv.first] = kv.second;
                    }
                    merged.topicPutTps += part.topicPutTps;
                } catch (const std::exception& e) {
                    logger_warn("getTopicStatsInfo error. topic=" + topic + ": "
                                + std::string(e.what()));
                }
            });
            return adminSuccess<TopicStatsTable>(merged);
        });
}

AdminToolResult<ConsumeStats> DefaultMQAdminExt::examineConsumeStatsConcurrent(
    const std::string& consumerGroup, const std::string& topic, int32_t timeoutMillis) {
    int32_t wait = timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis;
    return adminToolExecute<ConsumeStats>(
        [&]() -> AdminToolResult<ConsumeStats> {
            std::vector<std::string> routeTopics;
            routeTopics.push_back(MixAll::getRetryTopic(consumerGroup));
            if (!topic.empty()) {
                routeTopics.push_back(topic);
                routeTopics.push_back(extra_info::buildPopRetryTopicV1(topic, consumerGroup));
            }
            bool haveRoute = false;
            TopicRouteData route;
            for (const std::string& rt : routeTopics) {
                try {
                    if (examineTopicRouteOrNull(rt, route)) {
                        haveRoute = true;
                        break;
                    }
                } catch (const std::exception&) {
                    continue;
                }
            }
            if (!haveRoute || route.brokerDatas.empty()) {
                return adminFailure<ConsumeStats>(AdminToolResultCode::TOPIC_ROUTE_INFO_NOT_EXIST,
                                                  "topic router info not found");
            }
            ConsumeStats result;
            std::map<std::string, double> consumerTpsMap;
            std::mutex mergeMutex;
            runBoundedFanOut(route.brokerDatas.size(), wait, [&](size_t i) {
                try {
                    const std::string addr = route.brokerDatas[i].selectBrokerAddr();
                    if (addr.empty()) return;
                    PropertyMap ext;
                    ext["consumerGroup"] = consumerGroup;
                    if (!topic.empty()) ext["topic"] = topic;
                    RemotingCommand response = requireClient().invokeSync(
                        vipAddr(addr), RequestCode::GET_CONSUME_STATS, ext, Bytes(), false, wait);
                    ConsumeStats part;
                    if (!response.body.empty()) ConsumeStats::decode(response.body, part);
                    std::lock_guard<std::mutex> lk(mergeMutex);
                    for (const auto& kv : part.offsetTable) {
                        result.offsetTable[kv.first] = kv.second;
                    }
                    consumerTpsMap[addr] = part.consumeTps;
                } catch (const std::exception& e) {
                    logger_warn("getConsumeStats error. topic=" + topic + " consumerGroup="
                                + consumerGroup + ": " + std::string(e.what()));
                }
            });
            for (const auto& kv : consumerTpsMap) result.consumeTps += kv.second;
            if (result.offsetTable.empty()) {
                try {
                    ConsumerConnection connection = examineConsumerConnectionInfo(consumerGroup);
                    if (connection.messageModel == "BROADCASTING") {
                        return adminFailure<ConsumeStats>(
                            AdminToolResultCode::BROADCAST_CONSUMPTION,
                            "Not found the consumer group consume stats, because return offset "
                            "table is empty, the consumer is under the broadcast mode");
                    }
                } catch (const std::exception&) {
                    return adminFailure<ConsumeStats>(
                        AdminToolResultCode::CONSUMER_NOT_ONLINE,
                        "Not found the consumer group consume stats, because return offset table "
                        "is empty, maybe the consumer not consume any message");
                }
            }
            return adminSuccess<ConsumeStats>(result);
        });
}

AdminToolResult<BrokerOperatorResult> DefaultMQAdminExt::deleteTopicInBrokerConcurrent(
    const std::vector<std::string>& addrs, const std::string& topic, int32_t timeoutMillis) {
    int32_t wait = timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis;
    return adminToolExecute<BrokerOperatorResult>(
        [&]() -> AdminToolResult<BrokerOperatorResult> {
            std::vector<std::string> targets;
            {
                std::set<std::string> seen;
                for (const std::string& a : addrs) {
                    if (seen.insert(a).second) targets.push_back(a);
                }
            }
            BrokerOperatorResult result;
            std::mutex mergeMutex;
            runBoundedFanOut(targets.size(), wait, [&](size_t i) {
                const std::string& addr = targets[i];
                try {
                    requireClient().deleteTopicInBroker(addr, topic, wait);
                    std::lock_guard<std::mutex> lk(mergeMutex);
                    result.successList.push_back(addr);
                } catch (const std::exception& e) {
                    logger_warn("deleteTopicInBroker error. topic=" + topic + " broker=" + addr
                                + ": " + std::string(e.what()));
                    std::lock_guard<std::mutex> lk(mergeMutex);
                    result.failureList.push_back(addr);
                }
            });
            return adminSuccess<BrokerOperatorResult>(result);
        });
}

AdminToolResult<BrokerOperatorResult> DefaultMQAdminExt::resetOffsetNewConcurrent(
    const std::string& group, const std::string& topic, int64_t timestamp, int32_t timeoutMillis) {
    int32_t wait = timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis;
    return adminToolExecute<BrokerOperatorResult>(
        [&]() -> AdminToolResult<BrokerOperatorResult> {
            TopicRouteData route;
            if (!examineTopicRouteOrNull(topic, route) || route.brokerDatas.empty()) {
                return adminFailure<BrokerOperatorResult>(
                    AdminToolResultCode::TOPIC_ROUTE_INFO_NOT_EXIST, "topic router info not found");
            }
            std::map<std::string, QueueData> topicRouteMap;
            for (const QueueData& qd : route.queueDatas) topicRouteMap[qd.brokerName] = qd;
            BrokerOperatorResult result;
            std::mutex mergeMutex;
            runBoundedFanOut(route.brokerDatas.size(), wait, [&](size_t i) {
                const BrokerData& bd = route.brokerDatas[i];
                const std::string addr = bd.selectBrokerAddr();
                if (addr.empty()) return;
                try {
                    invokeBrokerToResetOffsetConcurrent(addr, topic, group, timestamp, wait);
                    std::lock_guard<std::mutex> lk(mergeMutex);
                    result.successList.push_back(addr);
                } catch (const MQClientException& e) {
                    if (e.getResponseCode() == ResponseCode::CONSUMER_NOT_ONLINE) {
                        try {
                            resetOffsetOldOnBroker(addr, topicRouteMap[bd.brokerName], group,
                                                   topic, timestamp, true);
                            std::lock_guard<std::mutex> lk(mergeMutex);
                            result.successList.push_back(addr);
                        } catch (const std::exception& e2) {
                            logger_warn("resetOffsetByTimestampOld error. addr=" + addr
                                        + " topic=" + topic + " group=" + group + ": "
                                        + std::string(e2.what()));
                            std::lock_guard<std::mutex> lk(mergeMutex);
                            result.failureList.push_back(addr);
                        }
                    } else if (e.getResponseCode() == ResponseCode::SYSTEM_ERROR) {
                        // the group never came online on this broker, the reset is a no-op
                        std::lock_guard<std::mutex> lk(mergeMutex);
                        result.successList.push_back(addr);
                    } else {
                        logger_warn("resetOffsetNewConcurrent error. addr=" + addr + " topic="
                                    + topic + " group=" + group + ": " + std::string(e.what()));
                        std::lock_guard<std::mutex> lk(mergeMutex);
                        result.failureList.push_back(addr);
                    }
                } catch (const std::exception& e) {
                    logger_warn("resetOffsetNewConcurrent error. addr=" + addr + " topic=" + topic
                                + " group=" + group + ": " + std::string(e.what()));
                    std::lock_guard<std::mutex> lk(mergeMutex);
                    result.failureList.push_back(addr);
                }
            });
            if (result.successList.size() == route.brokerDatas.size()) {
                return adminSuccess<BrokerOperatorResult>(result);
            }
            return adminFailure<BrokerOperatorResult>(AdminToolResultCode::MQ_BROKER_ERROR,
                                                      "operator failure", result);
        });
}

AdminToolResult<TopicList> DefaultMQAdminExt::queryTopicsByConsumerConcurrent(
    const std::string& group, int32_t timeoutMillis) {
    int32_t wait = timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis;
    return adminToolExecute<TopicList>(
        [&]() -> AdminToolResult<TopicList> {
            TopicRouteData route;
            if (!examineTopicRouteOrNull(MixAll::getRetryTopic(group), route)
                || route.brokerDatas.empty()) {
                return adminFailure<TopicList>(AdminToolResultCode::TOPIC_ROUTE_INFO_NOT_EXIST,
                                               "router info not found.");
            }
            TopicList result;
            std::mutex mergeMutex;
            runBoundedFanOut(route.brokerDatas.size(), wait, [&](size_t i) {
                try {
                    const std::string addr = route.brokerDatas[i].selectBrokerAddr();
                    if (addr.empty()) return;
                    PropertyMap ext;
                    ext["group"] = group;
                    RemotingCommand response = requireClient().invokeSync(
                        vipAddr(addr), RequestCode::QUERY_TOPICS_BY_CONSUMER, ext, Bytes(), false,
                        wait);
                    TopicList part;
                    if (!response.body.empty()) TopicList::decode(response.body, part);
                    std::lock_guard<std::mutex> lk(mergeMutex);
                    for (const std::string& t : part.topicList) result.topicList.push_back(t);
                } catch (const std::exception& e) {
                    logger_warn("queryTopicsByConsumer error. group=" + group + ": "
                                + std::string(e.what()));
                }
            });
            return adminSuccess<TopicList>(result);
        });
}

AdminToolResult<JsonValue> DefaultMQAdminExt::queryConsumeTimeSpanConcurrent(
    const std::string& topic, const std::string& group, int32_t timeoutMillis) {
    int32_t wait = timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis;
    return adminToolExecute<JsonValue>(
        [&]() -> AdminToolResult<JsonValue> {
            JsonValue spans = JsonValue::makeArray();
            TopicRouteData route;
            if (!examineTopicRouteOrNull(topic, route) || route.brokerDatas.empty()) {
                return adminSuccess<JsonValue>(spans);
            }
            std::mutex mergeMutex;
            runBoundedFanOut(route.brokerDatas.size(), wait, [&](size_t i) {
                try {
                    const std::string addr = route.brokerDatas[i].selectBrokerAddr();
                    if (addr.empty()) return;
                    PropertyMap ext;
                    ext["topic"] = topic;
                    ext["group"] = group;
                    RemotingCommand response = requireClient().invokeSync(
                        vipAddr(addr), RequestCode::QUERY_CONSUME_TIME_SPAN, ext, Bytes(), false,
                        wait);
                    if (response.body.empty()) return;
                    JsonValue v;
                    if (!RemotingSerializable::decode(response.body, v)) return;
                    const JsonValue* arr = v.find("consumeTimeSpanSet");
                    if (arr == nullptr || !arr->isArray()) return;
                    std::lock_guard<std::mutex> lk(mergeMutex);
                    for (size_t j = 0; j < arr->size(); ++j) spans.pushArray(arr->at(j));
                } catch (const std::exception& e) {
                    logger_warn("queryConsumeTimeSpan error. topic=" + topic + " group=" + group
                                + ": " + std::string(e.what()));
                }
            });
            return adminSuccess<JsonValue>(spans);
        });
}

std::vector<MessageTrack> DefaultMQAdminExt::messageTrackDetailConcurrent(const MessageExt& msg,
                                                                          int32_t timeoutMillis) {
    int32_t wait = timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis;
    TopicRouteData route;
    if (!examineTopicRouteOrNull(msg.topic, route)) {
        throw MQClientException("topic " + msg.topic + " not exist");
    }
    std::string brokerAddr;
    for (const BrokerData& bd : route.brokerDatas) {
        brokerAddr = bd.selectBrokerAddr();
        if (!brokerAddr.empty()) break;
    }
    if (brokerAddr.empty()) {
        throw MQClientException("no broker address along the route of " + msg.topic);
    }
    std::set<std::string> groupSet = queryTopicConsumeByWho(brokerAddr, msg.topic);
    std::vector<std::string> groupList(groupSet.begin(), groupSet.end());
    std::vector<MessageTrack> result;
    std::mutex mergeMutex;
    runBoundedFanOut(groupList.size(), wait, [&](size_t i) {
        MessageTrack mt = trackMessageGroup(msg, groupList[i], false);
        std::lock_guard<std::mutex> lk(mergeMutex);
        result.push_back(mt);
    });
    return result;
}

// ---------------------------------------------------------------- Long-tail helpers
bool DefaultMQAdminExt::examineTopicRouteOrNull(const std::string& topic, TopicRouteData& out) {
    // Route probe with the null-for-TOPIC_NOT_EXIST contract: a missing topic yields false, a link
    // failure on every name server rethrows the last RemotingException, any other non-SUCCESS code
    // becomes MQClientException (the envelope classifies it).
    MQClientInstance& client = requireClient();
    PropertyMap ext;
    ext["topic"] = topic;
    std::exception_ptr pending;
    bool sawError = false;
    for (const std::string& ns : client.nameServerAddrs()) {
        try {
            RemotingCommand response = client.invokeSyncRaw(
                ns, RequestCode::GET_ROUTEINFO_BY_TOPIC, ext, Bytes(), false, timeoutMillis_);
            if (response.code == ResponseCode::TOPIC_NOT_EXIST) return false;
            if (response.code != ResponseCode::SUCCESS) {
                throw MQClientException(response.remark, response.code);
            }
            TopicRouteData route;
            if (response.body.empty() || !TopicRouteData::decode(response.body, route)) {
                throw MQClientException("topic route query for " + topic + " returned a bad body");
            }
            out = route;
            return true;
        } catch (const RemotingException& e) {
            pending = std::current_exception();
            sawError = true;
        }
    }
    if (sawError) std::rethrow_exception(pending);
    throw MQClientException("no name server available for the route query of " + topic);
}

JsonValue DefaultMQAdminExt::invokeBrokerMethod(const std::string& addr, int32_t requestCode,
                                                const PropertyMap& extFields,
                                                int32_t timeoutMillis) {
    RemotingCommand response = requireClient().invokeSyncRaw(vipAddr(addr), requestCode, extFields,
                                                             Bytes(), false, timeoutMillis);
    if (response.code != ResponseCode::SUCCESS) {
        throw MQBrokerException(response.code, response.remark);
    }
    if (response.body.empty()) return JsonValue::makeNull();
    JsonValue v;
    if (!RemotingSerializable::decode(response.body, v)) {
        throw MQClientException("invokeBrokerMethod: bad response body");
    }
    return v;
}

std::map<MessageQueue, int64_t> DefaultMQAdminExt::invokeBrokerToResetOffsetConcurrent(
    const std::string& brokerAddr, const std::string& topic, const std::string& group,
    int64_t timestamp, int32_t timeoutMillis) {
    // Unlike invokeBrokerToResetOffset above, SUCCESS without a body is a failure carrying the
    // SUCCESS code and the remark (the raw switch falls through to the throw).
    MQClientInstance& client = requireClient();
    PropertyMap ext = buildResetOffsetExtFields(topic, group, timestamp, true, -1, -1);
    RemotingCommand response = client.invokeSyncRaw(
        vipAddr(brokerAddr), RequestCode::INVOKE_BROKER_TO_RESET_OFFSET, ext, Bytes(), false,
        timeoutMillis);
    if (response.code == ResponseCode::SUCCESS && !response.body.empty()) {
        ResetOffsetBody body;
        std::map<MessageQueue, int64_t> offsets;
        if (ResetOffsetBody::decode(response.body, body)) {
            for (const auto& kv : body.offsetTable) offsets[kv.first] = kv.second;
        }
        return offsets;
    }
    throw MQClientException(response.remark, response.code);
}

void DefaultMQAdminExt::resetOffsetOldOnBroker(const std::string& brokerAddr,
                                               const QueueData& queueData,
                                               const std::string& group,
                                               const std::string& topic, int64_t timestamp,
                                               bool force) {
    // Consume-stats walk for one broker: every consumed queue of the topic gets searchOffset +
    // a forced consumer-offset update; a group that never consumed anything falls back to the
    // topic-stats walk over the read queues.
    MQClientInstance& client = requireClient();
    ConsumeStats consumeStats;
    {
        PropertyMap ext;
        ext["consumerGroup"] = group;
        RemotingCommand response = client.invokeSync(vipAddr(brokerAddr),
                                                     RequestCode::GET_CONSUME_STATS, ext, Bytes(),
                                                     false, timeoutMillis_);
        if (!response.body.empty()) ConsumeStats::decode(response.body, consumeStats);
    }
    bool hasConsumed = false;
    for (const auto& kv : consumeStats.offsetTable) {
        const MessageQueue& queue = kv.first;
        if (queue.topic != topic) continue;
        hasConsumed = true;
        resetOffsetOldOnQueue(brokerAddr, group, queue, kv.second, timestamp, force);
    }
    if (hasConsumed) return;

    std::map<MessageQueue, TopicOffset> topicStatus;
    {
        PropertyMap ext;
        ext["topic"] = topic;
        RemotingCommand response = client.invokeSync(vipAddr(brokerAddr),
                                                     RequestCode::GET_TOPIC_STATS_INFO, ext,
                                                     Bytes(), false, timeoutMillis_);
        if (!response.body.empty()) {
            TopicStatsTable table;
            if (TopicStatsTable::decode(response.body, table)) {
                topicStatus = std::move(table.offsetTable);
            }
        }
    }
    for (int32_t i = 0; i < queueData.readQueueNums; ++i) {
        MessageQueue queue(topic, queueData.brokerName, i);
        OffsetWrapper wrapper;
        auto it = topicStatus.find(queue);
        if (it != topicStatus.end()) {
            // the stats walk feeds broker offset / min offset as the starting pair
            wrapper.brokerOffset = it->second.maxOffset;
            wrapper.consumerOffset = it->second.minOffset;
        }
        resetOffsetOldOnQueue(brokerAddr, group, queue, wrapper, timestamp, force);
    }
}

void DefaultMQAdminExt::resetOffsetOldOnQueue(const std::string& brokerAddr,
                                              const std::string& group, const MessageQueue& mq,
                                              const OffsetWrapper& wrapper, int64_t timestamp,
                                              bool force) {
    MQClientInstance& client = requireClient();
    int64_t resetOffset = (timestamp == -1) ? client.getMaxOffset(mq, timeoutMillis_, brokerAddr)
                                            : client.searchOffsetByTimestamp(mq, timestamp,
                                                                             timeoutMillis_,
                                                                             brokerAddr);
    if (force || resetOffset <= wrapper.consumerOffset) {
        client.updateConsumerOffset(group, mq, resetOffset, timeoutMillis_, brokerAddr);
    }
}

void DefaultMQAdminExt::runBoundedFanOut(size_t count, int32_t timeoutMillis,
                                         const std::function<void(size_t)>& task) {
    if (count == 0) return;
    std::mutex doneMutex;
    size_t done = 0;
    std::condition_variable doneCv;
    std::vector<std::thread> workers;
    workers.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        workers.emplace_back([&task, &doneMutex, &done, &doneCv, i]() {
            task(i);
            std::lock_guard<std::mutex> lk(doneMutex);
            ++done;
            doneCv.notify_one();
        });
    }
    {
        std::unique_lock<std::mutex> lk(doneMutex);
        doneCv.wait_for(lk, std::chrono::milliseconds(timeoutMillis),
                        [&] { return done == count; });
    }
    // Every task has to finish before the caller reads the shared result containers
    // (std::map is not thread-safe); the per-task RPC timeouts bound this join.
    for (auto& t : workers) t.join();
}

template <typename T>
AdminToolResult<T> DefaultMQAdminExt::adminToolExecute(
    const std::function<AdminToolResult<T>()>& handler) const {
    try {
        return handler();
    } catch (const RemotingException& e) {
        AdminToolResult<T> r;
        r.code = AdminToolResultCode::REMOTING_ERROR;
        r.errorMsg = e.what();
        return r;
    } catch (const MQClientException& e) {
        AdminToolResult<T> r;
        if (e.getResponseCode() == ResponseCode::TOPIC_NOT_EXIST) {
            r.code = AdminToolResultCode::TOPIC_ROUTE_INFO_NOT_EXIST;
        } else {
            r.code = AdminToolResultCode::MQ_CLIENT_ERROR;
        }
        r.errorMsg = e.what();
        return r;
    } catch (const std::exception& e) {
        AdminToolResult<T> r;
        r.code = AdminToolResultCode::MQ_BROKER_ERROR;
        r.errorMsg = e.what();
        return r;
    }
}

}  // namespace rocketmq
