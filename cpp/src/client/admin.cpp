// 管理客户端实现（对应 Python client/admin.py）。
#include "rocketmq/client/admin.h"

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/serialize.h"

namespace rocketmq {

namespace {

// 把 int 写进 extFields（Java 侧 extFields 全是字符串）
std::string i64str(int64_t v) { return std::to_string(v); }

// 取响应 extFields 中的整数（缺失返回 fallback）
int64_t extInt(const RemotingCommand& r, const std::string& key, int64_t fallback) {
    auto it = r.extFields.find(key);
    if (it == r.extFields.end() || it->second.empty()) return fallback;
    try {
        return std::stoll(it->second);
    } catch (...) {
        return fallback;
    }
}

// JsonValue 的等价文本（用于比较 DataVersion 是否变化）
std::string dvText(const JsonValue& v) { return v.isNull() ? std::string("null") : v.dump(); }

}  // namespace

// ---------------------------------------------------------------- 生命周期
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
        // 去首尾空白
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
    if (clientId_.empty()) {
        clientId_ = buildClientId(instanceName_);
    }
    mqClient_.reset(new MQClientInstance(clientId_, nameServerAddrs_));
    mqClient_->start();
    // ACL 鉴权钩子：管理端的所有请求（建/删 topic、查状态等）同样需要签名。
    if (rpcHook_ && !mqClient_->registerRPCHook(rpcHook_)) {
        logger_warn("admin rpc hook ignored: MQClientInstance already has one (clientId="
                    + clientId_ + ")");
    }
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

// ---------------------------------------------------------------- 内部工具
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

// ---------------------------------------------------------------- Topic 管理
void DefaultMQAdminExt::createTopic(const std::string& /*key*/, const std::string& newTopic,
                                    int32_t queueNum, int32_t /*topicSysFlag*/) {
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
        addr, RequestCode::GET_TOPIC_CONFIG, ext, Bytes(), false, timeoutMillis_);
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
    RemotingCommand response = requireClient().invokeSync(
        brokerAddr, RequestCode::GET_ALL_TOPIC_CONFIG, PropertyMap(), Bytes(), false,
        timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis);
    TopicConfigSerializeWrapper w;
    if (!response.body.empty()) {
        TopicConfigSerializeWrapper::decode(response.body, w);
    }
    return w;
}

TopicConfigSerializeWrapper DefaultMQAdminExt::getUserTopicConfig(
    const std::string& brokerAddr, bool specialTopic, int32_t timeoutMillis) {
    TopicConfigSerializeWrapper wrapper = getAllTopicConfig(brokerAddr, timeoutMillis);
    // 对应 Java getUserTopicConfig：剔除系统 topic 与 %RETRY%/%DLQ%
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
        brokerAddr, RequestCode::GET_SYSTEM_TOPIC_LIST_FROM_BROKER, PropertyMap(), Bytes(), false,
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
        brokerAddr, RequestCode::GET_TOPIC_STATS_INFO, ext, Bytes(), false, timeoutMillis_);
    TopicStatsTable t;
    if (!response.body.empty()) TopicStatsTable::decode(response.body, t);
    return t;
}

// ---------------------------------------------------------------- 集群 / Broker
ClusterInfo DefaultMQAdminExt::fetchBrokerClusterInfo() {
    return requireClient().getBrokerClusterInfo();
}

KVTable DefaultMQAdminExt::fetchBrokerRuntimeStats(const std::string& brokerAddr,
                                                  int32_t timeoutMillis) {
    RemotingCommand response = requireClient().invokeSync(
        brokerAddr, RequestCode::GET_BROKER_RUNTIME_INFO, PropertyMap(), Bytes(), false,
        timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis);
    KVTable t;
    if (!response.body.empty()) KVTable::decode(response.body, t);
    return t;
}

PropertyMap DefaultMQAdminExt::getBrokerConfig(const std::string& brokerAddr,
                                               int32_t timeoutMillis) {
    // ⚠ 响应体是 **properties 文本**（"k=v\n"），不是 JSON/KVTable。
    // 早期实现把它当 KVTable JSON 解析，真实 broker 上必然失败。
    RemotingCommand response = requireClient().invokeSync(
        brokerAddr, RequestCode::GET_BROKER_CONFIG, PropertyMap(), Bytes(), false,
        timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis);
    return MixAll::string2Properties(response.body);
}

void DefaultMQAdminExt::updateBrokerConfig(const std::string& brokerAddr,
                                          const PropertyMap& properties,
                                          int32_t timeoutMillis) {
    // 对应 Java Validators.checkBrokerConfig 的 brokerPermission 校验
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
    requireClient().invokeSync(brokerAddr, RequestCode::UPDATE_BROKER_CONFIG, PropertyMap(),
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
            client.invokeSync(addr, RequestCode::CLEAN_UNUSED_TOPIC, PropertyMap(), Bytes(), false,
                              timeoutMillis_);
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
        brokerAddr, RequestCode::VIEW_BROKER_STATS_DATA, ext);
    JsonValue v;
    if (response.body.empty()) return JsonValue::makeNull();
    RemotingSerializable::decode(response.body, v);
    return v;
}

// ---------------------------------------------------------------- NameServer KV
void DefaultMQAdminExt::createAndUpdateKvConfig(const std::string& ns, const std::string& key,
                                               const std::string& value) {
    // Java putKVConfigValue：广播到**每一个** NameServer
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
            // 该 NS 明确返回"没有"，直接返回 false（不再试下一个）
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

// ---------------------------------------------------------------- 订阅组管理
void DefaultMQAdminExt::createAndUpdateSubscriptionGroupConfig(
    const std::string& addr, const SubscriptionGroupConfig& config) {
    Bytes body = config.encode();
    requireClient().invokeSync(addr, RequestCode::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP,
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
        addr, RequestCode::GET_SUBSCRIPTIONGROUP_CONFIG, ext, Bytes(), false, timeoutMillis_);
    if (response.body.empty()) return false;
    return SubscriptionGroupConfig::decode(response.body, out);
}

SubscriptionGroupWrapper DefaultMQAdminExt::getAllSubscriptionGroup(const std::string& brokerAddr,
                                                                   int32_t timeoutMillis) {
    // 对应 Java getAllSubscriptionGroup：**分页**累积，直到 groupSeq >= totalGroupNum-1。
    // 老版本 broker 不带 totalGroupNum，此时一次性返回全部（单轮即结束）。
    MQClientInstance& client = requireClient();
    int32_t timeout = timeoutMillis < 0 ? timeoutMillis_ : timeoutMillis;
    int64_t begin = UtilAll::currentTimeMillis();

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
        ext["maxGroupNum"] = "10000";
        if (haveVersion) ext["dataVersion"] = dvText(currentDataVersion);

        RemotingCommand response = client.invokeSyncRaw(
            brokerAddr, RequestCode::GET_ALL_SUBSCRIPTIONGROUP_CONFIG, ext, Bytes(), false,
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
        JsonValue newVersion = wrapper.dataVersion;
        if (!haveVersion) {
            currentDataVersion = newVersion;
            haveVersion = true;
        }
        groupSeq += static_cast<int64_t>(wrapper.subscriptionGroupTable.size());

        auto totalIt = response.extFields.find("totalGroupNum");
        if (totalIt == response.extFields.end() || totalIt->second.empty()) {
            // 老 broker：一次返回全部
            break;
        }
        int64_t total = 0;
        try {
            total = std::stoll(totalIt->second);
        } catch (...) {
            break;
        }
        if (dvText(currentDataVersion) != dvText(newVersion)) {
            logger_warn("subscription group dataVersion changed, restart paging");
            currentDataVersion = newVersion;
            groupSeq = 0;
            table.clear();
            forbidden = JsonValue::makeObject();
            continue;
        }
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
    requireClient().invokeSync(addr, RequestCode::DELETE_SUBSCRIPTIONGROUP, ext, Bytes(), false,
                               timeoutMillis_);
}

// ---------------------------------------------------------------- 连接信息
ConsumerConnection DefaultMQAdminExt::examineConsumerConnectionInfo(
    const std::string& consumerGroup, const std::string& brokerAddr) {
    MQClientInstance& client = requireClient();
    std::string addr = brokerAddr.empty() ? findFirstBrokerAddr(client) : brokerAddr;
    PropertyMap ext;
    ext["consumerGroup"] = consumerGroup;
    RemotingCommand response = client.invokeSync(addr, RequestCode::GET_CONSUMER_CONNECTION_LIST,
                                                 ext, Bytes(), false, timeoutMillis_);
    if (response.body.empty()) {
        throw MQClientException("consumer group " + consumerGroup + " not online");
    }
    ConsumerConnection cc;
    ConsumerConnection::decode(response.body, cc);
    return cc;
}

ProducerConnection DefaultMQAdminExt::examineProducerConnectionInfo(
    const std::string& producerGroup, const std::string& brokerAddr) {
    MQClientInstance& client = requireClient();
    std::string addr = brokerAddr.empty() ? findFirstBrokerAddr(client) : brokerAddr;
    PropertyMap ext;
    ext["producerGroup"] = producerGroup;
    RemotingCommand response = client.invokeSync(addr, RequestCode::GET_PRODUCER_CONNECTION_LIST,
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
    RemotingCommand response = client.invokeSync(addr, RequestCode::GET_CONSUMER_RUNNING_INFO, ext,
                                                 Bytes(), false, timeoutMillis_);
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

// ---------------------------------------------------------------- 消费统计
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
        brokerAddr, RequestCode::GET_CONSUME_STATS, ext, Bytes(), false, timeoutMillis_);
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
        brokerAddr, RequestCode::GET_BROKER_CONSUME_STATS, ext, Bytes(), false,
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
        brokerAddr, RequestCode::QUERY_TOPIC_CONSUME_BY_WHO, ext, Bytes(), false, timeoutMillis_);
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

TopicList DefaultMQAdminExt::queryTopicsByConsumer(const std::string& brokerAddr,
                                                  const std::string& group) {
    PropertyMap ext;
    ext["group"] = group;
    RemotingCommand response = requireClient().invokeSync(
        brokerAddr, RequestCode::QUERY_TOPICS_BY_CONSUMER, ext, Bytes(), false, timeoutMillis_);
    TopicList tl;
    if (!response.body.empty()) TopicList::decode(response.body, tl);
    return tl;
}

JsonValue DefaultMQAdminExt::querySubscription(const std::string& brokerAddr,
                                              const std::string& group,
                                              const std::string& topic) {
    PropertyMap ext;
    ext["group"] = group;
    ext["topic"] = topic;
    RemotingCommand response = requireClient().invokeSync(
        brokerAddr, RequestCode::QUERY_SUBSCRIPTION_BY_CONSUMER, ext, Bytes(), false,
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
        brokerAddr, RequestCode::INVOKE_BROKER_TO_GET_CONSUMER_STATUS, ext, Bytes(), false,
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
    requireClient().invokeSync(brokerAddr, RequestCode::CLONE_GROUP_OFFSET, ext, Bytes(), false,
                               timeoutMillis_);
}

// ---------------------------------------------------------------- Offset 管理
int64_t DefaultMQAdminExt::maxOffset(const MessageQueue& mq) {
    return requireClient().getMaxOffset(mq);
}

int64_t DefaultMQAdminExt::minOffset(const MessageQueue& mq) {
    return requireClient().getMinOffset(mq);
}

int64_t DefaultMQAdminExt::searchOffset(const MessageQueue& mq, int64_t timestamp) {
    return requireClient().searchOffsetByTimestamp(mq, timestamp);
}

int64_t DefaultMQAdminExt::earliestMsgStoreTime(const MessageQueue& mq) {
    MQClientInstance& client = requireClient();
    std::string addr = brokerAddrForMq(client, mq);
    PropertyMap ext;
    ext["topic"] = mq.topic;
    ext["queueId"] = i64str(mq.queueId);
    ext["brokerName"] = mq.brokerName;
    RemotingCommand response = client.invokeSync(addr, RequestCode::GET_EARLIEST_MSG_STORETIME, ext,
                                                 Bytes(), false, timeoutMillis_);
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

std::map<MessageQueue, int64_t> DefaultMQAdminExt::resetOffsetByTimestamp(
    const std::string& topic, const std::string& group, int64_t timestamp, bool isForce,
    const std::string& clusterName, bool isCpp) {
    // 对应 Java resetOffsetByTimestamp：逐 broker 下发 INVOKE_BROKER_TO_RESET_OFFSET
    // （broker 端按 timestamp 计算新位点，并同步在线消费者 + 更新 offset 表）。
    //
    // 注意：这里**不再**走"逐队列 searchOffset + updateConsumerOffset"的旧本地实现——
    // 那不会同步在线消费者，也不会做 broker 端一致性校验。
    MQClientInstance& client = requireClient();
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
        PropertyMap ext;
        ext["topic"] = topic;
        ext["group"] = group;
        ext["timestamp"] = i64str(timestamp);
        ext["force"] = isForce ? "true" : "false";
        // Java：offset=-1 表示 offset 为空
        ext["offset"] = "-1";
        RemotingCommand response = client.invokeSyncRaw(
            addr, RequestCode::INVOKE_BROKER_TO_RESET_OFFSET, ext, Bytes(), false, timeoutMillis_,
            isCpp ? LanguageCode::CPP : -1);
        if (response.code == ResponseCode::SUCCESS) {
            if (!response.body.empty()) {
                ResetOffsetBody body;
                if (ResetOffsetBody::decode(response.body, body)) {
                    for (const auto& kv : body.offsetTable) allOffsets[kv.first] = kv.second;
                }
            }
        } else {
            throw MQClientException(response.remark.empty() ? "reset offset failed" : response.remark,
                                    response.code);
        }
    }
    if (allOffsets.empty()) {
        throw MQClientException("reset offset failed, no broker returned offset table");
    }
    return allOffsets;
}

void DefaultMQAdminExt::resetOffsetNew(const std::string& consumerGroup,
                                      const std::string& topic, int64_t timestamp) {
    // 对应 Java resetOffsetNew：先试新版（broker 端重置），消费者不在线再退化到旧版
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
    // 对应 Java resetOffsetByTimestampOld：逐队列 searchOffset 后按 force 决策写回
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

// ---------------------------------------------------------------- 消息查询
std::vector<MessageExt> DefaultMQAdminExt::queryMessage(const std::string& topic,
                                                        const std::string& key, int32_t maxNum,
                                                        int64_t begin, int64_t end) {
    return requireClient().queryMessageAllBrokers(topic, key, maxNum, begin, end,
                                                  MessageConst::INDEX_KEY_TYPE, false);
}

bool DefaultMQAdminExt::queryMessageByUniqKey(const std::string& topic,
                                             const std::string& uniqKey, MessageExt& out) {
    // 对应 Java queryMessageByUniqKey：indexType="U" + extFields["_UNIQUE_KEY_QUERY"]="true"。
    // 注意：broker 侧 uniqKey 倒排索引只有 RocksDB 索引实现（IndexRocksDBStore）支持；
    // 默认文件索引下该查询会返回空，属 broker 配置差异而非客户端问题。
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
    // 对应 Java MQAdminImpl.viewMessage：从 msgId 自身解出 broker 地址 + commitLog 偏移
    std::string ip;
    int32_t port = 0;
    int64_t offset = 0;
    if (!decodeMessageId(msgId, ip, port, offset)) {
        throw MQClientException("query message by id finished, but no message.",
                                ResponseCode::NO_MESSAGE);
    }
    std::string addr = ip + ":" + std::to_string(port);
    PropertyMap ext;
    ext["topic"] = topic;
    ext["offset"] = i64str(offset);
    RemotingCommand response = requireClient().invokeSync(
        addr, RequestCode::VIEW_MESSAGE_BY_ID, ext, Bytes(), false, timeoutMillis_);
    if (response.body.empty()) {
        throw MQBrokerException(ResponseCode::NO_MESSAGE, "message not found: " + msgId);
    }
    std::vector<MessageExt> msgs = decodeMessages(response.body, true);
    if (msgs.empty()) {
        throw MQBrokerException(ResponseCode::NO_MESSAGE, "message not found: " + msgId);
    }
    return msgs[0];
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
        brokerAddr, RequestCode::QUERY_CONSUME_QUEUE, ext, Bytes(), false, timeoutMillis_);
    QueryConsumeQueueResponseBody body;
    if (!response.body.empty()) QueryConsumeQueueResponseBody::decode(response.body, body);
    return body;
}

}  // namespace rocketmq
