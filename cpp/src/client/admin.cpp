// 管理客户端实现（对应 Python client/admin.py）。
#include "rocketmq/client/admin.h"

#include <algorithm>
#include <string>
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
    // Java `DefaultMQAdminExtImpl#start`:161 无条件 `changeInstanceNameToPID`，clientId
    // 再走 `ClientConfig#buildMQClientId` 的 `<本机 IP>@<instanceName>`。本端口的 admin
    // 默认 instanceName 是 "ADMIN"（不是 Java 的 "DEFAULT"），所以改写只在调用方显式
    // 设成 "DEFAULT" 时才起作用。
    instanceName_ = changeInstanceNameToPID(instanceName_);
    if (clientId_.empty()) {
        clientId_ = buildClientId(instanceName_, unitName_, enableStreamRequestType_);
    }
    mqClient_.reset(new MQClientInstance(clientId_, nameServerAddrs_,
                                        3000, 15000, MQClientInstance::tlsEnabledFromEnv(),
                                        unitName_, pollNameServerIntervalMillis_));
    // 请求钩子（namespaceV2 的 `nsd`/`ns`、ACL 签名 / stream 的 `ReqT`）：管理端所有
    // 请求同样要带上。namespaceV2 传**取值函数**（Java 每笔请求实时读 clientConfig）。
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
    // 对应 Java DefaultMQAdminExt.createTopic → Validators.checkTopic + isSystemTopic：
    // 与 producer 门面同款本地快失败，非法/系统重名的建 topic 请求不该打到 broker。
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
    RemotingCommand response = requireClient().invokeSync(
        vipAddr(brokerAddr), RequestCode::GET_ALL_TOPIC_CONFIG, PropertyMap(), Bytes(), false,
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

// ---------------------------------------- 批量配置 / 单元化
// GroupForbidden：UPDATE_AND_GET_GROUP_FORBIDDEN(353) 的响应体
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
    // UPDATE_AND_CREATE_TOPIC_LIST(18)：custom header 为**空**（topic 名在 body 里做
    // 授权资源），body 是 CreateTopicListRequestBody JSON，即 {"topicConfigList":[...]}。
    if (configs.empty()) {
        throw MQClientException("createAndUpdateTopicConfigList: empty topicConfigList");
    }
    JsonValue rows = JsonValue::makeArray();
    for (const TopicConfig& config : configs) {
        rows.pushArray(config.toJson());
    }
    JsonValue body = JsonValue::makeObject();
    body.set("topicConfigList", rows);
    requireClient().invokeSync(vipAddr(brokerAddr), RequestCode::UPDATE_AND_CREATE_TOPIC_LIST,
                               PropertyMap(), RemotingSerializable::encode(body), true,
                               timeoutMillis_);
}

void DefaultMQAdminExt::createAndUpdateSubscriptionGroupConfigList(
    const std::string& brokerAddr, const std::vector<SubscriptionGroupConfig>& configs) {
    // UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST(225)：与 topic-list 同形，但 body key 是
    // `groupConfigList`（单组版 200 的 body 是裸 SubscriptionGroupConfig 对象）。
    if (configs.empty()) {
        throw MQClientException(
            "createAndUpdateSubscriptionGroupConfigList: empty groupConfigList");
    }
    JsonValue rows = JsonValue::makeArray();
    for (const SubscriptionGroupConfig& config : configs) {
        rows.pushArray(config.toJson());
    }
    JsonValue body = JsonValue::makeObject();
    body.set("groupConfigList", rows);
    requireClient().invokeSync(vipAddr(brokerAddr),
                               RequestCode::UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST,
                               PropertyMap(), RemotingSerializable::encode(body), true,
                               timeoutMillis_);
}

void DefaultMQAdminExt::createStaticTopic(const std::string& brokerAddr,
                                         const std::string& defaultTopic,
                                         const TopicConfig& config,
                                         const JsonValue& mappingDetail, bool force) {
    // UPDATE_AND_CREATE_STATIC_TOPIC(513)：header 复用 CreateTopicRequestHeader 字段
    // （bool 转 "true"/"false" 小写字符串），body 是 TopicQueueMappingDetail 的 JSON 文档
    // （单元化静态 topic 的映射信息）。
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
    // UPDATE_AND_GET_GROUP_FORBIDDEN(353)：readable 仅在非 nullopt 时带（Java 不设该
    // 字段即"仅查询"）；响应体是 GroupForbidden JSON，解码返回。
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
    // RESUME_CHECK_HALF_MESSAGE(323)：Java（MQClientAPIImpl:3279）对非 SUCCESS **返回
    // false 而不抛错**——网络层异常才上抛；broker 拒绝（如 msgId 不是半消息，表现为
    // SYSTEM_ERROR）通过返回值表达。invokeSync 会把非 SUCCESS 转成异常，所以这里拿
    // 原始响应自己判断 code。
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
    // **不是独立请求**：NameServer KV namespace=ORDER_TOPIC_CONFIG 上的读改写。
    // 集群模式把 value 原样写入；非集群模式把存储值当作 ";" 分隔的 "key:value" 列表，
    // 替换 key 匹配的条目后整体写回（一次只动单个 topic，不动整个集群）。
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
        // Java 打印后按空表继续：key 缺失是首写的常态，不是失败。
        oldValue.clear();
    }
    // 条目 key -> 完整 "key:value" 文本（Java HashMap 语义：重复 key 后写胜出）
    std::map<std::string, std::string> entries;
    size_t start = 0;
    while (start <= oldValue.size()) {
        size_t semi = oldValue.find(';', start);
        std::string entry = (semi == std::string::npos) ? oldValue.substr(start)
                                                        : oldValue.substr(start, semi - start);
        // 去首尾空白
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

// ---------------------------------------- 运维清理
void DefaultMQAdminExt::cleanExpiredConsumerQueue(const std::string& brokerAddr,
                                                 int32_t timeHours) {
    // CLEAN_EXPIRED_CONSUMEQUEUE(306)：broker 丢弃 `time` 小时前的 consume-queue 条目。
    PropertyMap ext;
    ext["time"] = i64str(timeHours);
    requireClient().invokeSync(vipAddr(brokerAddr), RequestCode::CLEAN_EXPIRED_CONSUMEQUEUE, ext,
                               Bytes(), false, timeoutMillis_);
}

std::vector<std::string> DefaultMQAdminExt::cleanExpiredConsumerQueueByAddr(
    const std::vector<std::string>& addrs, int32_t timeHours) {
    // Java 的 ByAddr 形态：逐个地址执行，返回**失败的地址**列表（不 fail-fast，
    // 一台 broker 挂了不掩盖其他台的结果）。
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
    // DELETE_EXPIRED_COMMITLOG(329)：broker 删除 `time` 小时前的 commit-log 文件。
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
    // CLEAN_UNUSED_TOPIC(316)：**单请求**，由 broker 自行清理未使用 topic。客户端绝不
    // 遍历 topic 表逐个删——broker 自建的 BenchmarkTest、重试/死信 topic 会被 broker
    // 以 SYSTEM_ERROR 拒绝。
    requireClient().invokeSync(vipAddr(brokerAddr), RequestCode::CLEAN_UNUSED_TOPIC, PropertyMap(),
                               Bytes(), false, timeoutMillis_);
}

JsonValue DefaultMQAdminExt::queryConsumeTimeSpan(const std::string& topic,
                                                 const std::string& group) {
    // QUERY_CONSUME_TIME_SPAN(303)：按 topic 路由扇出到每个 master，聚合响应 body 里
    // consumeTimeSpanSet JSON 数组。
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
    // Java 有 getClusterList / getTopicClusterList 两个名字、同一实现（Go 同口径做别名）。
    return getClusterList(topic);
}

void DefaultMQAdminExt::setMessageRequestMode(const std::string& brokerAddr,
                                             const std::string& topic,
                                             const std::string& consumerGroup,
                                             const std::string& mode, int32_t popShareQueueNum) {
    // SET_MESSAGE_REQUEST_MODE(401)：在 POP 与 Pull 模式间切换消费组（单元化场景）。
    PropertyMap ext;
    ext["topic"] = topic;
    ext["consumerGroup"] = consumerGroup;
    ext["mode"] = mode;
    if (popShareQueueNum > 0) ext["popShareQueueNum"] = i64str(popShareQueueNum);
    requireClient().invokeSync(vipAddr(brokerAddr), RequestCode::SET_MESSAGE_REQUEST_MODE, ext,
                               Bytes(), false, timeoutMillis_);
}

// ---------------------------------------- NameServer 配置（318/319）
void DefaultMQAdminExt::updateNameServerConfig(const PropertyMap& properties,
                                              int32_t timeoutMillis) {
    // UPDATE_NAMESRV_CONFIG(318)：body 是 properties **文本**（k=v\n，不是 JSON）；
    // 广播到每个 NameServer，任一失败即抛（Java 记 errResponse 最后统一抛）。
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
    // GET_NAMESRV_CONFIG(319)：逐个 NameServer 查询，body 是 properties 文本；
    // 返回 {地址: properties 字典}。
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

// ---------------------------------------------------------------- 集群 / Broker
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
    // ⚠ 响应体是 **properties 文本**（"k=v\n"），不是 JSON/KVTable。
    // 早期实现把它当 KVTable JSON 解析，真实 broker 上必然失败。
    RemotingCommand response = requireClient().invokeSync(
        vipAddr(brokerAddr), RequestCode::GET_BROKER_CONFIG, PropertyMap(), Bytes(), false,
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
    requireClient().invokeSync(vipAddr(addr), RequestCode::DELETE_SUBSCRIPTIONGROUP, ext, Bytes(),
                               false, timeoutMillis_);
}

// ---------------------------------------------------------------- 连接信息
ConsumerConnection DefaultMQAdminExt::examineConsumerConnectionInfo(
    const std::string& consumerGroup, const std::string& brokerAddr) {
    MQClientInstance& client = requireClient();
    std::string addr = brokerAddr.empty() ? findFirstBrokerAddr(client) : brokerAddr;
    PropertyMap ext;
    ext["consumerGroup"] = consumerGroup;
    RemotingCommand response = client.invokeSync(vipAddr(addr), RequestCode::GET_CONSUMER_CONNECTION_LIST,
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
    // 对应 Java `examineConsumeStats(group[, topic])`（:389-424）：按
    // `%RETRY%<group>` 的路由扇出全部 broker，逐台取统计并合并（offsetTable
    // 并入、consumeTps 累加）；全空时抛错（Java 的 MQClientException 同口径，
    // 带 CONSUMER_NOT_ONLINE 让 resetOffsetNew 式的退化分支可用）。
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
    // 对应 Java `DefaultMQAdminExtImpl.consumed:1533-1557`：该组在本队列的
    // consumerOffset 是否已越过这条消息的 queueOffset（位点越过 ⇒ 已消费）。
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
        // Java 先把 master 地址规范化成 ip:port 再比对（convert2IpString）；
        // 四端存的 broker 地址本来就是注册时的 ip:port 形态，直接比。
        if (storeHost.empty() || addrIt->second != storeHost) continue;
        if (kv.second.consumerOffset > msg.getQueueOffset()) return true;
    }
    return false;
}

// Java 枚举名原文（对账用；C++ 成员名相同，只是给测试/日志一个稳定的字符串形态）
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

// Java 分支里 exceptionDesc 的统一格式："CODE:n DESC:msg"
std::string codeDesc(int32_t code, const std::string& msg) {
    return "CODE:" + std::to_string(code) + " DESC:" + msg;
}

}  // namespace

std::vector<MessageTrack> DefaultMQAdminExt::messageTrackDetail(const MessageExt& msg) {
    // 对应 Java `DefaultMQAdminExtImpl.messageTrackDetail:1349-1427`：查谁在消费
    // 这个 topic，逐组判 CONSUMED / FILTERED / PULL / NOT_ONLINE / BROADCASTING…。
    std::vector<MessageTrack> result;
    TopicRouteData route = examineTopicRoute(msg.topic);
    std::string brokerAddr;
    for (const BrokerData& bd : route.brokerDatas) {
        brokerAddr = bd.selectBrokerAddr();
        if (!brokerAddr.empty()) break;
    }
    if (brokerAddr.empty()) return result;
    std::set<std::string> groups = queryTopicConsumeByWho(brokerAddr, msg.topic);
    // Java 按 broker 返回顺序遍历；本端拿到的是 set，排序让输出确定
    for (const std::string& group : groups) {
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
            result.push_back(mt);
            continue;
        } catch (const std::exception& e) {
            mt.exceptionDesc = e.what();
            result.push_back(mt);
            continue;
        }

        if (cc.consumeType == "CONSUME_ACTIVELY") {
            mt.trackType = TrackType::PULL;
        } else if (cc.consumeType == "CONSUME_PASSIVELY") {
            bool ifConsumed = false;
            try {
                ifConsumed = consumed(msg, group);
            } catch (const MQBrokerException& e) {
                if (e.getResponseCode() == ResponseCode::CONSUMER_NOT_ONLINE) {
                    mt.trackType = TrackType::NOT_ONLINE;
                    mt.exceptionDesc =
                        codeDesc(e.getResponseCode(), e.getResponseMessage());
                } else if (e.getResponseCode() == ResponseCode::BROADCAST_CONSUMPTION) {
                    mt.trackType = TrackType::CONSUME_BROADCASTING;
                }
                result.push_back(mt);
                continue;
            } catch (const MQClientException& e) {
                if (e.getResponseCode() == ResponseCode::CONSUMER_NOT_ONLINE) {
                    mt.trackType = TrackType::NOT_ONLINE;
                    mt.exceptionDesc = codeDesc(e.getResponseCode(), e.what());
                } else if (e.getResponseCode() == ResponseCode::BROADCAST_CONSUMPTION) {
                    mt.trackType = TrackType::CONSUME_BROADCASTING;
                }
                result.push_back(mt);
                continue;
            } catch (const std::exception& e) {
                mt.exceptionDesc = e.what();
                result.push_back(mt);
                continue;
            }

            if (ifConsumed) {
                mt.trackType = TrackType::CONSUMED;
                // Java 遍历订阅表找本 topic：tagsSet 非空、既不含消息 tag 也不含
                // "*" ⇒ 订阅比消息窄，消息是被过滤掉的那部分（SQL92 订阅 tagsSet
                // 为空，同样落回 CONSUMED —— 忠实保留 Java 语义）。
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
        }
        result.push_back(mt);
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
    // 对应 Java DefaultMQAdminExtImpl:1078：按 %RETRY%<group> 查路由，逐 broker 下发 343 合并
    TopicRouteData route = examineTopicRoute(MixAll::getRetryTopic(group));
    TopicList merged;
    for (const BrokerData& bd : route.brokerDatas) {
        std::string addr = bd.selectBrokerAddr();
        if (addr.empty()) continue;
        TopicList part = queryTopicsByConsumerToBroker(addr, group);
        for (const std::string& topic : part.topicList) {
            // Java 侧 TopicList.topicList 是 Set<String>，这里等价地去重
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

// ---------------------------------------------------------------- Offset 管理
int64_t DefaultMQAdminExt::maxOffset(const MessageQueue& mq) {
    return requireClient().getMaxOffset(mq);
}

int64_t DefaultMQAdminExt::minOffset(const MessageQueue& mq) {
    return requireClient().getMinOffset(mq);
}

int64_t DefaultMQAdminExt::searchOffset(const MessageQueue& mq, int64_t timestamp) {
    // Java MQAdminImpl:189：显式下发 LOWER 边界。
    return searchLowerBoundaryOffset(mq, timestamp);
}

int64_t DefaultMQAdminExt::searchLowerBoundaryOffset(const MessageQueue& mq, int64_t timestamp) {
    // 对应 Java DefaultMQAdminExt:133。
    return requireClient().searchOffsetByBoundary(mq, timestamp, BoundaryType::LOWER);
}

int64_t DefaultMQAdminExt::searchUpperBoundaryOffset(const MessageQueue& mq, int64_t timestamp) {
    // 对应 Java DefaultMQAdminExt:137。时间戳落在队尾之后时 UPPER 给最后一条自身的位点，
    // LOWER 给它的下一个位点（maxOffset）。
    return requireClient().searchOffsetByBoundary(mq, timestamp, BoundaryType::UPPER);
}

int64_t DefaultMQAdminExt::earliestMsgStoreTime(const MessageQueue& mq) {
    MQClientInstance& client = requireClient();
    // Java MQAdminImpl:250 的 earliestMsgStoreTime 与 max/min/search 同一个形状：
    // 只认 master，刷一次路由重查，仍拿不到照 :264 抛「The broker[X] not exist」。
    // （publishAddrInAdmin 是实例私有，这里用同形状的 publishAddrFor。）
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
    // 键名是 **isForce** 不是 force：Java `RemotingCommand.makeCustomHeaderToNet:437-450`
    // 拿 requestHeader 的**字段名**做 ext key，而 `ResetOffsetRequestHeader` 声明的字段是
    // `private boolean isForce`（getter `isForce()` 不参与命名）。写成 force 时 broker 侧
    // isForce 恒为 false ⇒ `Broker2Client.resetOffset:152-158` 的分支退化成「取时间戳位点」，
    // 前重（timestamp=-1）会把 consumerOffset 原样回显而不是跳到 maxOffset。
    // 5.5.1 真机探针：{"force":"true", timestamp:-1} → 目标 3（=consumerOffset），
    //               {"isForce":"true", timestamp:-1} → 目标 10（=maxOffset）。
    ext["isForce"] = isForce ? "true" : "false";
    // Java：offset=-1 表示 offset 为空
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
    // 对应 Java resetOffsetByTimestamp：逐 broker 下发 INVOKE_BROKER_TO_RESET_OFFSET
    // （broker 端按 timestamp 计算新位点，并同步在线消费者 + 更新 offset 表）。
    //
    // 注意：这里**不再**走"逐队列 searchOffset + updateConsumerOffset"的旧本地实现——
    // 那不会同步在线消费者，也不会做 broker 端一致性校验。
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
        // queueId=-1 / offset=-1：Java 的"按 timestamp 重置整个 topic"那个重载
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
    // 对应 Java DefaultMQAdminExt#resetOffsetByQueueId（DefaultMQAdminExtImpl:1827）：
    // 两笔 RPC 缺一不可。
    // 1) updateConsumerOffset(25) 直接把 offsetTable 改成目标位点；
    // 2) 带 queueId + offset 的 222 走 AdminBrokerProcessor#resetOffsetInner，先按
    //    [min, max+1] 校验（越界回 SYSTEM_ERROR "Target offset N not in consume queue
    //    range [min-max]"），再 ConsumerOffsetManager#assignResetOffset —— 它同时写
    //    resetOffsetTable（一次性，下次 pull 用 queryThenEraseResetOffset 取走）和
    //    offsetTable，并清掉该队列的 POP 在途计数。只做第 1 步的话在线消费者仍按自己
    //    内存里的位点继续拉。
    // 实测（5.5.1 真机）两笔 RPC **不是原子的**：ConsumerOffsetManager#commitOffset 只做
    //    覆盖写（offset 变小也只打 [NOTIFYME] warn，不做区间校验），所以第 2 笔被拒时，
    //    第 1 笔已经把非法位点落库。Java 同样如此，这里不做保护性回滚。
    updateConsumerOffsetToBroker(brokerAddr, consumerGroup,
                                 MessageQueue(topic, "", queueId), resetOffset);
    // Java 的单队列重载不传 force（默认 false），timestamp 传 0（位点已给定、不参与计算）
    return invokeBrokerToResetOffset(brokerAddr, topic, consumerGroup, 0, false, false,
                                     queueId, resetOffset);
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
    // 对应 Java DefaultMQAdminExtImpl.viewMessage：先按 offset msgId 直查 commitLog，
    // 解不出或查不到就退回 uniqKey（5.x 客户端返回的 msgId 本身就是 uniqKey，
    // 硬解只会得到越界的垃圾端口）。
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
        // Java 同样只 warn，然后走 uniqKey 兜底
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

}  // namespace rocketmq
