// 请求/响应自定义头 <-> extFields 的映射实现。
//
// 严格对齐 python/rocketmq/remoting/protocol/headers.py 与 Java header/*.java：
//   - toExtFields()：仅输出已设置（optional 有值）的字段，等价 Java makeCustomHeaderToNet 的"非空才写"；
//   - fromExtFields()：从 extFields 读回，缺省保持 nullopt；
//   - V2 用短字段名 a..n 缩减头部体积，并可无损与 V1 互转。
#include "rocketmq/remoting/protocol/headers.h"

namespace rocketmq {

// ---------------------------------------------------------------- 发送消息

PropertyMap SendMessageRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "producerGroup", producerGroup);
    putOptStr(out, "topic", topic);
    putOptStr(out, "defaultTopic", defaultTopic);
    putOptInt32(out, "defaultTopicQueueNums", defaultTopicQueueNums);
    putOptInt32(out, "queueId", queueId);
    putOptInt32(out, "sysFlag", sysFlag);
    putOptInt(out, "bornTimestamp", bornTimestamp);
    putOptInt32(out, "flag", flag);
    putOptStr(out, "properties", properties);
    putOptInt32(out, "reconsumeTimes", reconsumeTimes);
    putOptBool(out, "unitMode", unitMode);
    putOptInt32(out, "maxReconsumeTimes", maxReconsumeTimes);
    putOptBool(out, "batch", batch);
    putOptStr(out, "brokerName", brokerName);
    return out;
}

void SendMessageRequestHeader::fromExtFields(const PropertyMap& ext) {
    producerGroup = getOptStr(ext, "producerGroup");
    topic = getOptStr(ext, "topic");
    defaultTopic = getOptStr(ext, "defaultTopic");
    defaultTopicQueueNums = getOptInt(ext, "defaultTopicQueueNums");
    queueId = getOptInt(ext, "queueId");
    sysFlag = getOptInt(ext, "sysFlag");
    bornTimestamp = getOptLong(ext, "bornTimestamp");
    flag = getOptInt(ext, "flag");
    properties = getOptStr(ext, "properties");
    reconsumeTimes = getOptInt(ext, "reconsumeTimes");
    unitMode = getOptBool(ext, "unitMode");
    maxReconsumeTimes = getOptInt(ext, "maxReconsumeTimes");
    batch = getOptBool(ext, "batch");
    brokerName = getOptStr(ext, "brokerName");
}

PropertyMap SendMessageRequestHeaderV2::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "a", producerGroup);
    putOptStr(out, "b", topic);
    putOptStr(out, "c", defaultTopic);
    putOptInt32(out, "d", defaultTopicQueueNums);
    putOptInt32(out, "e", queueId);
    putOptInt32(out, "f", sysFlag);
    putOptInt(out, "g", bornTimestamp);
    putOptInt32(out, "h", flag);
    putOptStr(out, "i", properties);
    putOptInt32(out, "j", reconsumeTimes);
    putOptBool(out, "k", unitMode);
    putOptInt32(out, "l", maxReconsumeTimes);
    putOptBool(out, "m", batch);
    putOptStr(out, "n", brokerName);
    return out;
}

void SendMessageRequestHeaderV2::fromExtFields(const PropertyMap& ext) {
    producerGroup = getOptStr(ext, "a");
    topic = getOptStr(ext, "b");
    defaultTopic = getOptStr(ext, "c");
    defaultTopicQueueNums = getOptInt(ext, "d");
    queueId = getOptInt(ext, "e");
    sysFlag = getOptInt(ext, "f");
    bornTimestamp = getOptLong(ext, "g");
    flag = getOptInt(ext, "h");
    properties = getOptStr(ext, "i");
    reconsumeTimes = getOptInt(ext, "j");
    unitMode = getOptBool(ext, "k");
    maxReconsumeTimes = getOptInt(ext, "l");
    batch = getOptBool(ext, "m");
    brokerName = getOptStr(ext, "n");
}

SendMessageRequestHeaderV2 SendMessageRequestHeaderV2::fromV1(const SendMessageRequestHeader& v1) {
    SendMessageRequestHeaderV2 v2;
    v2.producerGroup = v1.producerGroup;
    v2.topic = v1.topic;
    v2.defaultTopic = v1.defaultTopic;
    v2.defaultTopicQueueNums = v1.defaultTopicQueueNums;
    v2.queueId = v1.queueId;
    v2.sysFlag = v1.sysFlag;
    v2.bornTimestamp = v1.bornTimestamp;
    v2.flag = v1.flag;
    v2.properties = v1.properties;
    v2.reconsumeTimes = v1.reconsumeTimes;
    v2.unitMode = v1.unitMode;
    v2.maxReconsumeTimes = v1.maxReconsumeTimes;
    v2.batch = v1.batch;
    v2.brokerName = v1.brokerName;
    return v2;
}

SendMessageRequestHeader SendMessageRequestHeaderV2::toV1() const {
    SendMessageRequestHeader v1;
    v1.producerGroup = producerGroup;
    v1.topic = topic;
    v1.defaultTopic = defaultTopic;
    v1.defaultTopicQueueNums = defaultTopicQueueNums;
    v1.queueId = queueId;
    v1.sysFlag = sysFlag;
    v1.bornTimestamp = bornTimestamp;
    v1.flag = flag;
    v1.properties = properties;
    v1.reconsumeTimes = reconsumeTimes;
    v1.unitMode = unitMode;
    v1.maxReconsumeTimes = maxReconsumeTimes;
    v1.batch = batch;
    v1.brokerName = brokerName;
    return v1;
}

PropertyMap SendMessageResponseHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "msgId", msgId);
    putOptInt32(out, "queueId", queueId);
    putOptInt(out, "queueOffset", queueOffset);
    putOptStr(out, "transactionId", transactionId);
    putOptInt(out, "msgRegion", msgRegion);
    return out;
}

void SendMessageResponseHeader::fromExtFields(const PropertyMap& ext) {
    msgId = getOptStr(ext, "msgId");
    queueId = getOptInt(ext, "queueId");
    queueOffset = getOptLong(ext, "queueOffset");
    transactionId = getOptStr(ext, "transactionId");
    msgRegion = getOptLong(ext, "msgRegion");
}

// ---------------------------------------------------------------- Request-Reply 应答推送

PropertyMap ReplyMessageRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "producerGroup", producerGroup);
    putOptStr(out, "topic", topic);
    putOptStr(out, "defaultTopic", defaultTopic);
    putOptInt32(out, "defaultTopicQueueNums", defaultTopicQueueNums);
    putOptInt32(out, "queueId", queueId);
    putOptInt32(out, "sysFlag", sysFlag);
    putOptInt(out, "bornTimestamp", bornTimestamp);
    putOptInt32(out, "flag", flag);
    putOptStr(out, "properties", properties);
    putOptInt32(out, "reconsumeTimes", reconsumeTimes);
    putOptBool(out, "unitMode", unitMode);
    putOptStr(out, "bornHost", bornHost);
    putOptStr(out, "storeHost", storeHost);
    putOptInt(out, "storeTimestamp", storeTimestamp);
    return out;
}

void ReplyMessageRequestHeader::fromExtFields(const PropertyMap& ext) {
    producerGroup = getOptStr(ext, "producerGroup");
    topic = getOptStr(ext, "topic");
    defaultTopic = getOptStr(ext, "defaultTopic");
    defaultTopicQueueNums = getOptInt(ext, "defaultTopicQueueNums");
    queueId = getOptInt(ext, "queueId");
    sysFlag = getOptInt(ext, "sysFlag");
    bornTimestamp = getOptLong(ext, "bornTimestamp");
    flag = getOptInt(ext, "flag");
    properties = getOptStr(ext, "properties");
    reconsumeTimes = getOptInt(ext, "reconsumeTimes");
    unitMode = getOptBool(ext, "unitMode");
    bornHost = getOptStr(ext, "bornHost");
    storeHost = getOptStr(ext, "storeHost");
    storeTimestamp = getOptLong(ext, "storeTimestamp");
}

// ---------------------------------------------------------------- 拉取消息

PropertyMap PullMessageRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "consumerGroup", consumerGroup);
    putOptStr(out, "topic", topic);
    putOptStr(out, "liteTopic", liteTopic);
    putOptInt32(out, "queueId", queueId);
    putOptInt(out, "queueOffset", queueOffset);
    putOptInt32(out, "maxMsgNums", maxMsgNums);
    putOptInt32(out, "sysFlag", sysFlag);
    putOptInt(out, "commitOffset", commitOffset);
    putOptInt(out, "suspendTimeoutMillis", suspendTimeoutMillis);
    putOptStr(out, "subscription", subscription);
    putOptInt(out, "subVersion", subVersion);
    putOptStr(out, "expressionType", expressionType);
    putOptInt32(out, "maxMsgBytes", maxMsgBytes);
    putOptInt32(out, "requestSource", requestSource);
    putOptStr(out, "proxyFrowardClientId", proxyFrowardClientId);
    return out;
}

void PullMessageRequestHeader::fromExtFields(const PropertyMap& ext) {
    consumerGroup = getOptStr(ext, "consumerGroup");
    topic = getOptStr(ext, "topic");
    liteTopic = getOptStr(ext, "liteTopic");
    queueId = getOptInt(ext, "queueId");
    queueOffset = getOptLong(ext, "queueOffset");
    maxMsgNums = getOptInt(ext, "maxMsgNums");
    sysFlag = getOptInt(ext, "sysFlag");
    commitOffset = getOptLong(ext, "commitOffset");
    suspendTimeoutMillis = getOptLong(ext, "suspendTimeoutMillis");
    subscription = getOptStr(ext, "subscription");
    subVersion = getOptLong(ext, "subVersion");
    expressionType = getOptStr(ext, "expressionType");
    maxMsgBytes = getOptInt(ext, "maxMsgBytes");
    requestSource = getOptInt(ext, "requestSource");
    proxyFrowardClientId = getOptStr(ext, "proxyFrowardClientId");
}

PropertyMap PullMessageResponseHeader::toExtFields() const {
    PropertyMap out;
    putOptInt(out, "suggestWhichBrokerId", suggestWhichBrokerId);
    putOptInt(out, "nextBeginOffset", nextBeginOffset);
    putOptInt(out, "minOffset", minOffset);
    putOptInt(out, "maxOffset", maxOffset);
    putOptBool(out, "forbidCommitOffset", forbidCommitOffset);
    return out;
}

void PullMessageResponseHeader::fromExtFields(const PropertyMap& ext) {
    suggestWhichBrokerId = getOptLong(ext, "suggestWhichBrokerId");
    nextBeginOffset = getOptLong(ext, "nextBeginOffset");
    minOffset = getOptLong(ext, "minOffset");
    maxOffset = getOptLong(ext, "maxOffset");
    forbidCommitOffset = getOptBool(ext, "forbidCommitOffset");
}

// ---------------------------------------------------------------- 消费位点

PropertyMap QueryConsumerOffsetRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "consumerGroup", consumerGroup);
    putOptStr(out, "topic", topic);
    putOptInt32(out, "queueId", queueId);
    return out;
}

void QueryConsumerOffsetRequestHeader::fromExtFields(const PropertyMap& ext) {
    consumerGroup = getOptStr(ext, "consumerGroup");
    topic = getOptStr(ext, "topic");
    queueId = getOptInt(ext, "queueId");
}

PropertyMap QueryConsumerOffsetResponseHeader::toExtFields() const {
    PropertyMap out;
    putOptInt(out, "offset", offset);
    return out;
}

void QueryConsumerOffsetResponseHeader::fromExtFields(const PropertyMap& ext) {
    offset = getOptLong(ext, "offset");
}

PropertyMap UpdateConsumerOffsetRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "consumerGroup", consumerGroup);
    putOptStr(out, "topic", topic);
    putOptInt32(out, "queueId", queueId);
    putOptInt(out, "commitOffset", commitOffset);
    return out;
}

void UpdateConsumerOffsetRequestHeader::fromExtFields(const PropertyMap& ext) {
    consumerGroup = getOptStr(ext, "consumerGroup");
    topic = getOptStr(ext, "topic");
    queueId = getOptInt(ext, "queueId");
    commitOffset = getOptLong(ext, "commitOffset");
}

// ---------------------------------------------------------------- offset 查询

PropertyMap GetMaxOffsetRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "topic", topic);
    putOptInt32(out, "queueId", queueId);
    return out;
}

void GetMaxOffsetRequestHeader::fromExtFields(const PropertyMap& ext) {
    topic = getOptStr(ext, "topic");
    queueId = getOptInt(ext, "queueId");
}

PropertyMap GetMaxOffsetResponseHeader::toExtFields() const {
    PropertyMap out;
    putOptInt(out, "offset", offset);
    return out;
}

void GetMaxOffsetResponseHeader::fromExtFields(const PropertyMap& ext) {
    offset = getOptLong(ext, "offset");
}

PropertyMap GetMinOffsetRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "topic", topic);
    putOptInt32(out, "queueId", queueId);
    return out;
}

void GetMinOffsetRequestHeader::fromExtFields(const PropertyMap& ext) {
    topic = getOptStr(ext, "topic");
    queueId = getOptInt(ext, "queueId");
}

PropertyMap GetMinOffsetResponseHeader::toExtFields() const {
    PropertyMap out;
    putOptInt(out, "offset", offset);
    return out;
}

void GetMinOffsetResponseHeader::fromExtFields(const PropertyMap& ext) {
    offset = getOptLong(ext, "offset");
}

PropertyMap SearchOffsetRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "topic", topic);
    putOptInt32(out, "queueId", queueId);
    putOptInt(out, "timestamp", timestamp);
    return out;
}

void SearchOffsetRequestHeader::fromExtFields(const PropertyMap& ext) {
    topic = getOptStr(ext, "topic");
    queueId = getOptInt(ext, "queueId");
    timestamp = getOptLong(ext, "timestamp");
}

PropertyMap SearchOffsetResponseHeader::toExtFields() const {
    PropertyMap out;
    putOptInt(out, "offset", offset);
    return out;
}

void SearchOffsetResponseHeader::fromExtFields(const PropertyMap& ext) {
    offset = getOptLong(ext, "offset");
}

// ---------------------------------------------------------------- 其它常用头部

PropertyMap ViewMessageRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptInt(out, "offset", offset);
    return out;
}

void ViewMessageRequestHeader::fromExtFields(const PropertyMap& ext) {
    offset = getOptLong(ext, "offset");
}

PropertyMap QueryMessageRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "topic", topic);
    putOptStr(out, "key", key);
    putOptInt32(out, "maxNum", maxNum);
    putOptInt(out, "beginTimestamp", beginTimestamp);
    putOptInt(out, "endTimestamp", endTimestamp);
    putOptStr(out, "indexType", indexType);
    putOptStr(out, "lastKey", lastKey);
    return out;
}

void QueryMessageRequestHeader::fromExtFields(const PropertyMap& ext) {
    topic = getOptStr(ext, "topic");
    key = getOptStr(ext, "key");
    maxNum = getOptInt(ext, "maxNum");
    beginTimestamp = getOptLong(ext, "beginTimestamp");
    endTimestamp = getOptLong(ext, "endTimestamp");
    indexType = getOptStr(ext, "indexType");
    lastKey = getOptStr(ext, "lastKey");
}

PropertyMap EndTransactionRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "topic", topic);
    putOptStr(out, "producerGroup", producerGroup);
    putOptInt(out, "tranStateTableOffset", tranStateTableOffset);
    putOptInt(out, "commitLogOffset", commitLogOffset);
    putOptInt32(out, "commitOrRollback", commitOrRollback);
    putOptBool(out, "fromTransactionCheck", fromTransactionCheck);
    putOptStr(out, "msgId", msgId);
    putOptStr(out, "transactionId", transactionId);
    putOptStr(out, "bname", bname);
    return out;
}

void EndTransactionRequestHeader::fromExtFields(const PropertyMap& ext) {
    topic = getOptStr(ext, "topic");
    producerGroup = getOptStr(ext, "producerGroup");
    tranStateTableOffset = getOptLong(ext, "tranStateTableOffset");
    commitLogOffset = getOptLong(ext, "commitLogOffset");
    commitOrRollback = getOptInt(ext, "commitOrRollback");
    fromTransactionCheck = getOptBool(ext, "fromTransactionCheck");
    msgId = getOptStr(ext, "msgId");
    transactionId = getOptStr(ext, "transactionId");
    bname = getOptStr(ext, "bname");
}

PropertyMap ConsumerSendMsgBackRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "group", group);
    putOptInt(out, "offset", offset);
    putOptInt32(out, "delayLevel", delayLevel);
    putOptStr(out, "originMsgId", originMsgId);
    putOptStr(out, "originTopic", originTopic);
    putOptBool(out, "unitMode", unitMode);
    putOptInt32(out, "maxReconsumeTimes", maxReconsumeTimes);
    return out;
}

void ConsumerSendMsgBackRequestHeader::fromExtFields(const PropertyMap& ext) {
    group = getOptStr(ext, "group");
    offset = getOptLong(ext, "offset");
    delayLevel = getOptInt(ext, "delayLevel");
    originMsgId = getOptStr(ext, "originMsgId");
    originTopic = getOptStr(ext, "originTopic");
    unitMode = getOptBool(ext, "unitMode");
    maxReconsumeTimes = getOptInt(ext, "maxReconsumeTimes");
}

PropertyMap HeartbeatRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "clientID", clientId);
    return out;
}

void HeartbeatRequestHeader::fromExtFields(const PropertyMap& ext) {
    clientId = getOptStr(ext, "clientID");
}

PropertyMap UnregisterClientRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "clientID", clientId);
    putOptStr(out, "producerGroup", producerGroup);
    putOptStr(out, "consumerGroup", consumerGroup);
    return out;
}

void UnregisterClientRequestHeader::fromExtFields(const PropertyMap& ext) {
    clientId = getOptStr(ext, "clientID");
    producerGroup = getOptStr(ext, "producerGroup");
    consumerGroup = getOptStr(ext, "consumerGroup");
}

PropertyMap GetConsumerListByGroupRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "consumerGroup", consumerGroup);
    return out;
}

void GetConsumerListByGroupRequestHeader::fromExtFields(const PropertyMap& ext) {
    consumerGroup = getOptStr(ext, "consumerGroup");
}

PropertyMap NotifyConsumerIdsChangedRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "consumerGroup", consumerGroup);
    return out;
}

void NotifyConsumerIdsChangedRequestHeader::fromExtFields(const PropertyMap& ext) {
    consumerGroup = getOptStr(ext, "consumerGroup");
}

PropertyMap GetRouteInfoRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "topic", topic);
    return out;
}

void GetRouteInfoRequestHeader::fromExtFields(const PropertyMap& ext) {
    topic = getOptStr(ext, "topic");
}

PropertyMap CheckTransactionStateRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "topic", topic);
    putOptInt(out, "tranStateTableOffset", tranStateTableOffset);
    putOptInt(out, "commitLogOffset", commitLogOffset);
    putOptStr(out, "msgId", msgId);
    putOptStr(out, "transactionId", transactionId);
    putOptStr(out, "offsetMsgId", offsetMsgId);  // Java 侧是 String，不是 long
    putOptStr(out, "bname", bname);
    return out;
}

void CheckTransactionStateRequestHeader::fromExtFields(const PropertyMap& ext) {
    topic = getOptStr(ext, "topic");
    tranStateTableOffset = getOptLong(ext, "tranStateTableOffset");
    commitLogOffset = getOptLong(ext, "commitLogOffset");
    msgId = getOptStr(ext, "msgId");
    transactionId = getOptStr(ext, "transactionId");
    offsetMsgId = getOptStr(ext, "offsetMsgId");
    bname = getOptStr(ext, "bname");
}

PropertyMap CreateTopicRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "topic", topic);
    putOptStr(out, "defaultTopic", defaultTopic);
    putOptInt32(out, "readQueueNums", readQueueNums);
    putOptInt32(out, "writeQueueNums", writeQueueNums);
    putOptInt32(out, "perm", perm);
    putOptStr(out, "topicFilterType", topicFilterType);
    putOptInt32(out, "topicSysFlag", topicSysFlag);
    putOptBool(out, "order", order);
    putOptStr(out, "attributes", attributes);
    putOptBool(out, "force", force);
    return out;
}

void CreateTopicRequestHeader::fromExtFields(const PropertyMap& ext) {
    topic = getOptStr(ext, "topic");
    defaultTopic = getOptStr(ext, "defaultTopic");
    readQueueNums = getOptInt(ext, "readQueueNums");
    writeQueueNums = getOptInt(ext, "writeQueueNums");
    perm = getOptInt(ext, "perm");
    topicFilterType = getOptStr(ext, "topicFilterType");
    topicSysFlag = getOptInt(ext, "topicSysFlag");
    order = getOptBool(ext, "order");
    attributes = getOptStr(ext, "attributes");
    force = getOptBool(ext, "force");
}

// ---------------------------------------------------------------- POP 模式

PropertyMap PopMessageRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "consumerGroup", consumerGroup);
    putOptStr(out, "topic", topic);
    putOptInt32(out, "queueId", queueId);
    putOptInt32(out, "maxMsgNums", maxMsgNums);
    putOptInt(out, "invisibleTime", invisibleTime);
    putOptInt(out, "pollTime", pollTime);
    putOptInt(out, "bornTime", bornTime);
    putOptInt32(out, "initMode", initMode);
    putOptStr(out, "expType", expType);
    putOptStr(out, "exp", exp);
    // 非 optional：Java 侧是 Boolean order = Boolean.FALSE，总是写出
    putOptBool(out, "order", std::optional<bool>(order));
    putOptStr(out, "attemptId", attemptId);
    return out;
}

void PopMessageRequestHeader::fromExtFields(const PropertyMap& ext) {
    consumerGroup = getOptStr(ext, "consumerGroup");
    topic = getOptStr(ext, "topic");
    queueId = getOptInt(ext, "queueId");
    maxMsgNums = getOptInt(ext, "maxMsgNums");
    invisibleTime = getOptLong(ext, "invisibleTime");
    pollTime = getOptLong(ext, "pollTime");
    bornTime = getOptLong(ext, "bornTime");
    initMode = getOptInt(ext, "initMode");
    expType = getOptStr(ext, "expType");
    exp = getOptStr(ext, "exp");
    order = getOptBool(ext, "order").value_or(false);
    attemptId = getOptStr(ext, "attemptId");
}

PropertyMap PopMessageResponseHeader::toExtFields() const {
    PropertyMap out;
    putOptInt(out, "popTime", popTime);
    putOptInt(out, "invisibleTime", invisibleTime);
    putOptInt32(out, "reviveQid", reviveQid);
    putOptInt(out, "restNum", restNum);
    putOptStr(out, "startOffsetInfo", startOffsetInfo);
    putOptStr(out, "msgOffsetInfo", msgOffsetInfo);
    putOptStr(out, "orderCountInfo", orderCountInfo);
    return out;
}

void PopMessageResponseHeader::fromExtFields(const PropertyMap& ext) {
    popTime = getOptLong(ext, "popTime");
    invisibleTime = getOptLong(ext, "invisibleTime");
    reviveQid = getOptInt(ext, "reviveQid");
    restNum = getOptLong(ext, "restNum");
    startOffsetInfo = getOptStr(ext, "startOffsetInfo");
    msgOffsetInfo = getOptStr(ext, "msgOffsetInfo");
    orderCountInfo = getOptStr(ext, "orderCountInfo");
}

PropertyMap AckMessageRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "consumerGroup", consumerGroup);
    putOptStr(out, "topic", topic);
    putOptInt32(out, "queueId", queueId);
    putOptStr(out, "extraInfo", extraInfo);
    putOptInt(out, "offset", offset);
    putOptStr(out, "liteTopic", liteTopic);
    return out;
}

void AckMessageRequestHeader::fromExtFields(const PropertyMap& ext) {
    consumerGroup = getOptStr(ext, "consumerGroup");
    topic = getOptStr(ext, "topic");
    queueId = getOptInt(ext, "queueId");
    extraInfo = getOptStr(ext, "extraInfo");
    offset = getOptLong(ext, "offset");
    liteTopic = getOptStr(ext, "liteTopic");
}

PropertyMap ChangeInvisibleTimeRequestHeader::toExtFields() const {
    PropertyMap out;
    putOptStr(out, "consumerGroup", consumerGroup);
    putOptStr(out, "topic", topic);
    putOptInt32(out, "queueId", queueId);
    putOptStr(out, "extraInfo", extraInfo);
    putOptInt(out, "offset", offset);
    putOptInt(out, "invisibleTime", invisibleTime);
    putOptStr(out, "liteTopic", liteTopic);
    // 非 optional：Java 侧是 private boolean suspend = false，总是写出
    putOptBool(out, "suspend", std::optional<bool>(suspend));
    return out;
}

void ChangeInvisibleTimeRequestHeader::fromExtFields(const PropertyMap& ext) {
    consumerGroup = getOptStr(ext, "consumerGroup");
    topic = getOptStr(ext, "topic");
    queueId = getOptInt(ext, "queueId");
    extraInfo = getOptStr(ext, "extraInfo");
    offset = getOptLong(ext, "offset");
    invisibleTime = getOptLong(ext, "invisibleTime");
    liteTopic = getOptStr(ext, "liteTopic");
    suspend = getOptBool(ext, "suspend").value_or(false);
}

PropertyMap ChangeInvisibleTimeResponseHeader::toExtFields() const {
    PropertyMap out;
    putOptInt(out, "popTime", popTime);
    putOptInt(out, "invisibleTime", invisibleTime);
    putOptInt32(out, "reviveQid", reviveQid);
    return out;
}

void ChangeInvisibleTimeResponseHeader::fromExtFields(const PropertyMap& ext) {
    popTime = getOptLong(ext, "popTime");
    invisibleTime = getOptLong(ext, "invisibleTime");
    reviveQid = getOptInt(ext, "reviveQid");
}

}  // namespace rocketmq
