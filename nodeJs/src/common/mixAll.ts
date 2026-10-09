// -*- coding: utf-8 -*-
// MixAll constants and helpers (org.apache.rocketmq.common.MixAll).
import os from 'node:os';
import { createHash } from 'node:crypto';

export const MixAll = {
  NAMESRV_ADDR_PROPERTY: 'rocketmq.namesrv.addr',
  NAMESRV_ADDR_ENV: 'NAMESRV_ADDR',
  MESSAGE_COMPRESS_LEVEL: 'rocketmq.message.compressLevel',
  DEFAULT_TOPIC: 'TBW102',
  BENCHMARK_TOPIC: 'BenchmarkTest',
  DEFAULT_PRODUCER_GROUP: 'DEFAULT_PRODUCER',
  DEFAULT_CONSUMER_GROUP: 'DEFAULT_CONSUMER',
  CLIENT_INNER_PRODUCER_GROUP: 'CLIENT_INNER_PRODUCER',
  SELF_TEST_PRODUCER_GROUP: 'SELF_TEST_P_GROUP',
  SELF_TEST_CONSUMER_GROUP: 'SELF_TEST_C_GROUP',
  SCHEDULE_CONSUMER_GROUP: 'SCHEDULE_CONSUMER',
  ONS_HTTP_PROXY_GROUP: 'CID_ONS-HTTP-PROXY',
  CID_ONSAPI_PERMISSION_GROUP: 'CID_ONSAPI_PERMISSION',
  CID_ONSAPI_OWNER_GROUP: 'CID_ONSAPI_OWNER',
  CID_ONSAPI_PULL_GROUP: 'CID_ONSAPI_PULL',
  CID_RMQ_SYS_PREFIX: 'CID_RMQ_SYS_',
  CID_ONSAPI_PREFIX: 'CID_ONSAPI_',
  CID_SDK_SYNC_PREFIX: 'CID_SDK_SYNC_',
  CID_SDK_ASYNC_PREFIX: 'CID_SDK_ASYNC_',
  CID_SDK_PROXY_PREFIX: 'CID_SDK_PROXY_',
  PROXY_NAME: 'MQProxy',
  DEFAULT_PRODUCER_GROUP_AND_STREAM: 'DEFAULT_PRODUCER_AND_STREAM',

  RETRY_GROUP_TOPIC_PREFIX: '%RETRY%',
  DLQ_GROUP_TOPIC_PREFIX: '%DLQ%',
  REPLY_TOPIC_PREFIX: '%REPLY%',
  REPLY_TOPIC_POSTFIX: 'REPLY_TOPIC',
  REPLY_MESSAGE_FLAG: 'reply',
  SYSTEM_TOPIC_PREFIX: 'rmq_sys_',
  TOOLS_CONSUMER_GROUP: 'TOOLS_CONSUMER',
  FILTERSRV_CONSUMER_GROUP: 'FILTERSRV_CONSUMER',
  MONITOR_CONSUMER_GROUP: '__MONITOR_CONSUMER',
  CLIENT_INNER_CONSUMER_GROUP: 'CLIENT_INNER_CONSUMER',
  SELF_TEST_CONSUMER_GROUP2: 'SELF_TEST_C_GROUP2',
  ONS_NAMESPACE: 'namespace',
  UNIQUE_MSG_QUERY_FLAG: '_UNIQUE_KEY_QUERY',
  TRACE_TOPIC: 'RMQ_SYS_TRACE_TOPIC',
  REAL_TRACE_TOPIC: 'rmq_sys_TRACE_DATA',
  DEFAULT_TRACE_REGION_ID: 'DefaultRegion',
  TRANS_STAT_PROGRESS_TOPIC: 'RMQ_SYS_TRANS_OP_HALF_TOPIC',
  RMQ_SYS_TRANS_HALF_TOPIC: 'RMQ_SYS_TRANS_HALF_TOPIC',
  RMQ_SYS_TRANS_OP_HALF_TOPIC: 'RMQ_SYS_TRANS_OP_HALF_TOPIC',
  RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC: 'RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC',
  TRANS_CHECK_MAX_TIME: 15,
  UNIT_PREFIX: 'unit_',
  REQ_T: 'ReqT',
  STREAM_REQUEST_TYPE: 'STREAM',
  // Java MixAll.java:122 — RPC_REQUEST_HEADER_NAMESPACED_FIELD /
  // RPC_REQUEST_HEADER_NAMESPACE_FIELD. Written on every request by
  // NamespaceRpcHook when ClientConfig.namespaceV2 is non-empty (the
  // SERVER-side namespace mechanism of the Aliyun-style serverless instances).
  RPC_REQUEST_HEADER_NAMESPACED_FIELD: 'nsd',
  RPC_REQUEST_HEADER_NAMESPACE_FIELD: 'ns',
  LMQ_PREFIX: '%LMQ%',
  LMQ_QUEUE_ID: 0,
  DEFAULT_TOPIC_QUEUE_NUMS: 4,
  DEFAULT_TOPIC_READ_QUEUE_NUMS: 4,
  DEFAULT_TOPIC_WRITE_QUEUE_NUMS: 4,
  MAX_TOPIC_LENGTH: 127,
  MAX_GROUP_LENGTH: 255,
  CHARACTER_MAX_LENGTH: 255,
  PULL_TIMEOUT_MILLIS_HIGH: 30000,
  PULL_TIMEOUT_MILLIS_MEDIUM: 20000,
  PULL_TIMEOUT_MILLIS_LOW: 10000,
  LOG_STATS_TOPIC: 'LOG_STATS_TOPIC',

  W_HELPER: 'HELPER',
  W_EXPIRY_DATE: 'EXPIRY_DATE',
  W_AVATAR: 'AVATAR',
  W_REGION_ID: 'REGION_ID',

  MASTER_ID: 0,
  DEFAULT_CENTER: 'DEFAULT_CENTER',
  NAMESPACE_PATTERN: '^[%s]{4}[a-zA-Z0-9_-]+$',

  READ_PERM_BY_DEFAULT: 4 | 2,

  getRetryTopic(consumerGroup) { return `${MixAll.RETRY_GROUP_TOPIC_PREFIX}${consumerGroup}`; },
  isRetryTopic(topic) { return topic != null && topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX); },
  getDlqTopic(consumerGroup) { return `${MixAll.DLQ_GROUP_TOPIC_PREFIX}${consumerGroup}`; },
  isDlqTopic(topic) { return topic != null && topic.startsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX); },
  isSysTopic(topic) { return topic != null && topic.startsWith(MixAll.SYSTEM_TOPIC_PREFIX); },
  isLmq(meta) { return meta != null && meta.startsWith(MixAll.LMQ_PREFIX); },
  isSysConsumerGroup(g) { return g != null && g.startsWith(MixAll.CID_RMQ_SYS_PREFIX); },
  isPredefinedGroup(g) { return _PREDEFINE_GROUP_SET.has(g); },
  resetRetryAndDlqTopic(topic) {
    if (topic == null) return null;
    if (MixAll.isRetryTopic(topic)) return topic.slice(MixAll.RETRY_GROUP_TOPIC_PREFIX.length);
    if (MixAll.isDlqTopic(topic)) return topic.slice(MixAll.DLQ_GROUP_TOPIC_PREFIX.length);
    return topic;
  },
  brokerVipChannel(isChange, brokerAddr) {
    if (!isChange) return brokerAddr;
    const idx = brokerAddr.lastIndexOf(':');
    if (idx < 0) return brokerAddr;
    const host = brokerAddr.slice(0, idx);
    const portStr = brokerAddr.slice(idx + 1);
    const port = parseInt(portStr, 10);
    if (Number.isNaN(port)) return brokerAddr;
    return `${host}:${port - 2}`;
  },
  getReplyTopic(clusterName) { return `${clusterName}_${MixAll.REPLY_TOPIC_POSTFIX}`; },
  getBrokerCircuitBreakerConsumeGroup() { return 'BROKER_CIRCUIT_BREAKER'; },
  getBrokerCircuitBreakerTopic() { return 'BROKER_CIRCUIT_BREAKER_TOPIC'; },
  compareAndIncreaseNamespace(instanceName, namespace) {
    if (namespace == null || !namespace) return instanceName;
    if (instanceName.startsWith(namespace)) return instanceName;
    const prefix = `%%${namespace}%%`;
    if (instanceName.startsWith(prefix)) return instanceName;
    return `%%${namespace}%%${instanceName}`;
  },
  createUniqName(prefix) { return `${prefix}${createHash('md5').update(String(Math.random())).digest('hex').slice(0, 24)}`; },
  pid() { return process.pid; },

  DEFAULT_INSTANCE_NAME: 'DEFAULT',
  _cachedIp: null,
  getIpStr() {
    const { networkInterfaces } = os;
    const ifaces = networkInterfaces();
    for (const name of Object.keys(ifaces)) {
      for (const ni of ifaces[name]) {
        if (ni.family === 'IPv4' && !ni.internal) return ni.address;
      }
    }
    return '127.0.0.1';
  },
  cachedIpStr() {
    if (MixAll._cachedIp == null) MixAll._cachedIp = MixAll.getIpStr();
    return MixAll._cachedIp;
  },
  buildMqClientId(clientIp, instanceName, unitName = null, enableStreamRequestType = false) {
    let cid = `${clientIp}@${instanceName}`;
    if (unitName != null && unitName.trim()) cid = `${cid}@${unitName}`;
    if (enableStreamRequestType) cid = `${cid}@${MixAll.STREAM_REQUEST_TYPE}`;
    return cid;
  },
  changeInstanceNameToPid(instanceName) {
    if (instanceName === MixAll.DEFAULT_INSTANCE_NAME) {
      return `${MixAll.pid()}#${process.hrtime.bigint().toString()}`;
    }
    return instanceName;
  },
  clientIdFor(instanceName, unitName = null, enableStreamRequestType = false) {
    return MixAll.buildMqClientId(MixAll.cachedIpStr(), instanceName, unitName, enableStreamRequestType);
  },

  // ---- Namespace helpers (Java NamespaceUtil; port of go/common/namespace.go) ----
  // Facts that are easy to get wrong — do not "simplify" these away:
  //   - the separator is `%` (not `/`, not `:`);
  //   - the %RETRY% / %DLQ% prefix sits OUTSIDE the namespace, so the fully
  //     qualified retry topic is `%RETRY%NS%GID`. Both wrap and unwrap must
  //     strip the retry/DLQ prefix, work on the remainder, then put it back;
  //   - system resources (topics starting with `rmq_sys_`, groups starting
  //     with `CID_RMQ_SYS_`) are NEVER namespaced.
  NAMESPACE_SEPARATOR: '%',
  SYSTEM_TOPIC_PREFIX_RMQ: 'rmq_sys_',
  SYSTEM_CONSUMER_GROUP_PREFIX: 'CID_RMQ_SYS_',

  isSysTopicStrict(topic) { return topic != null && topic.startsWith(MixAll.SYSTEM_TOPIC_PREFIX_RMQ); },
  isSysConsumerGroupStrict(group) { return group != null && group.startsWith(MixAll.SYSTEM_CONSUMER_GROUP_PREFIX); },
  // isSystemResource mirrors Java NamespaceUtil.isSystemResource.
  isSystemResource(resource) {
    if (!resource) return false;
    return MixAll.isSysTopicStrict(resource) || MixAll.isSysConsumerGroupStrict(resource);
  },
  // withoutRetryAndDLQ strips the %RETRY% / %DLQ% prefix.
  withoutRetryAndDLQ(resource) { return MixAll.resetRetryAndDlqTopic(resource); },
  // isAlreadyWithNamespace mirrors Java NamespaceUtil.isAlreadyWithNamespace.
  isAlreadyWithNamespace(resource, namespace) {
    if (!namespace || !resource || MixAll.isSystemResource(resource)) return false;
    return MixAll.withoutRetryAndDLQ(resource).startsWith(namespace + MixAll.NAMESPACE_SEPARATOR);
  },
  // withoutNamespace strips the namespace prefix. `MQ_INST_XX%Topic` ->
  // `Topic`, `%RETRY%MQ_INST_XX%GID` -> `%RETRY%GID`. An empty namespace means
  // "drop whatever namespace is there"; a non-empty one only strips when it
  // actually matches.
  withoutNamespace(resourceWithNamespace, namespace = '') {
    if (!resourceWithNamespace) return resourceWithNamespace;
    if (namespace) {
      const plain = MixAll.withoutRetryAndDLQ(resourceWithNamespace);
      if (!plain.startsWith(namespace + MixAll.NAMESPACE_SEPARATOR)) return resourceWithNamespace;
    } else if (MixAll.isSystemResource(resourceWithNamespace)) {
      return resourceWithNamespace;
    }
    let prefix = '';
    if (MixAll.isRetryTopic(resourceWithNamespace)) prefix = MixAll.RETRY_GROUP_TOPIC_PREFIX;
    if (MixAll.isDlqTopic(resourceWithNamespace)) prefix = MixAll.DLQ_GROUP_TOPIC_PREFIX;
    const plain = MixAll.withoutRetryAndDLQ(resourceWithNamespace);
    const index = plain.indexOf(MixAll.NAMESPACE_SEPARATOR);
    if (index > 0) return prefix + plain.slice(index + 1);
    return resourceWithNamespace;
  },
  // wrapNamespace prefixes a resource with the namespace. Idempotent: a
  // resource that already carries the namespace, or any system resource, comes
  // back unchanged.
  wrapNamespace(namespace, resourceWithoutNamespace) {
    if (!namespace || !resourceWithoutNamespace) return resourceWithoutNamespace;
    if (MixAll.isSystemResource(resourceWithoutNamespace)) return resourceWithoutNamespace;
    if (MixAll.isAlreadyWithNamespace(resourceWithoutNamespace, namespace)) return resourceWithoutNamespace;
    let prefix = '';
    if (MixAll.isRetryTopic(resourceWithoutNamespace)) prefix = MixAll.RETRY_GROUP_TOPIC_PREFIX;
    if (MixAll.isDlqTopic(resourceWithoutNamespace)) prefix = MixAll.DLQ_GROUP_TOPIC_PREFIX;
    const plain = MixAll.withoutRetryAndDLQ(resourceWithoutNamespace);
    return prefix + namespace + MixAll.NAMESPACE_SEPARATOR + plain;
  },
  // wrapNamespaceAndRetry builds `%RETRY%<wrapNamespace(namespace, group)>`.
  wrapNamespaceAndRetry(namespace, consumerGroup) {
    if (!consumerGroup) return consumerGroup;
    return MixAll.RETRY_GROUP_TOPIC_PREFIX + MixAll.wrapNamespace(namespace, consumerGroup);
  },
  // getNamespaceFromResource extracts the namespace out of a resource name;
  // '' means "none".
  getNamespaceFromResource(resource) {
    if (!resource || MixAll.isSystemResource(resource)) return '';
    const plain = MixAll.withoutRetryAndDLQ(resource);
    const index = plain.indexOf(MixAll.NAMESPACE_SEPARATOR);
    if (index > 0) return plain.slice(0, index);
    return '';
  },
};

const _PREDEFINE_GROUP_SET = new Set([
  MixAll.DEFAULT_CONSUMER_GROUP,
  MixAll.DEFAULT_PRODUCER_GROUP,
  MixAll.TOOLS_CONSUMER_GROUP,
  MixAll.SCHEDULE_CONSUMER_GROUP,
  MixAll.FILTERSRV_CONSUMER_GROUP,
  MixAll.MONITOR_CONSUMER_GROUP,
  MixAll.CLIENT_INNER_PRODUCER_GROUP,
  MixAll.SELF_TEST_PRODUCER_GROUP,
  MixAll.SELF_TEST_CONSUMER_GROUP,
  MixAll.ONS_HTTP_PROXY_GROUP,
  MixAll.CID_ONSAPI_PERMISSION_GROUP,
  MixAll.CID_ONSAPI_OWNER_GROUP,
  MixAll.CID_ONSAPI_PULL_GROUP,
  MixAll.CID_RMQ_SYS_PREFIX + 'TRANS',
]);

export default MixAll;
