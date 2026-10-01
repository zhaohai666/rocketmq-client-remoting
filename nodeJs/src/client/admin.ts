// -*- coding: utf-8 -*-
// DefaultMQAdminExt — the management client.
// Ported from org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl (Java 5.x,
// reference source: zhaohai666-rocketmq), with the Python/Go ports' live-5.5.1
// verified wire facts folded in. Method names and semantics follow Java.
//
// Java structure: DefaultMQAdminExtImpl delegates to
// MQClientInstance.getMQClientAPIImpl(); here the same RPCs are issued through
// MQClient's RemotingClient directly (the TS client has no separate API layer).
//
// Wire facts that were expensive to establish — do NOT "simplify" them:
//
//   - GET_BROKER_CONFIG(26)'s body is java.util.Properties TEXT (`k=v\n`), not
//     JSON. Parsing it as a KVTable fails on every real broker.
//   - UPDATE_AND_CREATE_SUBSCRIPTIONGROUP(200)'s body is a
//     SubscriptionGroupConfig JSON object.
//   - GET_TOPIC_CONFIG(351) sends `topic` + `lo`, and its body is a plain
//     TopicConfig JSON.
//   - GET_ALL_SUBSCRIPTIONGROUP_CONFIG(201) is PAGED:
//     groupSeq / maxGroupNum / dataVersion, accumulating until
//     groupSeq >= totalGroupNum-1. A broker that omits totalGroupNum returns
//     everything in one round.
//   - fetchTopicsByCluster sends ext key `cluster` (NOT `clusterName`); with
//     the wrong key the nameserver NPEs internally, swallows it, and answers
//     SUCCESS with an empty list.
//   - The KV-config PUT/DELETE requests go to the NAMESERVER and must be
//     BROADCAST to every nameserver (Java putKVConfigValue / deleteKVConfigValue).
//   - ResetOffsetBody.offsetTable is Map<MessageQueue, Long> (fastjson2 inline
//     object keys — decoded via decodeMessageQueueMap).
//   - UPDATE_AND_CREATE_TOPIC(17) MUST carry `topicFilterType`: the broker's
//     CreateTopicRequestHeader.checkFields() rejects a null with
//     `topicFilterType = [null] value invalid`.
import { RemotingCommand } from '../remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../remoting/codes.ts';
import { TopicRouteData } from '../remoting/route.ts';
import {
  ClusterInfo, ConsumerConnection, ConsumeStats, ConsumeStatsList,
  GetConsumerListByGroupResponseBody, GroupList, KVTable, ProducerConnection,
  ResetOffsetBody, TopicConfig, TopicConfigSerializeWrapper, TopicList,
  TopicFilterType, DEFAULT_PERM, messageQueueKey,
  QueryMsgResponseBody, ConsumeMessageDirectlyResult,
} from '../remoting/bodies.ts';
import { TopicStatsTable } from '../remoting/admin_body.ts';
import { RemotingSerializable } from '../remoting/serialize.ts';
import { MixAll } from '../common/mixAll.ts';
import { MessageQueue, MessageExt } from '../common/message.ts';
import { MessageConst } from '../common/messageConst.ts';
import { BoundaryType } from '../common/boundary_type.ts';
import { decodeMessage, decodeMessageId } from '../common/messageDecoder.ts';
import { getLogger } from '../logging.ts';
import { MQClientException, MQBrokerException } from '../remoting/exception.ts';
import { MQClient } from './mq_client.ts';

const logger = getLogger('admin');

// Java DefaultMQAdminExtImpl field: `private long timeoutMillis = 20000;`
export const DefaultAdminTimeoutMillis = 20000;
// Java DefaultMQAdminExt: `private String adminExtGroup = "admin_ext_group";`
export const DefaultAdminExtGroup = 'admin_ext_group';
// Java NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG (kvNamespaceToDeleteList).
export const NAMESPACE_ORDER_TOPIC_CONFIG = 'ORDER_TOPIC_CONFIG';
// Java KeyBuilder.POP_RETRY_SEPARATOR_V1
const POP_RETRY_SEPARATOR_V1 = '_';

// ---------------- java.util.Properties codec ----------------
// Port of Java MixAll.string2Properties / propertiesToString (semantics of
// java.util.Properties.load): blank lines and `#`/`!` comments are skipped, an
// UNESCAPED trailing `\` continues onto the next line, the key ends at the
// first `=`, `:` or whitespace, and the value's trailing whitespace is
// preserved. Broker config exports never contain `\t`/`\n`/`\uXXXX` escapes,
// so (like the Python/Go ports) they are deliberately not expanded.
function splitLines(text: string): string[] {
  return text.split(/\r\n|\n|\r/);
}

export function string2Properties(text: string): Record<string, string> {
  const result: Record<string, string> = {};
  if (!text) return result;
  const propWS = ' \t\f';

  // Merge continuation lines into logical lines.
  const logical: string[] = [];
  let pending = '';
  let hasPending = false;
  for (const raw of splitLines(text)) {
    let line = raw;
    if (hasPending) {
      line = pending + line.replace(new RegExp(`^[${propWS}]+`), '');
      hasPending = false;
    }
    let trailing = 0;
    for (let k = line.length - 1; k >= 0 && line[k] === '\\'; k--) trailing++;
    if (trailing % 2 === 1) {
      pending = line.slice(0, -1);
      hasPending = true;
      continue;
    }
    logical.push(line);
  }
  if (hasPending) logical.push(pending);

  for (const line of logical) {
    const stripped = line.trim();
    if (stripped === '' || stripped[0] === '#' || stripped[0] === '!') continue;
    const n = line.length;
    let pos = 0;
    while (pos < n && propWS.includes(line[pos])) pos++;
    const keyStart = pos;
    while (pos < n && line[pos] !== '=' && line[pos] !== ':' && !propWS.includes(line[pos])) pos++;
    const key = line.slice(keyStart, pos);
    while (pos < n && propWS.includes(line[pos])) pos++;
    let value = '';
    if (pos < n && (line[pos] === '=' || line[pos] === ':')) {
      pos++;
      while (pos < n && propWS.includes(line[pos])) pos++;
      value = line.slice(pos);
    }
    if (key) result[key] = value;
  }
  return result;
}

function propertiesToString(properties: Record<string, string>): string {
  const keys = Object.keys(properties).sort();
  return keys.map((k) => `${k}=${properties[k]}\n`).join('');
}

function sameDataVersion(a: Record<string, any> | null, b: Record<string, any> | null): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

// Renders ext values the way Java RemotingCommand.makeCustomHeaderToNet does:
// String.valueOf for numbers, "true"/"false" for booleans. Keys written here
// MUST be the Java FIELD names — the broker reads them back by reflection
// (the `cluster`-not-`clusterName` trap lives exactly here).
function extValue(v: any): string {
  if (v == null) return '';
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  return String(v);
}

function adminExt(kv: Record<string, any>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(kv)) out[k] = extValue(v);
  return out;
}

export interface RollbackStats {
  brokerName: string;
  queueId: number;
  brokerOffset: number;
  consumerOffset: number;
  timestampOffset: number;
  rollbackOffset: number;
}

export class DefaultMQAdminExt {
  adminExtGroup: string;
  createTopicKey: string;
  namesrvAddr: string | null = null;
  instanceName: string = 'ADMIN';
  unitName: string | null = null;
  enableStreamRequestType = false;
  // Java ClientConfig#vipChannelEnabled — false by default in 5.x. When true,
  // broker requests go to the VIP port (port - 2); NEVER applied to the
  // nameserver.
  vipChannelEnabled = false;
  pollNameServerIntervalMillis = 30000;
  rpcHook: any;
  timeoutMillis: number;
  // Java: kvNamespaceToDeleteList = [NAMESPACE_ORDER_TOPIC_CONFIG]; cleaned up
  // when a topic is deleted.
  kvNamespaceToDeleteList: string[] = [NAMESPACE_ORDER_TOPIC_CONFIG];

  client: MQClient | null;
  private _started = false;

  constructor(rpcHook: any = null, timeoutMillis: number = DefaultAdminTimeoutMillis,
    adminExtGroup: string = DefaultAdminExtGroup) {
    this.rpcHook = rpcHook;
    this.timeoutMillis = timeoutMillis;
    this.adminExtGroup = adminExtGroup;
    this.createTopicKey = MixAll.DEFAULT_TOPIC;
    this.client = null;
  }

  setNamesrvAddr(addr: string): void { this.namesrvAddr = addr; }
  setNameServerAddressList(addrs: string[]): void { this.namesrvAddr = addrs.join(';'); }
  setInstanceName(name: string): void { this.instanceName = name; }
  setUnitName(unitName: string): void { this.unitName = unitName; }
  setVipChannelEnabled(enable: boolean): void { this.vipChannelEnabled = enable; }
  setTimeoutMillis(timeoutMillis: number): void { this.timeoutMillis = timeoutMillis; }
  getNameServerAddressList(): string[] {
    return this.client != null ? this.client.getNameServerAddressList() : [];
  }

  // Java DefaultMQAdminExt#changeInstanceNameToPID: only the "ADMIN" default
  // becomes pid-unique (instanceName = "ADMIN#<pid>").
  private _changeInstanceNameToPID(): void {
    if (this.instanceName === 'ADMIN') {
      this.instanceName = `ADMIN#${process.pid}`;
    }
  }

  start(): void {
    if (this._started) return;
    if (!this.namesrvAddr) {
      throw new MQClientException('name server address is not set, call setNamesrvAddr() first');
    }
    // Java start():161 unconditionally calls changeInstanceNameToPID, after
    // which the clientId goes through ClientConfig#buildMQClientId.
    this._changeInstanceNameToPID();
    const clientId = MixAll.clientIdFor(this.instanceName, this.unitName, this.enableStreamRequestType);
    const remotingClient = this.client != null ? this.client.remotingClient : null;
    this.client = new MQClient(clientId, this.namesrvAddr, remotingClient);
    if (this.rpcHook != null) {
      this.client.remotingClient.registerRpcHook(this.rpcHook);
    }
    this.client.start();
    this._started = true;
    logger.info(`adminExt ${this.adminExtGroup} started, clientId=${clientId}`);
  }

  shutdown(): void {
    if (!this._started) return;
    this._started = false;
    if (this.client != null) this.client.shutdown();
    this.client = null;
    logger.info(`adminExt ${this.adminExtGroup} shutdown OK`);
  }

  private _requireClient(): MQClient {
    if (!this._started || this.client == null) {
      throw new MQClientException('admin not started, call start() first');
    }
    return this.client;
  }

  private _timeout(millis?: number): number {
    if (millis != null && millis > 0) return millis;
    return this.timeoutMillis > 0 ? this.timeoutMillis : DefaultAdminTimeoutMillis;
  }

  // ---------------- low-level invoke helpers ----------------

  private _vipChannel(addr: string): string {
    return MixAll.brokerVipChannel(this.vipChannelEnabled, addr);
  }

  private _request(code: number, ext: Record<string, string> | null, body: Buffer | null = null): RemotingCommand {
    const request = RemotingCommand.createRequestCommand(code, null);
    if (ext != null) {
      for (const [k, v] of Object.entries(ext)) request.addExtField(k, v);
    }
    if (body != null) request.body = body;
    return request;
  }

  // Sends to one broker (with VIP translation) and raises on non-SUCCESS.
  private async _invokeBroker(addr: string, code: number, ext: Record<string, string> | null,
    body: Buffer | null = null, timeoutMillis?: number): Promise<RemotingCommand> {
    const client = this._requireClient();
    const target = this._vipChannel(addr);
    const response = await client.remotingClient.invokeSync(target, this._request(code, ext, body),
      this._timeout(timeoutMillis));
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQBrokerException(response.code, response.remark || `CODE: ${response.code}`, target);
    }
    return response;
  }

  // Broadcasts to EVERY nameserver (Java putKVConfigValue / deleteKVConfigValue)
  // and raises if any of them failed.
  private async _invokeNameServerAll(code: number, ext: Record<string, string> | null,
    timeoutMillis?: number): Promise<void> {
    const client = this._requireClient();
    const request = this._request(code, ext);
    let errResponse: RemotingCommand | null = null;
    for (const nsAddr of client.getNameServerAddressList()) {
      const response = await client.remotingClient.invokeSync(nsAddr, request, this._timeout(timeoutMillis));
      if (response.code !== ResponseCode.SUCCESS) errResponse = response;
    }
    if (errResponse != null) {
      throw new MQBrokerException(errResponse.code,
        errResponse.remark || 'put/delete kv config failed');
    }
  }

  // Sends to the FIRST nameserver that answers (Java invokeSync(null, ...)
  // semantics); the response is returned unchecked so callers can branch.
  private async _invokeNameServerOne(code: number, ext: Record<string, string> | null,
    timeoutMillis?: number): Promise<RemotingCommand> {
    const client = this._requireClient();
    const request = this._request(code, ext);
    let lastErr: Error | null = null;
    for (const nsAddr of client.getNameServerAddressList()) {
      try {
        return await client.remotingClient.invokeSync(nsAddr, request, this._timeout(timeoutMillis));
      } catch (e) {
        lastErr = e as Error;
      }
    }
    throw new MQClientException(`all name servers unreachable: ${lastErr != null ? lastErr.message : ''}`);
  }

  // Java examineTopicRouteInfo → MQClientAPIImpl.getTopicRouteInfoFromNameServer.
  // NOTE: NO default-topic fallback here — the admin must surface TOPIC_NOT_EXIST
  // (the client's updateTopicRouteInfoFromNameServer falls back to TBW102, which
  // would silently mask a missing topic).
  async examineTopicRouteInfo(topic: string, timeoutMillis?: number): Promise<TopicRouteData> {
    const client = this._requireClient();
    const timeout = this._timeout(timeoutMillis);
    const ns = client.getNameServerAddressList()[0];
    if (ns == null) throw new MQClientException('No name server address, please set it first.');
    const request = this._request(RequestCode.GET_ROUTEINFO_BY_TOPIC, adminExt({ topic }));
    const response = await client.remotingClient.invokeSync(ns, request, timeout);
    if (response.code !== ResponseCode.SUCCESS) {
      const e = new MQClientException(`Not exist route info for this topic: ${topic}`);
      (e as any).responseCode = ResponseCode.TOPIC_NOT_EXIST;
      throw e;
    }
    const routeData = TopicRouteData.decode(response.body as Buffer);
    // Cache into the client's route/broker-addr tables (Java
    // MQClientInstance.updateTopicRouteInfoFromNameServer does the same).
    client.topicRouteData2TopicPublishInfo(topic, routeData);
    return routeData;
  }

  // ---------------- Topic management ----------------

  // Java MQAdminImpl.createTopic: push the topic to every MASTER in the
  // createTopicKey (TBW102) route with READ|WRITE perm. Succeeding on at least
  // one broker is enough.
  async createTopic(key: string, newTopic: string, queueNum: number, topicSysFlag = 0,
    attributes: string = ''): Promise<void> {
    const client = this._requireClient();
    let route = client.getTopicRouteData(key);
    if (route == null) {
      await client.updateTopicRouteInfoFromNameServer(key, true);
      route = client.getTopicRouteData(key);
    }
    if (route == null) {
      throw new MQClientException(`No route info of default topic ${key}`);
    }
    if (queueNum <= 0) queueNum = MixAll.DEFAULT_TOPIC_QUEUE_NUMS;
    const perm = MixAll.READ_PERM_BY_DEFAULT; // PermName.PERM_READ | PERM_WRITE
    let created = false;
    let lastErr: any = null;
    for (const bd of route.brokerDatas) {
      const addr = bd.selectBrokerAddr();
      if (addr == null) continue;
      try {
        await this._createTopicInBroker(addr, key, newTopic, queueNum, queueNum, perm,
          topicSysFlag, TopicFilterType.SINGLE_TAG, false, attributes);
        created = true;
      } catch (e) {
        lastErr = e;
      }
    }
    if (!created && lastErr != null) {
      throw new MQClientException(`create new topic failed: ${lastErr.message}`);
    }
  }

  // UPDATE_AND_CREATE_TOPIC(17) to one broker. `topicFilterType` MUST be sent
  // (broker rejects null); only transport errors are retried — a broker
  // rejection is final (Java rethrows MQBrokerException out of the retry loop).
  private async _createTopicInBroker(brokerAddr: string, defaultTopic: string, topic: string,
    readQueueNums: number, writeQueueNums: number, perm: number, topicSysFlag: number,
    topicFilterType: string, order: boolean, attributes: string): Promise<void> {
    const retryTimes = 5; // MQAdminImpl.createTopic's per-broker retry count
    let lastErr: Error | null = null;
    for (let attempt = 0; attempt < retryTimes; attempt++) {
      try {
        await this._invokeBroker(brokerAddr, RequestCode.UPDATE_AND_CREATE_TOPIC, adminExt({
          topic,
          defaultTopic,
          readQueueNums,
          writeQueueNums,
          perm,
          topicFilterType,
          topicSysFlag,
          order,
          // Java AttributeParser.parseToString(map) — an empty map renders as
          // "", not null.
          attributes,
          force: false,
        }));
        return;
      } catch (e) {
        if (e instanceof MQBrokerException) throw e; // broker rejection is final
        lastErr = e as Error;
      }
    }
    if (lastErr != null) throw lastErr;
  }

  // Java createAndUpdateTopicConfig → MQClientAPIImpl.createTopic(addr,
  // createTopicKey, config, timeout).
  async createAndUpdateTopicConfig(addr: string, config: TopicConfig): Promise<void> {
    await this._createTopicInBroker(addr, this.createTopicKey, config.topicName,
      config.readQueueNums, config.writeQueueNums, config.perm,
      config.topicSysFlag, config.topicFilterType || TopicFilterType.SINGLE_TAG,
      config.order === true, config.attributes != null ? JSON.stringify(config.attributes) : '');
  }

  // DELETE_TOPIC_IN_BROKER(215) for each addr.
  async deleteTopicInBroker(addrs: string[], topic: string): Promise<void> {
    for (const addr of addrs) {
      await this._invokeBroker(addr, RequestCode.DELETE_TOPIC_IN_BROKER, adminExt({ topic }));
    }
  }

  // DELETE_TOPIC_IN_NAMESRV(216) for each addr; addrs == null → all nameservers
  // (Java fetchNameServerAddr).
  async deleteTopicInNameServer(addrs: string[] | null, topic: string): Promise<void> {
    const client = this._requireClient();
    const targets = addrs != null ? addrs : client.getNameServerAddressList();
    for (const nsAddr of targets) {
      await this._invokeBroker(nsAddr, RequestCode.DELETE_TOPIC_IN_NAMESRV, adminExt({ topic }), null, 5000);
    }
  }

  // Java deleteTopic: delete from ALL brokers (master AND slave, per
  // CommandUtil.fetchMasterAndSlaveAddrByClusterName), then the nameserver
  // route, then the KV namespaces (ORDER_TOPIC_CONFIG).
  async deleteTopic(topic: string, clusterName: string): Promise<void> {
    const client = this._requireClient();
    const clusterInfo = await this.examineBrokerClusterInfo();
    const names = clusterInfo.clusterAddrTable[clusterName];
    if (names == null || names.length === 0) {
      throw new MQClientException(`The cluster [${clusterName}] not exist`);
    }
    // master AND slave addrs (Java fetchMasterAndSlaveAddrByClusterName)
    const addrs: string[] = [];
    for (const brokerName of names) {
      const inner = clusterInfo.brokerAddrTable[brokerName];
      if (inner != null) addrs.push(...Object.values(inner));
    }
    await this.deleteTopicInBroker(addrs, topic);
    await this.deleteTopicInNameServer(null, topic);
    for (const namespace of this.kvNamespaceToDeleteList) {
      await this.deleteKvConfig(namespace, topic);
    }
    void client;
  }

  // Java fetchAllTopicList → getTopicListFromNameServer(timeoutMillis).
  async fetchAllTopicList(timeoutMillis?: number): Promise<TopicList> {
    const response = await this._invokeNameServerOne(RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, null,
      timeoutMillis);
    if (response.body == null || (response.body as Buffer).length === 0) {
      const t = new TopicList();
      return t;
    }
    return TopicList.decode(response.body as Buffer);
  }

  // GET_TOPICS_BY_CLUSTER(224). The ext key MUST be Java's `cluster`; with
  // `clusterName` the nameserver NPEs internally, swallows it, and answers
  // SUCCESS with an empty list.
  async fetchTopicsByCluster(clusterName: string, timeoutMillis?: number): Promise<Set<string>> {
    const response = await this._invokeNameServerOne(RequestCode.GET_TOPICS_BY_CLUSTER,
      adminExt({ cluster: clusterName }), timeoutMillis);
    const topics = new Set<string>();
    if (response.code === ResponseCode.SUCCESS && response.body != null) {
      const obj = RemotingSerializable.decode(response.body as Buffer) as Record<string, any>;
      for (const item of (obj['topicList'] || []) as any[]) {
        if (typeof item === 'string') topics.add(item);
      }
    }
    return topics;
  }

  // Java examineTopicStats(topic): merge GET_TOPIC_STATS_INFO(202) across the
  // topic's brokers. Java's sync version does NOT sum topicPutTps (only the
  // *Concurrent variant does) — kept faithful.
  async examineTopicStats(topic: string): Promise<TopicStatsTable> {
    const route = await this.examineTopicRouteInfo(topic);
    const merged = new TopicStatsTable();
    for (const bd of route.brokerDatas) {
      const addr = bd.selectBrokerAddr();
      if (addr == null) continue;
      try {
        const part = await this.examineTopicStatsByBroker(addr, topic);
        for (const [q, off] of part.offsetTable) merged.offsetTable.set(q, off);
      } catch (e) {
        logger.warn(`getTopicStatsInfo error. topic=${topic} broker=${addr}: ${(e as Error).message}`);
      }
    }
    if (merged.offsetTable.size === 0) {
      throw new MQClientException('Not found the topic stats info');
    }
    return merged;
  }

  async examineTopicStatsByBroker(brokerAddr: string, topic: string): Promise<TopicStatsTable> {
    const response = await this._invokeBroker(brokerAddr, RequestCode.GET_TOPIC_STATS_INFO,
      adminExt({ topic }));
    return TopicStatsTable.decode(response.body as Buffer);
  }

  // ---------------- Cluster / broker ----------------

  // GET_BROKER_CLUSTER_INFO(106), first nameserver that answers.
  async examineBrokerClusterInfo(timeoutMillis?: number): Promise<ClusterInfo> {
    const response = await this._invokeNameServerOne(RequestCode.GET_BROKER_CLUSTER_INFO, null,
      timeoutMillis);
    return ClusterInfo.decode(response.body as Buffer);
  }

  // GET_BROKER_RUNTIME_INFO(28), KVTable body.
  async fetchBrokerRuntimeStats(brokerAddr: string, timeoutMillis?: number): Promise<KVTable> {
    const response = await this._invokeBroker(brokerAddr, RequestCode.GET_BROKER_RUNTIME_INFO, null,
      null, timeoutMillis);
    return KVTable.decode(response.body as Buffer);
  }

  // GET_BROKER_CONFIG(26). The body is java.util.Properties TEXT, NOT JSON —
  // a client that parses it as a KVTable fails on every real broker.
  async getBrokerConfig(brokerAddr: string, timeoutMillis?: number): Promise<Record<string, string>> {
    const response = await this._invokeBroker(brokerAddr, RequestCode.GET_BROKER_CONFIG, null,
      null, timeoutMillis);
    return string2Properties((response.body as Buffer).toString('utf8'));
  }

  // UPDATE_BROKER_CONFIG(25), body = `k=v\n` text.
  async updateBrokerConfig(brokerAddr: string, properties: Record<string, string>,
    timeoutMillis?: number): Promise<void> {
    const text = propertiesToString(properties);
    if (!text) return;
    await this._invokeBroker(brokerAddr, RequestCode.UPDATE_BROKER_CONFIG, null,
      Buffer.from(text, 'utf8'), timeoutMillis);
  }

  // WIPE_WRITE_PERM_OF_BROKER(205) — returns the number of topics affected.
  async wipeWritePermOfBroker(namesrvAddr: string, brokerName: string): Promise<number> {
    const response = await this._invokeBroker(namesrvAddr, RequestCode.WIPE_WRITE_PERM_OF_BROKER,
      adminExt({ brokerName }));
    const v = response.extFields ? response.extFields['wipeTopicCount'] : null;
    return v != null ? parseInt(v, 10) : 0;
  }

  // ADD_WRITE_PERM_OF_BROKER(327) — returns the number of topics affected.
  async addWritePermOfBroker(namesrvAddr: string, brokerName: string): Promise<number> {
    const response = await this._invokeBroker(namesrvAddr, RequestCode.ADD_WRITE_PERM_OF_BROKER,
      adminExt({ brokerName }));
    const v = response.extFields ? response.extFields['addTopicCount'] : null;
    return v != null ? parseInt(v, 10) : 0;
  }

  // CLEAN_UNUSED_TOPIC(316) on one broker.
  async cleanUnusedTopicByAddr(addr: string): Promise<boolean> {
    await this._invokeBroker(addr, RequestCode.CLEAN_UNUSED_TOPIC, null);
    return true;
  }

  // Java cleanUnusedTopic(cluster): every broker of the cluster; the last
  // result wins.
  async cleanUnusedTopic(cluster: string): Promise<boolean> {
    const clusterInfo = await this.examineBrokerClusterInfo();
    const clusters = cluster ? [cluster] : Object.keys(clusterInfo.clusterAddrTable);
    let result = false;
    for (const c of clusters) {
      for (const addr of clusterInfo.clusterAddrTable[c] || []) {
        const inner = clusterInfo.brokerAddrTable[addr];
        const addrs = inner != null ? Object.values(inner) : [];
        for (const a of addrs) result = await this.cleanUnusedTopicByAddr(a);
      }
    }
    return result;
  }

  // CLEAN_EXPIRED_CONSUMEQUEUE(306).
  async cleanExpiredConsumerQueueByAddr(addr: string): Promise<boolean> {
    await this._invokeBroker(addr, RequestCode.CLEAN_EXPIRED_CONSUMEQUEUE, null);
    return true;
  }

  // DELETE_EXPIRED_COMMITLOG(329).
  async deleteExpiredCommitLogByAddr(addr: string): Promise<boolean> {
    await this._invokeBroker(addr, RequestCode.DELETE_EXPIRED_COMMITLOG, null);
    return true;
  }

  // VIEW_BROKER_STATS_DATA(315) — raw JSON object.
  async viewBrokerStatsData(brokerAddr: string, statsName: string, statsKey: string): Promise<Record<string, any>> {
    const response = await this._invokeBroker(brokerAddr, RequestCode.VIEW_BROKER_STATS_DATA,
      adminExt({ statsName, statsKey }));
    return RemotingSerializable.decode(response.body as Buffer) as Record<string, any>;
  }

  // Java getClusterList(topic): the clusters whose brokers appear in the
  // topic's route.
  async getClusterList(topic: string): Promise<Set<string>> {
    const clusterInfo = await this.examineBrokerClusterInfo();
    const route = await this.examineTopicRouteInfo(topic);
    const brokerNames = new Set(route.brokerDatas.map((bd) => bd.brokerName));
    const clusters = new Set<string>();
    for (const [clusterName, names] of Object.entries(clusterInfo.clusterAddrTable)) {
      if (names.some((n) => brokerNames.has(n))) clusters.add(clusterName);
    }
    return clusters;
  }

  // Java getTopicClusterList(topic): uses the FIRST BrokerData's brokerName
  // only (faithful to the Java quirk).
  async getTopicClusterList(topic: string): Promise<Set<string>> {
    const clusterInfo = await this.examineBrokerClusterInfo();
    const route = await this.examineTopicRouteInfo(topic);
    const clusterSet = new Set<string>();
    if (route.brokerDatas.length === 0) return clusterSet;
    const brokerName = route.brokerDatas[0].brokerName;
    for (const [clusterName, names] of Object.entries(clusterInfo.clusterAddrTable)) {
      if (names.includes(brokerName)) clusterSet.add(clusterName);
    }
    return clusterSet;
  }

  // ---------------- Topic config (broker side) ----------------

  // GET_ALL_TOPIC_CONFIG(21) on one broker.
  async getAllTopicConfig(brokerAddr: string, timeoutMillis?: number): Promise<TopicConfigSerializeWrapper> {
    const response = await this._invokeBroker(brokerAddr, RequestCode.GET_ALL_TOPIC_CONFIG, null,
      null, timeoutMillis);
    return TopicConfigSerializeWrapper.decode(response.body as Buffer);
  }

  // GET_SYSTEM_TOPIC_LIST_FROM_BROKER(305).
  async getSystemTopicListFromBroker(brokerAddr: string, timeoutMillis?: number): Promise<TopicList> {
    const response = await this._invokeBroker(brokerAddr, RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER,
      null, null, timeoutMillis);
    return TopicList.decode(response.body as Buffer);
  }

  // Java getUserTopicConfig: drops system topics and (unless specialTopic) the
  // %RETRY% / %DLQ% topics, and topics with an invalid perm (Java
  // PermName.isValid: perm >= 0 && perm < PERM_PRIORITY=8).
  async getUserTopicConfig(brokerAddr: string, specialTopic: boolean,
    timeoutMillis?: number): Promise<TopicConfigSerializeWrapper> {
    const wrapper = await this.getAllTopicConfig(brokerAddr, timeoutMillis);
    const sysList = await this.getSystemTopicListFromBroker(brokerAddr, timeoutMillis);
    const sysTopics = new Set(sysList.topicList);
    const kept: Record<string, TopicConfig> = {};
    for (const [name, cfg] of Object.entries(wrapper.topicConfigTable)) {
      if (sysTopics.has(name) || MixAll.isSysTopic(name)) continue;
      if (!specialTopic && (MixAll.isRetryTopic(name) || MixAll.isDlqTopic(name))) continue;
      if (!(cfg.perm >= 0 && cfg.perm < 8)) continue; // PermName.isValid
      kept[name] = cfg;
    }
    wrapper.topicConfigTable = kept;
    return wrapper;
  }

  // GET_TOPIC_CONFIG(351) — sends `topic` + `lo`, body is a plain TopicConfig
  // JSON.
  async examineTopicConfig(addr: string, topic: string): Promise<TopicConfig> {
    const response = await this._invokeBroker(addr, RequestCode.GET_TOPIC_CONFIG,
      adminExt({ topic, lo: true }));
    if (response.body == null || (response.body as Buffer).length === 0) {
      throw new MQBrokerException(ResponseCode.SYSTEM_ERROR, `empty topic config for ${topic}`, addr);
    }
    return TopicConfig.decode(response.body as Buffer);
  }

  // ---------------- Nameserver KV config ----------------

  // PUT_KV_CONFIG(100), BROADCAST to every nameserver (Java putKVConfigValue).
  async createAndUpdateKvConfig(namespace: string, key: string, value: string): Promise<void> {
    await this._invokeNameServerAll(RequestCode.PUT_KV_CONFIG,
      adminExt({ namespace, key, value }));
  }

  async putKVConfig(namespace: string, key: string, value: string): Promise<void> {
    await this.createAndUpdateKvConfig(namespace, key, value);
  }

  // GET_KV_CONFIG(101) — ONE nameserver.
  async getKVConfig(namespace: string, key: string): Promise<string | null> {
    const response = await this._invokeNameServerOne(RequestCode.GET_KV_CONFIG,
      adminExt({ namespace, key }));
    if (response.code === ResponseCode.SUCCESS && response.extFields) {
      return response.extFields['value'] != null ? response.extFields['value'] : null;
    }
    return null;
  }

  // DELETE_KV_CONFIG(102), BROADCAST to every nameserver.
  async deleteKvConfig(namespace: string, key: string): Promise<void> {
    await this._invokeNameServerAll(RequestCode.DELETE_KV_CONFIG, adminExt({ namespace, key }));
  }

  // GET_KVLIST_BY_NAMESPACE(219).
  async getKVListByNamespace(namespace: string): Promise<KVTable> {
    const response = await this._invokeNameServerOne(RequestCode.GET_KVLIST_BY_NAMESPACE,
      adminExt({ namespace }));
    return KVTable.decode(response.body as Buffer);
  }

  // ---------------- Subscription groups ----------------

  // UPDATE_AND_CREATE_SUBSCRIPTIONGROUP(200); the body is a
  // SubscriptionGroupConfig JSON object (plain RemotingSerializable JSON).
  async createAndUpdateSubscriptionGroupConfig(addr: string, config: Record<string, any>): Promise<void> {
    await this._invokeBroker(addr, RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, null,
      RemotingSerializable.encode(config));
  }

  // GET_ALL_SUBSCRIPTIONGROUP_CONFIG(201), PAGED: accumulate until
  // groupSeq >= totalGroupNum-1. A broker that does not send `totalGroupNum`
  // (older versions) returns everything in one round.
  async getAllSubscriptionGroup(brokerAddr: string, timeoutMillis?: number): Promise<Record<string, any>> {
    const client = this._requireClient();
    const timeout = this._timeout(timeoutMillis);
    let currentDataVersion: Record<string, any> | null = null;
    let groupSeq = 0;
    const table: Record<string, any> = {};
    const forbidden: Record<string, any> = {};
    const begin = Date.now();

    for (;;) {
      const left = timeout - (Date.now() - begin);
      if (left < 0) throw new MQClientException('invokeSync call timeout');
      const ext: Record<string, string> = adminExt({ groupSeq, maxGroupNum: 10000 });
      if (currentDataVersion != null) {
        ext['dataVersion'] = JSON.stringify(currentDataVersion);
      }
      const response = await client.remotingClient.invokeSync(brokerAddr,
        this._request(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, ext), left);
      if (response.code !== ResponseCode.SUCCESS) {
        throw new MQBrokerException(response.code, response.remark || '', brokerAddr);
      }
      const wrapper = RemotingSerializable.decode(response.body as Buffer) as Record<string, any>;
      const subTable = (wrapper['subscriptionGroupTable'] || {}) as Record<string, any>;
      for (const [k, v] of Object.entries(subTable)) table[k] = v;
      for (const [k, v] of Object.entries((wrapper['forbiddenTable'] || {}) as Record<string, any>)) {
        forbidden[k] = v;
      }
      const newVersion = (wrapper['dataVersion'] || null) as Record<string, any> | null;
      if (currentDataVersion == null) currentDataVersion = newVersion;
      groupSeq += Object.keys(subTable).length;

      const totalText = response.extFields ? response.extFields['totalGroupNum'] : null;
      if (totalText == null) break; // older broker: one round returns everything
      const total = parseInt(totalText, 10);
      if (Number.isNaN(total)) break;
      if (!sameDataVersion(currentDataVersion, newVersion)) {
        logger.warn('subscription group dataVersion changed, restart paging');
        currentDataVersion = newVersion;
        groupSeq = 0;
        for (const k of Object.keys(table)) delete table[k];
        for (const k of Object.keys(forbidden)) delete forbidden[k];
        continue;
      }
      if (groupSeq >= total - 1) break;
    }

    return {
      dataVersion: currentDataVersion != null ? currentDataVersion : {},
      forbiddenTable: forbidden,
      subscriptionGroupTable: table,
    };
  }

  // Drops system (CID_RMQ_SYS_) and predefined groups (Java MixAll filters).
  async getUserSubscriptionGroup(brokerAddr: string, timeoutMillis?: number): Promise<Record<string, any>> {
    const wrapper = await this.getAllSubscriptionGroup(brokerAddr, timeoutMillis);
    const kept: Record<string, any> = {};
    for (const [k, v] of Object.entries(wrapper.subscriptionGroupTable)) {
      if (MixAll.isSysConsumerGroup(k) || MixAll.isPredefinedGroup(k)) continue;
      kept[k] = v;
    }
    wrapper.subscriptionGroupTable = kept;
    return wrapper;
  }

  // Java examineSubscriptionGroupConfig: read the group out of the full
  // wrapper (NOT the GET_SUBSCRIPTIONGROUP_CONFIG(352) call — that one is
  // broker-version dependent).
  async examineSubscriptionGroupConfig(addr: string, group: string): Promise<Record<string, any> | null> {
    const wrapper = await this.getAllSubscriptionGroup(addr);
    return wrapper.subscriptionGroupTable[group] != null ? wrapper.subscriptionGroupTable[group] : null;
  }

  // DELETE_SUBSCRIPTIONGROUP(207).
  async deleteSubscriptionGroup(addr: string, groupName: string, removeOffset = false): Promise<void> {
    await this._invokeBroker(addr, RequestCode.DELETE_SUBSCRIPTIONGROUP,
      adminExt({ groupName, cleanOffset: removeOffset }));
  }

  // ---------------- Consumer / producer connections ----------------

  // GET_CONSUMER_CONNECTION_LIST(203). brokerAddr == null → a RANDOM broker of
  // the %RETRY%<group> route (Java: brokers.get(random.nextInt(size))).
  // Empty connectionSet → CONSUMER_NOT_ONLINE.
  async examineConsumerConnectionInfo(consumerGroup: string, brokerAddr: string | null = null): Promise<ConsumerConnection> {
    let addr = brokerAddr;
    if (addr == null) {
      const route = await this.examineTopicRouteInfo(MixAll.getRetryTopic(consumerGroup));
      const brokers = route.brokerDatas;
      if (brokers.length === 0) {
        const e = new MQClientException('Not found the consumer group connection');
        (e as any).responseCode = ResponseCode.CONSUMER_NOT_ONLINE;
        throw e;
      }
      addr = brokers[Math.floor(Math.random() * brokers.length)].selectBrokerAddr();
      if (addr == null) {
        const e = new MQClientException('Not found the consumer group connection');
        (e as any).responseCode = ResponseCode.CONSUMER_NOT_ONLINE;
        throw e;
      }
    }
    const response = await this._invokeBroker(addr, RequestCode.GET_CONSUMER_CONNECTION_LIST,
      adminExt({ consumerGroup }));
    const result = ConsumerConnection.decode(response.body as Buffer);
    if (result.connectionSet.length === 0) {
      logger.warn(`the consumer group not online. brokerAddr=${addr}, group=${consumerGroup}`);
      const e = new MQClientException('Not found the consumer group connection');
      (e as any).responseCode = ResponseCode.CONSUMER_NOT_ONLINE;
      throw e;
    }
    return result;
  }

  // GET_PRODUCER_CONNECTION_LIST(204) — random broker of the TOPIC route.
  async examineProducerConnectionInfo(producerGroup: string, topic: string): Promise<ProducerConnection> {
    const route = await this.examineTopicRouteInfo(topic);
    const brokers = route.brokerDatas;
    if (brokers.length === 0) {
      throw new MQClientException('Not found the producer group connection');
    }
    const addr = brokers[Math.floor(Math.random() * brokers.length)].selectBrokerAddr();
    if (addr == null) throw new MQClientException('Not found the producer group connection');
    const response = await this._invokeBroker(addr, RequestCode.GET_PRODUCER_CONNECTION_LIST,
      adminExt({ producerGroup }));
    const result = ProducerConnection.decode(response.body as Buffer);
    if (result.connectionSet.length === 0) {
      logger.warn(`the producer group not online. brokerAddr=${addr}, group=${producerGroup}`);
      throw new MQClientException('Not found the producer group connection');
    }
    return result;
  }

  // GET_CONSUMER_RUNNING_INFO(307) — walks the %RETRY%<group> route IN ORDER
  // and asks the first broker with a master addr (Java getConsumerRunningInfo).
  async getConsumerRunningInfo(consumerGroup: string, clientId: string,
    jstack = false, metrics = false): Promise<any | null> {
    void metrics;
    const topic = MixAll.getRetryTopic(consumerGroup);
    const route = await this.examineTopicRouteInfo(topic);
    for (const bd of route.brokerDatas) {
      const addr = bd.selectBrokerAddr();
      if (addr == null) continue;
      const response = await this._invokeBroker(addr, RequestCode.GET_CONSUMER_RUNNING_INFO,
        adminExt({ consumerGroup, clientId, jstackEnable: jstack }));
      if (response.body == null || (response.body as Buffer).length === 0) return null;
      const { ConsumerRunningInfo } = await import('../remoting/bodies.ts');
      return ConsumerRunningInfo.decode(response.body as Buffer);
    }
    return null;
  }

  // GET_CONSUMER_LIST_BY_GROUP(38) — brokerAddr is required (Java's
  // MQAdminExt#examineConsumerListByGroup shape; pick one via the route).
  async getConsumerListByGroup(consumerGroup: string, brokerAddr: string): Promise<string[]> {
    const response = await this._invokeBroker(brokerAddr, RequestCode.GET_CONSUMER_LIST_BY_GROUP,
      adminExt({ consumerGroup }));
    const body = GetConsumerListByGroupResponseBody.decode(response.body as Buffer);
    return body.consumerIdList;
  }

  // ---------------- Consume stats ----------------

  // GET_CONSUME_STATS(208) against ONE broker (Java examineConsumeStats
  // overload with brokerAddr). timeout is *3 in Java's composite path.
  async examineConsumeStatsByBroker(brokerAddr: string, consumerGroup: string,
    topic: string | null = null, timeoutMillis?: number): Promise<ConsumeStats> {
    const ext: Record<string, string> = { consumerGroup };
    if (topic) ext['topic'] = topic;
    const response = await this._invokeBroker(brokerAddr, RequestCode.GET_CONSUME_STATS, ext,
      null, timeoutMillis != null ? timeoutMillis * 3 : undefined);
    return ConsumeStats.decode(response.body as Buffer);
  }

  // Java examineConsumeStats(group[, topic]): route candidates are the
  // %RETRY%<group> topic, then the topic itself, then the POP retry topic —
  // the first route that resolves wins. Fan out over all brokers, merge the
  // offset tables and sum the tps. An empty table raises CONSUMER_NOT_ONLINE
  // (or BROADCAST_CONSUMPTION when the group consumes in broadcast mode).
  async examineConsumeStats(consumerGroup: string, topic: string | null = null): Promise<ConsumeStats> {
    const routeTopics: string[] = [MixAll.getRetryTopic(consumerGroup)];
    if (topic != null) {
      routeTopics.push(topic);
      // Java KeyBuilder.buildPopRetryTopic: %RETRY%<group>_<topic>
      routeTopics.push(`${MixAll.RETRY_GROUP_TOPIC_PREFIX}${consumerGroup}${POP_RETRY_SEPARATOR_V1}${topic}`);
    }

    let route: TopicRouteData | null = null;
    for (let i = 0; i < routeTopics.length; i++) {
      try {
        route = await this.examineTopicRouteInfo(routeTopics[i]);
        break;
      } catch (e) {
        if (i === routeTopics.length - 1) throw e;
      }
    }

    const result = new ConsumeStats();
    for (const bd of route!.brokerDatas) {
      const addr = bd.selectBrokerAddr();
      if (addr == null) continue;
      const part = await this.examineConsumeStatsByBroker(addr, consumerGroup, topic);
      for (const [q, w] of part.offsetTable) result.offsetTable.set(q, w);
      result.consumeTps += part.consumeTps;
    }

    if (result.offsetTable.size === 0) {
      let broadcast = false;
      try {
        const connection = await this.examineConsumerConnectionInfo(consumerGroup);
        broadcast = (connection as any).messageModel === 'BROADCASTING';
      } catch (e) {
        const err = new MQClientException('Not found the consumer group consume stats, because return '
          + 'offset table is empty, maybe the consumer not online');
        (err as any).responseCode = ResponseCode.CONSUMER_NOT_ONLINE;
        throw err;
      }
      if (broadcast) {
        const err = new MQClientException('Not found the consumer group consume stats, because return '
          + 'offset table is empty, the consumer is under the broadcast mode');
        (err as any).responseCode = ResponseCode.BROADCAST_CONSUMPTION;
        throw err;
      }
    }
    return result;
  }

  // GET_BROKER_CONSUME_STATS(317).
  async fetchConsumeStatsInBroker(brokerAddr: string, isOrder: boolean,
    timeoutMillis?: number): Promise<ConsumeStatsList> {
    const response = await this._invokeBroker(brokerAddr, RequestCode.GET_BROKER_CONSUME_STATS,
      adminExt({ isOrder }), null, timeoutMillis);
    return ConsumeStatsList.decode(response.body as Buffer);
  }

  // QUERY_TOPIC_CONSUME_BY_WHO(300) — FIRST broker of the topic route wins.
  async queryTopicConsumeByWho(topic: string): Promise<GroupList | null> {
    const route = await this.examineTopicRouteInfo(topic);
    for (const bd of route.brokerDatas) {
      const addr = bd.selectBrokerAddr();
      if (addr == null) continue;
      const response = await this._invokeBroker(addr, RequestCode.QUERY_TOPIC_CONSUME_BY_WHO,
        adminExt({ topic }));
      return GroupList.decode(response.body as Buffer);
    }
    return null;
  }

  // QUERY_TOPICS_BY_CONSUMER(343) — fan out over the %RETRY%<group> route and
  // merge (Java's TopicList.topicList is a Set, hence de-duplication).
  async queryTopicsByConsumer(group: string): Promise<TopicList> {
    const retryTopic = MixAll.getRetryTopic(group);
    const route = await this.examineTopicRouteInfo(retryTopic);
    const result = new TopicList();
    const seen = new Set<string>();
    for (const bd of route.brokerDatas) {
      const addr = bd.selectBrokerAddr();
      if (addr == null) continue;
      const response = await this._invokeBroker(addr, RequestCode.QUERY_TOPICS_BY_CONSUMER,
        adminExt({ group }));
      const part = TopicList.decode(response.body as Buffer);
      for (const t of part.topicList) {
        if (!seen.has(t)) { seen.add(t); result.topicList.push(t); }
      }
    }
    return result;
  }

  // QUERY_SUBSCRIPTION_BY_CONSUMER(345) — FIRST broker of the topic route.
  async querySubscription(group: string, topic: string): Promise<Record<string, any> | null> {
    const route = await this.examineTopicRouteInfo(topic);
    for (const bd of route.brokerDatas) {
      const addr = bd.selectBrokerAddr();
      if (addr == null) continue;
      const response = await this._invokeBroker(addr, RequestCode.QUERY_SUBSCRIPTION_BY_CONSUMER,
        adminExt({ group, topic }));
      if (response.body == null || (response.body as Buffer).length === 0) return null;
      return RemotingSerializable.decode(response.body as Buffer) as Record<string, any>;
    }
    return null;
  }

  // INVOKE_BROKER_TO_GET_CONSUMER_STATUS(223) — FIRST broker of the topic
  // route.
  async getConsumeStatus(topic: string, group: string, clientAddr = ''): Promise<Record<string, Record<string, any>>> {
    const route = await this.examineTopicRouteInfo(topic);
    if (route.brokerDatas.length === 0) return {};
    const addr = route.brokerDatas[0].selectBrokerAddr();
    if (addr == null) return {};
    const response = await this._invokeBroker(addr, RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS,
      adminExt({ topic, group, clientAddr }));
    const out: Record<string, Record<string, any>> = {};
    if (response.body == null || (response.body as Buffer).length === 0) return out;
    const obj = RemotingSerializable.decode(response.body as Buffer) as Record<string, any>;
    const table = obj['consumerTable'];
    if (table != null && typeof table === 'object') {
      for (const [k, v] of Object.entries(table)) out[k] = v as Record<string, any>;
    }
    return out;
  }

  // CLONE_GROUP_OFFSET(314) — fan out over the SRC group's retry route.
  async cloneGroupOffset(srcGroup: string, destGroup: string, topic: string, isOffline: boolean): Promise<void> {
    const retryTopic = MixAll.getRetryTopic(srcGroup);
    const route = await this.examineTopicRouteInfo(retryTopic);
    for (const bd of route.brokerDatas) {
      const addr = bd.selectBrokerAddr();
      if (addr == null) continue;
      await this._invokeBroker(addr, RequestCode.CLONE_GROUP_OFFSET, adminExt({
        srcGroup, destGroup, topic, offline: isOffline,
      }));
    }
  }

  // ---------------- Offset reset ----------------

  // INVOKE_BROKER_TO_RESET_OFFSET(222) → ResetOffsetBody (Map<MessageQueue,
  // Long> with fastjson2 inline object keys). isC selects the C-compatible
  // response shape (Java's isC flag).
  async invokeBrokerToResetOffset(brokerAddr: string, topic: string, group: string,
    timestamp: number, isForce: boolean, isC = false): Promise<Map<MessageQueue, number> | null> {
    const ext = adminExt({ topic, consumerGroup: group, timestamp, isForce });
    let code: number = RequestCode.INVOKE_BROKER_TO_RESET_OFFSET;
    if (isC) {
      // Java uses the same request code; the C shape is requested via the
      // `isC` ext field on INVOKE_BROKER_TO_RESET_OFFSET... actually Java's
      // MQClientAPIImpl.invokeBrokerToResetOffset(isC) reuses code 222 and
      // decodes ResetOffsetBodyForC. The request itself is identical.
      code = RequestCode.INVOKE_BROKER_TO_RESET_OFFSET;
      void isC;
    }
    const response = await this._invokeBroker(brokerAddr, code, ext);
    if (response.body == null || (response.body as Buffer).length === 0) return null;
    const body = ResetOffsetBody.decode(response.body as Buffer);
    return body.offsetTable;
  }

  // Java resetOffsetByTimestamp: fan out over the topic route and merge.
  async resetOffsetByTimestamp(topic: string, group: string, timestamp: number,
    isForce = true, clusterName: string | null = null): Promise<Map<MessageQueue, number>> {
    void clusterName; // Java only reroutes via clusterName for LMQ topics
    const route = await this.examineTopicRouteInfo(topic);
    const allOffsetTable = new Map<MessageQueue, number>();
    for (const bd of route.brokerDatas) {
      const addr = bd.selectBrokerAddr();
      if (addr == null) continue;
      const offsetTable = await this.invokeBrokerToResetOffset(addr, topic, group, timestamp, isForce);
      if (offsetTable != null) {
        for (const [mq, off] of offsetTable) allOffsetTable.set(mq, off);
      }
    }
    return allOffsetTable;
  }

  // Java resetOffsetNew: prefer the broker-side reset; when the consumer is
  // offline fall back to the old per-queue UPDATE_CONSUMER_OFFSET path.
  async resetOffsetNew(consumerGroup: string, topic: string, timestamp: number): Promise<void> {
    try {
      await this.resetOffsetByTimestamp(topic, consumerGroup, timestamp, true);
    } catch (e) {
      if ((e as any).responseCode === ResponseCode.CONSUMER_NOT_ONLINE) {
        await this.resetOffsetByTimestampOld(consumerGroup, topic, timestamp, true);
        return;
      }
      throw e;
    }
  }

  // Java resetOffsetByTimestampOld: per queue — when the group consumed the
  // topic, reset each queue it touched; otherwise reset EVERY read queue.
  async resetOffsetByTimestampOld(consumerGroup: string, topic: string, timestamp: number,
    force: boolean): Promise<RollbackStats[]> {
    const route = await this.examineTopicRouteInfo(topic);
    const queueNumsByBroker: Record<string, number> = {};
    for (const qd of route.queueDatas) queueNumsByBroker[qd['brokerName']] = qd['readQueueNums'];

    const rollbackStatsList: RollbackStats[] = [];
    for (const bd of route.brokerDatas) {
      const addr = bd.selectBrokerAddr();
      if (addr == null) continue;
      const consumeStats = await this.examineConsumeStatsByBroker(addr, consumerGroup);
      let hasConsumed = false;
      for (const [queue, offsetWrapper] of consumeStats.offsetTable) {
        if (queue.getTopic() === topic) {
          hasConsumed = true;
          rollbackStatsList.push(
            await this._resetOffsetConsumeOffset(addr, consumerGroup, queue, offsetWrapper, timestamp, force));
        }
      }
      if (!hasConsumed) {
        const topicStatus = await this.examineTopicStatsByBroker(addr, topic);
        const readQueueNums = queueNumsByBroker[bd.brokerName] != null ? queueNumsByBroker[bd.brokerName] : 0;
        for (let i = 0; i < readQueueNums; i++) {
          const queue = new MessageQueue(topic, bd.brokerName, i);
          const to = topicStatus.offsetTable.get(queue);
          const offsetWrapper = {
            brokerOffset: to != null ? to.maxOffset : 0,
            consumerOffset: to != null ? to.minOffset : 0,
          };
          rollbackStatsList.push(
            await this._resetOffsetConsumeOffset(addr, consumerGroup, queue, offsetWrapper as any, timestamp, force));
        }
      }
    }
    return rollbackStatsList;
  }

  private async _resetOffsetConsumeOffset(brokerAddr: string, consumeGroup: string,
    queue: MessageQueue, offsetWrapper: any, timestamp: number, force: boolean): Promise<RollbackStats> {
    const client = this._requireClient();
    let resetOffset: number;
    if (timestamp === -1) {
      resetOffset = await client.getMaxOffset(brokerAddr, queue.getTopic(), queue.getQueueId());
    } else {
      resetOffset = await client.searchOffsetByTimestamp(brokerAddr, queue.getTopic(),
        queue.getQueueId(), timestamp);
    }
    const stats: RollbackStats = {
      brokerName: queue.getBrokerName(),
      queueId: queue.getQueueId(),
      brokerOffset: offsetWrapper.brokerOffset,
      consumerOffset: offsetWrapper.consumerOffset,
      timestampOffset: resetOffset,
      rollbackOffset: offsetWrapper.consumerOffset,
    };
    if (force || resetOffset <= offsetWrapper.consumerOffset) {
      stats.rollbackOffset = resetOffset;
      await client.updateConsumerOffset(brokerAddr, consumeGroup, queue, resetOffset);
    }
    return stats;
  }

  // Java resetOffsetByQueueId: direct UPDATE_CONSUMER_OFFSET, then a
  // broker-side reset of that single queue.
  async resetOffsetByQueueId(brokerAddr: string, consumeGroup: string, topicName: string,
    queueId: number, resetOffset: number): Promise<void> {
    const client = this._requireClient();
    await client.updateConsumerOffset(brokerAddr, consumeGroup,
      new MessageQueue(topicName, '', queueId), resetOffset);
    await this.invokeBrokerToResetOffset(brokerAddr, topicName, consumeGroup, 0, queueId, false);
    void messageQueueKey;
  }

  // ---------------- Offset queries (MQAdminImpl surface) ----------------

  private _masterAddrForMq(mq: MessageQueue): string {
    const client = this._requireClient();
    const addr = client.findBrokerAddressInPublish(mq.getBrokerName());
    if (addr == null) {
      throw new MQClientException(`The broker[${mq.getBrokerName()}] not exist`);
    }
    return addr;
  }

  async maxOffset(mq: MessageQueue): Promise<number> {
    return this._requireClient().getMaxOffset(this._masterAddrForMq(mq), mq.getTopic(), mq.getQueueId());
  }

  async minOffset(mq: MessageQueue): Promise<number> {
    return this._requireClient().getMinOffset(this._masterAddrForMq(mq), mq.getTopic(), mq.getQueueId());
  }

  async searchOffset(mq: MessageQueue, timestamp: number): Promise<number> {
    return this._requireClient().searchOffsetByTimestamp(this._masterAddrForMq(mq), mq.getTopic(),
      mq.getQueueId(), timestamp);
  }

  async earliestMsgStoreTime(mq: MessageQueue): Promise<number> {
    return this._requireClient().getEarliestMsgStoretime(this._masterAddrForMq(mq), mq.getTopic(),
      mq.getQueueId());
  }

  // Java updateConsumeOffset(brokerAddr, group, mq, offset).
  async updateConsumeOffset(brokerAddr: string, consumeGroup: string, mq: MessageQueue,
    offset: number): Promise<void> {
    await this._requireClient().updateConsumerOffset(brokerAddr, consumeGroup, mq, offset);
  }

  // Java examineConsumerOffset → QUERY_CONSUMER_OFFSET(14). Returns the
  // committed offset; `hasOffset` is false when the broker answers
  // QUERY_NOT_FOUND (no offset recorded yet).
  async examineConsumerOffset(consumerGroup: string, mq: MessageQueue): Promise<{ offset: number; hasOffset: boolean }> {
    const brokerAddr = this._masterAddrForMq(mq);
    const r = await this._requireClient().queryConsumerOffset(brokerAddr, consumerGroup, mq);
    return { offset: r.offset, hasOffset: r.found };
  }

  // Java searchOffsetByTimestamp(addr, topic, queueId, timestamp, boundaryType)
  // → SEARCH_OFFSET_BY_TIMESTAMP(29) with the `boundaryType` ext field. The
  // lower/upper helpers mirror Java DefaultMQAdminExt's convenience methods.
  async searchBoundaryOffset(mq: MessageQueue, timestamp: number,
    boundaryType: string = BoundaryType.LOWER): Promise<number> {
    const brokerAddr = this._masterAddrForMq(mq);
    const client = this._requireClient();
    const request = this._request(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, adminExt({
      topic: mq.getTopic(),
      queueId: String(mq.getQueueId()),
      timestamp: String(timestamp),
      boundaryType: BoundaryType.get_type(boundaryType),
    }));
    const response = await client.remotingClient.invokeSync(brokerAddr, request, this._timeout());
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQBrokerException(response.code, response.remark || `CODE: ${response.code}`, brokerAddr);
    }
    const offset = response.extFields != null ? response.extFields['offset'] : null;
    return offset != null ? parseInt(offset, 10) : 0;
  }

  async searchLowerBoundaryOffset(mq: MessageQueue, timestamp: number): Promise<number> {
    return this.searchBoundaryOffset(mq, timestamp, BoundaryType.LOWER);
  }

  async searchUpperBoundaryOffset(mq: MessageQueue, timestamp: number): Promise<number> {
    return this.searchBoundaryOffset(mq, timestamp, BoundaryType.UPPER);
  }

  // ---------------- Message query (QUERY_MESSAGE / VIEW_MESSAGE_BY_ID) ----------------

  // Java queryMessage → QUERY_MESSAGE(12) against one broker's index. The body
  // is a QueryMsgResponseBody: the matched ids (the broker answers ids only).
  // indexType: K (default) / U (uniq key); mirrors Java QueryMessageRequestHeader.
  async queryMessage(brokerAddr: string, topic: string, key: string,
    maxNum: number = 32, beginTimestamp = 0, endTimestamp = 0,
    indexType: string = 'K'): Promise<QueryMsgResponseBody> {
    const response = await this._invokeBroker(brokerAddr, RequestCode.QUERY_MESSAGE, adminExt({
      topic,
      key,
      maxNum: String(maxNum),
      beginTimestamp: String(beginTimestamp),
      endTimestamp: String(endTimestamp),
      indexType,
    }));
    return QueryMsgResponseBody.decode(response.body);
  }

  async queryMessageByKey(brokerAddr: string, topic: string, key: string, maxNum = 32): Promise<QueryMsgResponseBody> {
    return this.queryMessage(brokerAddr, topic, key, maxNum);
  }

  // Java queryMessage(topic, uniqKey): the U-index query, then each hit is
  // fetched through viewMessage. Returns the decoded MessageExt list.
  async queryMessageByUniqKey(brokerAddr: string, topic: string, uniqKey: string,
    maxNum = 32): Promise<any[]> {
    const ids = await this.queryMessage(brokerAddr, topic, uniqKey, maxNum, 0, 0, 'U');
    const out: any[] = [];
    for (const msgId of ids.msgIdList) {
      try {
        out.push(await this.viewMessage(topic, msgId));
      } catch (e) { /* a pruned index hit is skipped, like Java's viewMessage loop */ }
    }
    return out;
  }

  // Java viewMessage → VIEW_MESSAGE_BY_ID(33) sent STRAIGHT to the broker
  // address encoded inside the offset msgId (no route lookup). The body is one
  // 17-segment stored message.
  async viewMessage(topic: string, msgId: string): Promise<MessageExt> {
    const client = this._requireClient();
    const { ip, port, offset } = decodeMessageId(msgId);
    if (port <= 0 || port > 65535) {
      throw new MQClientException(`not a valid offset msgId: ${msgId}`);
    }
    const addr = `${ip}:${port}`;
    const request = this._request(RequestCode.VIEW_MESSAGE_BY_ID, adminExt({
      offset: String(offset),
      topic,
    }));
    const response = await client.remotingClient.invokeSync(addr, request, this._timeout());
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQBrokerException(response.code, response.remark || `CODE: ${response.code}`, addr);
    }
    if (response.body == null || response.body.length === 0) {
      throw new MQClientException(`message not found: ${msgId}`);
    }
    return decodeMessage(response.body, true, true);
  }

  // Java DefaultMQAdminExt.consumeMessageDirectly → CONSUME_MESSAGE_DIRECTLY(309)
  // sent to the broker that STORES the message; the broker relays it to the
  // named client and answers with the client's verdict body. Java resolves the
  // store host through viewMessage and substitutes the OFFSET msgId when the
  // resolved message carries a client uniq key.
  async consumeMessageDirectly(consumerGroup: string, clientId: string, topic: string,
    msgId: string, brokerAddr?: string): Promise<ConsumeMessageDirectlyResult> {
    let addr = brokerAddr;
    let outMsgId = msgId;
    if (addr == null || addr === '') {
      const msg = await this.viewMessage(topic, msgId);
      addr = msg.getStoreHostString();
      if (msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) != null && msg.getOffsetMsgId() != null) {
        outMsgId = msg.getOffsetMsgId();
      }
    }
    const response = await this._invokeBroker(addr!, RequestCode.CONSUME_MESSAGE_DIRECTLY, adminExt({
      consumerGroup,
      clientId,
      msgId: outMsgId,
      topic,
    }));
    if (response.body == null || response.body.length === 0) {
      throw new MQClientException(`no consume result for client ${clientId}`);
    }
    return ConsumeMessageDirectlyResult.decode(response.body);
  }

  // ---------------- Name-server config (Java updateNameServerConfig) ----------------

  // UPDATE_NAMESRV_CONFIG(318) broadcast: the properties ride in the BODY as
  // java.util.Properties TEXT; the first failing nameserver decides the error.
  async updateNameServerConfig(properties: Record<string, string>, timeoutMillis?: number): Promise<void> {
    const text = propertiesToString(properties);
    if (!text) return;
    const client = this._requireClient();
    const request = this._request(RequestCode.UPDATE_NAMESRV_CONFIG, null, Buffer.from(text, 'utf8'));
    let errResponse: RemotingCommand | null = null;
    for (const nsAddr of client.getNameServerAddressList()) {
      const response = await client.remotingClient.invokeSync(nsAddr, request, this._timeout(timeoutMillis));
      if (response.code !== ResponseCode.SUCCESS) errResponse = response;
    }
    if (errResponse != null) {
      throw new MQBrokerException(errResponse.code, errResponse.remark || 'update name server config failed');
    }
  }

  // GET_NAMESRV_CONFIG(319), one request per nameserver (nil/empty = all,
  // Java's default); the Properties-text body is parsed back per server.
  async getNameServerConfig(nameServers: string[] | null, timeoutMillis?: number): Promise<Record<string, Record<string, string>>> {
    const client = this._requireClient();
    const targets = (nameServers != null && nameServers.length > 0)
      ? nameServers
      : client.getNameServerAddressList();
    if (targets == null || targets.length === 0) {
      throw new MQClientException('no name server address available');
    }
    const out: Record<string, Record<string, string>> = {};
    for (const ns of targets) {
      const response = await client.remotingClient.invokeSync(ns,
        this._request(RequestCode.GET_NAMESRV_CONFIG, null), this._timeout(timeoutMillis));
      if (response.code !== ResponseCode.SUCCESS) {
        throw new MQBrokerException(response.code, response.remark || `CODE: ${response.code}`, ns);
      }
      out[ns] = string2Properties(response.body != null ? response.body.toString('utf8') : '');
    }
    return out;
  }

  // ---------------- POP assignment ----------------

  // SET_MESSAGE_REQUEST_MODE(401), body = MessageRequestModeRequestBody JSON.
  async setMessageRequestMode(brokerAddr: string, topic: string, consumerGroup: string,
    mode: string, popShareQueueNum: number, timeoutMillis?: number): Promise<void> {
    await this._invokeBroker(brokerAddr, RequestCode.SET_MESSAGE_REQUEST_MODE, null,
      RemotingSerializable.encode({
        topic, consumerGroup, mode, popShareQueueNum,
      }), timeoutMillis);
  }
}

export default DefaultMQAdminExt;
