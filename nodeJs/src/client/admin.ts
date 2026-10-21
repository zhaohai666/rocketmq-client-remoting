// -*- coding: utf-8 -*-
// DefaultMQAdminExt — the management client: topic/group/offset/config/cluster
// administration over the remoting protocol.
//
// This port issues the admin RPCs through MQClient's RemotingClient directly —
// no separate admin API layer.
//
// Wire facts that were expensive to establish — do NOT "simplify" them:
//
//   - GET_BROKER_CONFIG(26)'s body is Properties-format TEXT (`k=v\n`) on the
//     broker side, not JSON. Parsing it as a KVTable fails on every real broker.
//   - UPDATE_AND_CREATE_SUBSCRIPTIONGROUP(200)'s body is a
//     SubscriptionGroupConfig JSON object.
//   - GET_TOPIC_CONFIG(351) sends `topic` + `lo`, and its body is a plain
//     TopicConfig JSON.
//   - GET_ALL_SUBSCRIPTIONGROUP_CONFIG(201) and GET_ALL_TOPIC_CONFIG(21) are
//     PAGED: seq / page size / dataVersion, accumulating until seq >= total-1.
//     The first page sends dataVersion as the EMPTY STRING. A broker that
//     omits totalNum returns everything in one round.
//   - fetchTopicsByCluster sends ext key `cluster` (NOT `clusterName`); with
//     the wrong key the nameserver NPEs internally, swallows it, and answers
//     SUCCESS with an empty list.
//   - The KV-config PUT/DELETE requests go to the NAMESERVER and must be
//     BROADCAST to every nameserver — one nameserver alone would not replicate.
//   - ResetOffsetBody.offsetTable is keyed by MessageQueue; the broker's fastjson
//     codec inlines the queue object as the map key, so decoding uses
//     decodeMessageQueueMap.
//   - UPDATE_AND_CREATE_TOPIC(17) MUST carry `topicFilterType`: the broker's
//     create-topic header validation rejects a null with
//     `topicFilterType = [null] value invalid`.
//   - The auth user/ACL management RPCs (3001..3010) are dialled at the address
//     the caller passed, VERBATIM: the auth service listens on the plain broker
//     port, so neither the VIP translation nor a route lookup belongs on that
//     path. Their header sets are exactly the field names listed per method
//     below — an extra key or an empty-string value is a different request.
import { RemotingCommand } from '../remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../remoting/codes.ts';
import { TopicRouteData, QueueData } from '../remoting/route.ts';
import {
  ClusterInfo, ConsumerConnection, ConsumeStats, ConsumeStatsList,
  GetConsumerListByGroupResponseBody, GroupList, KVTable, ProducerConnection,
  ResetOffsetBody, TopicConfig, TopicConfigSerializeWrapper, TopicList,
  TopicFilterType, DEFAULT_PERM,
  ConsumeMessageDirectlyResult, QueryConsumeTimeSpanBody,
} from '../remoting/bodies.ts';
import {
  TopicStatsTable, UserInfo, AclInfo, QueryConsumeQueueResponseBody, GroupForbidden,
  BrokerReplicasInfo, GetMetaDataResponseHeader, CheckRocksdbCqWriteResult,
  GetBrokerLiteInfoResponseBody, GetParentTopicInfoResponseBody, GetLiteTopicInfoResponseBody,
  GetLiteClientInfoResponseBody, GetLiteGroupInfoResponseBody,
  BrokerOperatorResult, AdminToolResult, AdminToolsResultCodeEnum,
} from '../remoting/admin_body.ts';
import { TopicQueueMappingDetail } from '../remoting/static_topic.ts';
import { RemotingSerializable } from '../remoting/serialize.ts';
import { ConsumeType } from '../remoting/heartbeat.ts';
import { MQClientManager } from './mq_client_manager.ts';
import { MixAll } from '../common/mixAll.ts';
import { MessageQueue, MessageExt } from '../common/message.ts';
import { MessageConst } from '../common/messageConst.ts';
import { BoundaryType } from '../common/boundary_type.ts';
import { decodeMessage, decodeMessageId, decodeMessages } from '../common/messageDecoder.ts';
import { QueryResult } from '../remoting/bodies.ts';
import { getLogger } from '../logging.ts';
import {
  MQClientException, MQBrokerException,
  RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
} from '../remoting/exception.ts';
import { isBlank } from '../common/utilAll.ts';
import { MQClient } from './mq_client.ts';
import { registerRpcHooks } from '../remoting/rpc_hooks.ts';

const logger = getLogger('admin');

// Default admin RPC timeout: 20000 ms.
export const DefaultAdminTimeoutMillis = 20000;
// Default admin consumer group: `admin_ext_group`.
export const DefaultAdminExtGroup = 'admin_ext_group';
// KV namespace holding ordered-topic config — the one cleaned on shutdown.
export const NAMESPACE_ORDER_TOPIC_CONFIG = 'ORDER_TOPIC_CONFIG';
// POP retry topic separator on the wire: `_`.
const POP_RETRY_SEPARATOR_V1 = '_';
// Header key of SET_COMMITLOG_READ_MODE(2004) — the request has no header
// class, this one field is written by hand.
const FILE_READAHEAD_MODE_KEY = 'READ_AHEAD_MODE';
// Fixed budget of the leader-routed controller RPCs (metadata / sync state /
// clean): the controller answers locally, a longer wait only hides a dead
// controller.
const ControllerInvokeTimeoutMillis = 3000;

// ---------------- Properties-text codec ----------------
// Parses/serialises the broker-side Properties TEXT format: blank lines and
// `#`/`!` comments are skipped, an UNESCAPED trailing `\` continues onto the
// next line, the key ends at the first `=`, `:` or whitespace, and the value's
// trailing whitespace is preserved. Broker config exports never contain
// `\t`/`\n`/`\uXXXX` escapes, so this codec deliberately does not expand them.
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

// Renders ext values the way the remoting header-to-wire encoding does:
// plain string for numbers, "true"/"false" for booleans. Keys written here
// MUST be the exact protocol field names — the broker reads them back by
// reflection (the `cluster`-not-`clusterName` trap lives exactly here).
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

// Header map for the auth RPCs. A field with no value is dropped rather than
// sent: `adminExt` above renders an unset value as "", and the auth-side header
// validation reads an empty string as a supplied (empty) value — a different
// request from an absent one.
function authExt(kv: Record<string, any>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(kv)) {
    if (v === null || v === undefined) continue;
    out[k] = extValue(v);
  }
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
  // Server-side namespaceV2 (Aliyun-style serverless instance id). When
  // non-empty, NamespaceRpcHook (registered FIRST on the remoting client, so
  // the ACL signature covers it) stamps every admin rpc with nsd=true /
  // ns=<value>.
  namespaceV2: string | null = null;
  // VIP channel flag — false by default. When true, broker requests go to the
  // VIP port (port - 2); NEVER applied to the nameserver.
  vipChannelEnabled = false;
  pollNameServerIntervalMillis = 30000;
  // Page size of the paged metadata reads (GET_ALL_TOPIC_CONFIG /
  // GET_ALL_SUBSCRIPTIONGROUP_CONFIG): maxTopicNum / maxGroupNum on the wire.
  // Default 2000.
  maxPageSizeInGetMetadata = 2000;
  rpcHook: any;
  timeoutMillis: number;
  // KV namespaces cleaned up when a topic is deleted: ORDER_TOPIC_CONFIG.
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
  setVipChannelEnabled(enable: boolean): void {
    this.vipChannelEnabled = enable;
    // The offset / consumer-list requests are issued on the shared instance, so
    // keep its VIP view in step with this facade's switch.
    if (this.client != null) this.client.setVipChannelEnabled(enable);
  }
  // Server-side namespace accessor, read live per request by NamespaceRpcHook.
  setNamespaceV2(namespaceV2: string | null): void { this.namespaceV2 = namespaceV2; }
  getNamespaceV2(): string | null { return this.namespaceV2; }
  setTimeoutMillis(timeoutMillis: number): void { this.timeoutMillis = timeoutMillis; }
  // Page size used as maxTopicNum / maxGroupNum by the paged metadata calls.
  // Applied to the shared instance when it is already up, and at start().
  setMaxPageSizeInGetMetadata(size: number): void {
    if (!Number.isInteger(size) || size <= 0) {
      throw new Error(`maxPageSizeInGetMetadata must be a positive integer, got ${size}`);
    }
    this.maxPageSizeInGetMetadata = size;
    if (this.client != null) this.client.setMaxPageSizeInGetMetadata(size);
  }
  getNameServerAddressList(): string[] {
    return this.client != null ? this.client.getNameServerAddressList() : [];
  }

  // Only the "ADMIN" default instance name becomes pid-unique
  // (instanceName = "ADMIN#<pid>"); a custom name is left alone.
  private _makeAdminInstancePidUnique(): void {
    if (this.instanceName === 'ADMIN') {
      this.instanceName = `ADMIN#${process.pid}`;
    }
  }

  start(): void {
    if (this._started) return;
    if (!this.namesrvAddr) {
      throw new MQClientException('name server address is not set, call setNamesrvAddr() first');
    }
    // Unconditionally make the instance name pid-unique, then build the
    // clientId as `<ip>@<instanceName>[@<unitName>][@STREAM]`.
    this._makeAdminInstancePidUnique();
    const clientId = MixAll.clientIdFor(this.instanceName, this.unitName, this.enableStreamRequestType);
    const presetRC = this.client != null ? this.client.remotingClient : null;
    const manager = MQClientManager.getInstance();
    const { instance: client, created } = manager.getOrCreateMQClientInstance(
      clientId, () => new MQClient(clientId, this.namesrvAddr, presetRC));
    this.client = client;
    if (created) {
      // Pass this facade's configured route-refresh period into the shared
      // instance ONCE at registration; the instance-level route timer reads
      // it from there instead of this facade field.
      client.setPollNameServerIntervalMillis(this.pollNameServerIntervalMillis);
      registerRpcHooks(this.client.remotingClient, {
        namespaceV2: () => this.namespaceV2,
        enableStreamRequestType: this.enableStreamRequestType,
        userHook: this.rpcHook,
      });
    }
    manager.registerClient(clientId, client);
    this.client.start();
    // Paging goes through the shared instance's page size; this facade's
    // configured value wins for its own metadata reads.
    this.client.setMaxPageSizeInGetMetadata(this.maxPageSizeInGetMetadata);
    // A facade created with the VIP switch already on (before start()) must have
    // it reflected on the instance it just obtained.
    this.client.setVipChannelEnabled(this.vipChannelEnabled);
    this._started = true;
    logger.info(`adminExt ${this.adminExtGroup} started, clientId=${clientId}`);
  }

  shutdown(): void {
    if (!this._started) return;
    this._started = false;
    if (this.client != null) {
      // Hand back to the manager: decrement the count; the real shutdown
      // (stop timers, close connections) happens only when it hits zero.
      MQClientManager.getInstance().unregisterClient(this.client.clientId, this.client);
    }
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
    return this._invokeChecked(this._vipChannel(addr), code, ext, body, timeoutMillis);
  }

  // Sends to an explicitly named name-server address and raises on non-SUCCESS.
  // The VIP port (listen port - 2) is a broker-only contract, so a name-server
  // address is used as-is and NEVER passed through the VIP translation.
  private async _invokeNameServerAddr(addr: string, code: number,
    ext: Record<string, string> | null, body: Buffer | null = null,
    timeoutMillis?: number): Promise<RemotingCommand> {
    return this._invokeChecked(addr, code, ext, body, timeoutMillis);
  }

  // Shared checked invoke: raise on any non-SUCCESS response. `target` is the
  // already-resolved address to dial.
  private async _invokeChecked(target: string, code: number,
    ext: Record<string, string> | null, body: Buffer | null = null,
    timeoutMillis?: number): Promise<RemotingCommand> {
    const client = this._requireClient();
    const response = await client.remotingClient.invokeSync(target, this._request(code, ext, body),
      this._timeout(timeoutMillis));
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQBrokerException(response.code, response.remark || `CODE: ${response.code}`, target);
    }
    return response;
  }

  // Broadcasts to EVERY nameserver for KV-config PUT/DELETE and raises if any
  // of them failed — a partial broadcast would leave one nameserver serving
  // stale config.
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

  // Sends to the FIRST nameserver that answers; the response is returned
  // unchecked so callers can branch.
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

  // Unchecked invoke at an explicitly named target with NO VIP translation —
  // the controller-leader RPCs and other requests whose target is not a broker
  // listener dial the address verbatim. Callers decide the error shape.
  private async _invokeRaw(target: string, code: number, ext: Record<string, string> | null,
    body: Buffer | null = null, timeoutMillis?: number): Promise<RemotingCommand> {
    const client = this._requireClient();
    return client.remotingClient.invokeSync(target, this._request(code, ext, body),
      this._timeout(timeoutMillis));
  }

  // Broker invoke with the VIP translation applied but WITHOUT the SUCCESS
  // check — a few admin RPCs surface a client exception rather than a broker
  // one, so the caller decides.
  private async _invokeBrokerRaw(addr: string, code: number, ext: Record<string, string> | null,
    body: Buffer | null = null, timeoutMillis?: number): Promise<RemotingCommand> {
    return this._invokeRaw(this._vipChannel(addr), code, ext, body, timeoutMillis);
  }

  // Client-side exception carrying the failing response code, for the admin
  // RPCs whose error mapping is a client exception rather than a broker one.
  private _clientError(code: number, remark: string | null): MQClientException {
    const e = new MQClientException(remark || `CODE: ${code}`);
    (e as any).responseCode = code;
    return e;
  }

  // ---------------- *Concurrent admin tooling ----------------

  // Error mapping of the concurrent wrappers: remoting failures report as
  // such, a missing topic route is its own code, and everything else lands on
  // the client / broker error codes.
  private _adminToolFailure(e: unknown): AdminToolResult<any> {
    if (e instanceof RemotingConnectException || e instanceof RemotingSendRequestException
      || e instanceof RemotingTimeoutException) {
      return AdminToolResult.failure(AdminToolsResultCodeEnum.REMOTING_ERROR, (e as Error).message);
    }
    if (e instanceof MQClientException) {
      const code = (e as any).responseCode;
      if (code === ResponseCode.TOPIC_NOT_EXIST) {
        return AdminToolResult.failure(AdminToolsResultCodeEnum.TOPIC_ROUTE_INFO_NOT_EXIST, e.message);
      }
      return AdminToolResult.failure(AdminToolsResultCodeEnum.MQ_CLIENT_ERROR, e.message);
    }
    if (e instanceof InterruptedException) {
      return AdminToolResult.failure(AdminToolsResultCodeEnum.INTERRUPT_ERROR, (e as Error).message);
    }
    return AdminToolResult.failure(AdminToolsResultCodeEnum.MQ_BROKER_ERROR,
      (e as Error)?.message ?? String(e));
  }

  // Runs one concurrent-wrapper body: the value it returns is wrapped as a
  // success result, a returned AdminToolResult passes through as-is (the leaf
  // decided the failure itself), and a throw maps onto the error codes.
  private async _adminToolExecute<T>(fn: () => Promise<T | AdminToolResult<any>>): Promise<AdminToolResult<T>> {
    try {
      const data = await fn();
      if (data instanceof AdminToolResult) return data as AdminToolResult<T>;
      return AdminToolResult.success(data as T);
    } catch (e) {
      logger.warn(`admin tool operation failed: ${(e as Error)?.message}`);
      return this._adminToolFailure(e);
    }
  }

  // Fetch a topic's route from the nameserver.
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
    // Cache into the client's route/broker-addr tables so subsequent
    // publish/subscribe paths see the fresh route.
    client.topicRouteData2TopicPublishInfo(topic, routeData);
    return routeData;
  }

  // ---------------- Topic management ----------------

  // createTopic pushes the topic to every MASTER in the
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
  // rejection is final (MQBrokerException from the retry loop is not retried).
  private async _createTopicInBroker(brokerAddr: string, defaultTopic: string, topic: string,
    readQueueNums: number, writeQueueNums: number, perm: number, topicSysFlag: number,
    topicFilterType: string, order: boolean, attributes: string): Promise<void> {
    const retryTimes = 5; // per-broker retry count for createTopic
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
          // Attribute-map serialization: an empty map renders as "", not null.
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

  // createAndUpdateTopicConfig sends UPDATE_AND_CREATE_TOPIC(17) to one addr.
  async createAndUpdateTopicConfig(addr: string, config: TopicConfig): Promise<void> {
    // TopicConfig's field name is `topic` (topicName is only the JSON key of
    // toDict) — reading the wrong field leaves topic=undefined in the header,
    // and the broker answers "specified topic is blank" (caught on the
    // real-machine static-topic leg).
    await this._createTopicInBroker(addr, this.createTopicKey, config.topic,
      config.readQueueNums, config.writeQueueNums, config.perm,
      config.topicSysFlag, config.topicFilterType || TopicFilterType.SINGLE_TAG,
      config.order === true, config.attributes != null ? JSON.stringify(config.attributes) : '');
  }

  // UPDATE_AND_CREATE_TOPIC_LIST(18): the list header has no fields of its
  // own, so the request carries no ext field keys; the topic configs ride the
  // body as {"topicConfigList":[…]}. No per-config validation happens on the
  // client side — unlike the single-config form, whatever the caller passes
  // goes out verbatim.
  async createAndUpdateTopicConfigList(brokerAddr: string, topicConfigList: TopicConfig[],
    timeoutMillis?: number): Promise<void> {
    const body = RemotingSerializable.encode({
      topicConfigList: topicConfigList.map((cfg) => cfg.toDict()),
    });
    const response = await this._invokeBrokerRaw(brokerAddr, RequestCode.UPDATE_AND_CREATE_TOPIC_LIST,
      {}, body, timeoutMillis);
    if (response.code !== ResponseCode.SUCCESS) throw this._clientError(response.code, response.remark);
  }

  async createStaticTopic(brokerAddr: string, defaultTopic: string, config: TopicConfig,
    mappingDetail: TopicQueueMappingDetail, force: boolean = false): Promise<void> {
    if (mappingDetail == null) {
      throw new MQClientException('createStaticTopic: mappingDetail must not be null');
    }
    await this._invokeBroker(brokerAddr, RequestCode.UPDATE_AND_CREATE_STATIC_TOPIC, adminExt({
      topic: config.topic,
      defaultTopic,
      readQueueNums: config.readQueueNums,
      writeQueueNums: config.writeQueueNums,
      perm: config.perm,
      topicFilterType: config.topicFilterType || TopicFilterType.SINGLE_TAG,
      topicSysFlag: config.topicSysFlag,
      order: config.order === true,
      force,
    }), mappingDetail.encode());
  }

  // DELETE_TOPIC_IN_BROKER(215) leaf for one address.
  private async _deleteTopicInBrokerAt(brokerAddr: string, topic: string): Promise<void> {
    await this._invokeBroker(brokerAddr, RequestCode.DELETE_TOPIC_IN_BROKER, adminExt({ topic }));
  }

  // DELETE_TOPIC_IN_BROKER(215) for each addr.
  async deleteTopicInBroker(addrs: string[], topic: string): Promise<void> {
    for (const addr of addrs) {
      await this._deleteTopicInBrokerAt(addr, topic);
    }
  }

  // Fan-out flavor of deleteTopicInBroker over the caller's address set: every
  // address is asked at once, each outcome is collected into the success /
  // failure lists, and the sweep itself never throws.
  async deleteTopicInBrokerConcurrent(addrs: string[] | Set<string>,
    topic: string): Promise<AdminToolResult<BrokerOperatorResult>> {
    const targetAddrs = Array.isArray(addrs) ? addrs : Array.from(addrs);
    return this._adminToolExecute(async () => {
      const result = new BrokerOperatorResult();
      const settled = await Promise.allSettled(targetAddrs.map(async (addr) => {
        await this._deleteTopicInBrokerAt(addr, topic);
        return addr;
      }));
      for (let i = 0; i < settled.length; i++) {
        const s = settled[i];
        if (s.status === 'fulfilled') {
          result.successList.push(targetAddrs[i]);
        } else {
          logger.warn(`deleteTopicInBroker error. topic=${topic}, broker=${targetAddrs[i]}: `
            + `${(s.reason as Error)?.message}`);
          result.failureList.push(targetAddrs[i]);
        }
      }
      return result;
    });
  }

  // DELETE_TOPIC_IN_NAMESRV(216) for each addr; addrs == null → all nameservers.
  async deleteTopicInNameServer(addrs: string[] | null, topic: string): Promise<void> {
    const client = this._requireClient();
    const targets = addrs != null ? addrs : client.getNameServerAddressList();
    for (const nsAddr of targets) {
      await this._invokeNameServerAddr(nsAddr, RequestCode.DELETE_TOPIC_IN_NAMESRV, adminExt({ topic }), null, 5000);
    }
  }

  // deleteTopic removes from ALL brokers (master AND slave) of the cluster,
  // then the nameserver route, then the KV namespaces (ORDER_TOPIC_CONFIG).
  async deleteTopic(topic: string, clusterName: string): Promise<void> {
    const client = this._requireClient();
    const clusterInfo = await this.examineBrokerClusterInfo();
    const names = clusterInfo.clusterAddrTable[clusterName];
    if (names == null || names.length === 0) {
      throw new MQClientException(`The cluster [${clusterName}] not exist`);
    }
    // Collect master AND slave addrs of the cluster.
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

  // Fetch every topic from the nameserver: GET_ALL_TOPIC_LIST_FROM_NAMESERVER.
  async fetchAllTopicList(timeoutMillis?: number): Promise<TopicList> {
    const response = await this._invokeNameServerOne(RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, null,
      timeoutMillis);
    if (response.body == null || (response.body as Buffer).length === 0) {
      const t = new TopicList();
      return t;
    }
    return TopicList.decode(response.body as Buffer);
  }

  // GET_TOPICS_BY_CLUSTER(224). The ext key MUST be `cluster`; with
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

  // examineTopicStats(topic) merges GET_TOPIC_STATS_INFO(202) across the
  // topic's brokers. The synchronous path does NOT sum topicPutTps (only the
  // *Concurrent variant does).
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

  // Fan-out flavor of examineTopicStats: every master of the route is asked at
  // once, offset tables are merged and topicPutTps is SUMMED. A broker that
  // fails is logged and skipped — the sweep never fails because one broker
  // did. A topic without a route is reported as a failure result, not a throw.
  async examineTopicStatsConcurrent(topic: string): Promise<AdminToolResult<TopicStatsTable>> {
    return this._adminToolExecute(async () => {
      const merged = new TopicStatsTable();
      const route = await this.examineTopicRouteInfo(topic);
      if (route == null || route.brokerDatas.length === 0) return merged;
      const settled = await Promise.allSettled(route.brokerDatas.map(async (bd) => {
        const addr = bd.selectBrokerAddr();
        if (addr == null) return null;
        return this.examineTopicStatsByBroker(addr, topic);
      }));
      for (const s of settled) {
        if (s.status === 'rejected') {
          logger.warn(`getTopicStatsInfo error. topic=${topic}: ${(s.reason as Error)?.message}`);
          continue;
        }
        if (s.value == null) continue;
        for (const [q, off] of s.value.offsetTable) merged.offsetTable.set(q, off);
        merged.topicPutTps += s.value.topicPutTps;
      }
      return merged;
    });
  }

  // QUERY_CONSUME_TIME_SPAN(303) leaf for one address.
  private async _queryConsumeTimeSpanAt(brokerAddr: string, topic: string,
    group: string): Promise<Record<string, any>[]> {
    const response = await this._invokeBroker(brokerAddr, RequestCode.QUERY_CONSUME_TIME_SPAN,
      adminExt({ topic, group }), null);
    const body = response.body as Buffer;
    if (body == null || body.length === 0) return [];
    return QueryConsumeTimeSpanBody.decode(body).consumeTimeSpanSet;
  }

  // QUERY_CONSUME_TIME_SPAN(303): walk the route's masters and concatenate each
  // broker's consumeTimeSpanSet. A broker that answers with an empty body
  // contributes nothing; a broker that ERRORS propagates -- unlike the topic-stats
  // sweep above, which is best-effort, this one never swallows a per-broker error.
  async queryConsumeTimeSpan(topic: string, group: string,
    timeoutMillis?: number): Promise<Record<string, any>[]> {
    const route = await this.examineTopicRouteInfo(topic);
    const spans: Record<string, any>[] = [];
    for (const bd of route.brokerDatas) {
      const addr = bd.selectBrokerAddr();
      if (addr == null) continue;
      const response = await this._invokeBroker(addr, RequestCode.QUERY_CONSUME_TIME_SPAN,
        adminExt({ topic, group }), null, timeoutMillis);
      const body = response.body as Buffer;
      if (body == null || body.length === 0) continue;
      spans.push(...QueryConsumeTimeSpanBody.decode(body).consumeTimeSpanSet);
    }
    return spans;
  }

  // Fan-out flavor of queryConsumeTimeSpan: the masters are asked at once and
  // the consumeTimeSpanSet entries are concatenated. Task failures are logged;
  // a missing route answers with an EMPTY span list as a success result (this
  // one sweep, unlike its topic-list sibling, does not treat a missing route
  // as a failure).
  async queryConsumeTimeSpanConcurrent(topic: string,
    group: string): Promise<AdminToolResult<Record<string, any>[]>> {
    return this._adminToolExecute(async () => {
      const spans: Record<string, any>[] = [];
      const route = await this.examineTopicRouteInfo(topic);
      if (route == null || route.brokerDatas.length === 0) return spans;
      const settled = await Promise.allSettled(route.brokerDatas.map(async (bd) => {
        const addr = bd.selectBrokerAddr();
        if (addr == null) return null;
        return this._queryConsumeTimeSpanAt(addr, topic, group);
      }));
      for (const s of settled) {
        if (s.status === 'rejected') {
          logger.warn(`queryConsumeTimeSpan error. topic=${topic}, group=${group}: `
            + `${(s.reason as Error)?.message}`);
          continue;
        }
        if (s.value != null) spans.push(...s.value);
      }
      return spans;
    });
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

  // GET_BROKER_CONFIG(26). The body is Properties-format TEXT on the broker
  // side, NOT JSON — a client that parses it as a KVTable fails on every real
  // broker.
  //
  // This read is the one broker RPC that always uses the listening port: the fast channel
  // serves the other admin RPCs, not this one, so the address goes out untranslated even
  // when the VIP switch is on.
  async getBrokerConfig(brokerAddr: string, timeoutMillis?: number): Promise<Record<string, string>> {
    const response = await this._invokeChecked(brokerAddr, RequestCode.GET_BROKER_CONFIG, null,
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
    const response = await this._invokeNameServerAddr(namesrvAddr, RequestCode.WIPE_WRITE_PERM_OF_BROKER,
      adminExt({ brokerName }));
    const v = response.extFields ? response.extFields['wipeTopicCount'] : null;
    return v != null ? parseInt(v, 10) : 0;
  }

  // ADD_WRITE_PERM_OF_BROKER(327) — returns the number of topics affected.
  async addWritePermOfBroker(namesrvAddr: string, brokerName: string): Promise<number> {
    const response = await this._invokeNameServerAddr(namesrvAddr, RequestCode.ADD_WRITE_PERM_OF_BROKER,
      adminExt({ brokerName }));
    const v = response.extFields ? response.extFields['addTopicCount'] : null;
    return v != null ? parseInt(v, 10) : 0;
  }

  // CLEAN_UNUSED_TOPIC(316) on one broker.
  async cleanUnusedTopicByAddr(addr: string): Promise<boolean> {
    await this._invokeBroker(addr, RequestCode.CLEAN_UNUSED_TOPIC, null);
    return true;
  }

  // CLEAN_UNUSED_TOPIC(316) across every broker of the cluster; the last
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

  // getClusterList(topic): the clusters whose brokers appear in the topic's
  // route.
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

  // getTopicClusterList(topic): uses the FIRST BrokerData's brokerName only —
  // the nameserver's cluster-per-topic convention keeps that shape.
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

  // GET_ALL_TOPIC_CONFIG(21) on one broker, PAGED: topicSeq / maxTopicNum /
  // dataVersion, accumulating until topicSeq >= totalTopicNum-1. A broker that
  // does not send `totalTopicNum` (older versions) returns everything in one
  // round. The FIRST page sends dataVersion as the EMPTY STRING (the key is
  // present, not omitted); later pages send the JSON of the last seen version.
  async getAllTopicConfig(brokerAddr: string, timeoutMillis?: number): Promise<TopicConfigSerializeWrapper> {
    const client = this._requireClient();
    const timeout = this._timeout(timeoutMillis);
    // The VIP port is a broker-only contract; every page of this one request is
    // dialled at the resolved target, never at the cached route address.
    const target = this._vipChannel(brokerAddr);
    let currentDataVersion: Record<string, any> | null = null;
    let topicSeq = 0;
    const table: Record<string, TopicConfig> = {};
    const begin = Date.now();

    for (;;) {
      const left = timeout - (Date.now() - begin);
      if (left < 0) throw new MQClientException('invokeSync call timeout');
      const ext: Record<string, string> = adminExt({
        topicSeq,
        maxTopicNum: client.maxPageSizeInGetMetadata,
        dataVersion: currentDataVersion != null ? JSON.stringify(currentDataVersion) : '',
      });
      const response = await client.remotingClient.invokeSync(target,
        this._request(RequestCode.GET_ALL_TOPIC_CONFIG, ext), left);
      if (response.code !== ResponseCode.SUCCESS) {
        throw new MQBrokerException(response.code, response.remark || '', target);
      }
      const wrapper = TopicConfigSerializeWrapper.decode(response.body as Buffer);
      const page = wrapper.topicConfigTable || {};
      for (const [k, v] of Object.entries(page)) table[k] = v;
      const newVersion = (wrapper.dataVersion || null) as Record<string, any> | null;
      if (currentDataVersion == null) {
        // Fill the version before the break so an old broker (no totalTopicNum)
        // still returns the version it sent.
        currentDataVersion = newVersion;
      }

      const totalText = response.extFields ? response.extFields['totalTopicNum'] : null;
      if (totalText == null) break; // older broker: one round returns everything
      const total = parseInt(totalText, 10);
      if (Number.isNaN(total)) break;
      if (!sameDataVersion(currentDataVersion, newVersion)) {
        logger.warn('topic config dataVersion changed, restart paging');
        currentDataVersion = newVersion;
        topicSeq = 0;
        for (const k of Object.keys(table)) delete table[k];
        continue;
      }
      topicSeq += Object.keys(page).length;
      if (topicSeq >= total - 1) break;
    }

    const out = new TopicConfigSerializeWrapper();
    out.topicConfigTable = table;
    out.dataVersion = currentDataVersion != null ? currentDataVersion : {};
    return out;
  }

  // GET_SYSTEM_TOPIC_LIST_FROM_BROKER(305).
  async getSystemTopicListFromBroker(brokerAddr: string, timeoutMillis?: number): Promise<TopicList> {
    const response = await this._invokeBroker(brokerAddr, RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER,
      null, null, timeoutMillis);
    return TopicList.decode(response.body as Buffer);
  }

  // getUserTopicConfig drops system topics and (unless specialTopic) the
  // %RETRY% / %DLQ% topics, and topics with an invalid perm (perm >= 0 &&
  // perm < PERM_PRIORITY=8).
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

  // PUT_KV_CONFIG(100), BROADCAST to every nameserver so none serves stale
  // config.
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

  // UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST(225): header null, the group
  // configs ride the body under the groupConfigList key.
  async createAndUpdateSubscriptionGroupConfigList(brokerAddr: string, configs: Record<string, any>[],
    timeoutMillis?: number): Promise<void> {
    const body = RemotingSerializable.encode({ groupConfigList: configs });
    const response = await this._invokeBrokerRaw(brokerAddr,
      RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST, null, body, timeoutMillis);
    if (response.code !== ResponseCode.SUCCESS) throw this._clientError(response.code, response.remark);
  }

  // GET_ALL_SUBSCRIPTIONGROUP_CONFIG(201), PAGED: accumulate until
  // groupSeq >= totalGroupNum-1. A broker that does not send `totalGroupNum`
  // (older versions) returns everything in one round.
  async getAllSubscriptionGroup(brokerAddr: string, timeoutMillis?: number): Promise<Record<string, any>> {
    const client = this._requireClient();
    const timeout = this._timeout(timeoutMillis);
    // The VIP port is a broker-only contract; every page of this one request is
    // dialled at the resolved target, never at the cached route address.
    const target = this._vipChannel(brokerAddr);
    let currentDataVersion: Record<string, any> | null = null;
    let groupSeq = 0;
    const table: Record<string, any> = {};
    const forbidden: Record<string, any> = {};
    const begin = Date.now();

    for (;;) {
      const left = timeout - (Date.now() - begin);
      if (left < 0) throw new MQClientException('invokeSync call timeout');
      // dataVersion MUST be present on the first page as the EMPTY STRING
      // (not omitted), and carries the JSON of the last seen version afterwards.
      const ext: Record<string, string> = adminExt({
        groupSeq,
        maxGroupNum: client.maxPageSizeInGetMetadata,
        dataVersion: currentDataVersion != null ? JSON.stringify(currentDataVersion) : '',
      });
      const response = await client.remotingClient.invokeSync(target,
        this._request(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, ext), left);
      if (response.code !== ResponseCode.SUCCESS) {
        throw new MQBrokerException(response.code, response.remark || '', target);
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

  // Drops system (CID_RMQ_SYS_) and predefined groups.
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

  // Read one group out of the full subscription-group wrapper (NOT
  // GET_SUBSCRIPTIONGROUP_CONFIG(352) — that call is broker-version dependent).
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
  // the %RETRY%<group> route. Empty connectionSet → CONSUMER_NOT_ONLINE.
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
  // and asks the first broker with a master addr.
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

  // GET_CONSUMER_LIST_BY_GROUP(38) — brokerAddr is required (pick one via the
  // %RETRY%<group> route).
  async getConsumerListByGroup(consumerGroup: string, brokerAddr: string): Promise<string[]> {
    const response = await this._invokeBroker(brokerAddr, RequestCode.GET_CONSUMER_LIST_BY_GROUP,
      adminExt({ consumerGroup }));
    const body = GetConsumerListByGroupResponseBody.decode(response.body as Buffer);
    return body.consumerIdList;
  }

  // ---------------- Consume stats ----------------

  // GET_CONSUME_STATS(208) against ONE broker. The composite path multiplies
  // the timeout by 3 so a fan-out stays within one round-trip budget.
  async examineConsumeStatsByBroker(brokerAddr: string, consumerGroup: string,
    topic: string | null = null, timeoutMillis?: number): Promise<ConsumeStats> {
    const ext: Record<string, string> = { consumerGroup };
    if (topic) ext['topic'] = topic;
    const response = await this._invokeBroker(brokerAddr, RequestCode.GET_CONSUME_STATS, ext,
      null, timeoutMillis != null ? timeoutMillis * 3 : undefined);
    return ConsumeStats.decode(response.body as Buffer);
  }

  // GET_CONSUME_STATS(208) leaf at the PLAIN timeout: the concurrent sweep
  // bounds the whole fan-out with one budget, so the per-broker call must not
  // multiply it.
  private async _getConsumeStatsAt(brokerAddr: string, consumerGroup: string, topic: string | null,
    timeoutMillis?: number): Promise<ConsumeStats> {
    const ext: Record<string, string> = { consumerGroup };
    if (topic) ext['topic'] = topic;
    const response = await this._invokeBroker(brokerAddr, RequestCode.GET_CONSUME_STATS, ext,
      null, timeoutMillis);
    return ConsumeStats.decode(response.body as Buffer);
  }

  // examineConsumeStats(group[, topic]): route candidates are the
  // %RETRY%<group> topic, then the topic itself, then the POP retry topic —
  // the first route that resolves wins. Fan out over all brokers, merge the
  // offset tables and sum the tps. An empty table raises CONSUMER_NOT_ONLINE
  // (or BROADCAST_CONSUMPTION when the group consumes in broadcast mode).
  async examineConsumeStats(consumerGroup: string, topic: string | null = null): Promise<ConsumeStats> {
    const routeTopics: string[] = [MixAll.getRetryTopic(consumerGroup)];
    if (topic != null) {
      routeTopics.push(topic);
      // POP retry topic name: %RETRY%<group>_<topic>
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

  // Fan-out flavor of examineConsumeStats: the route is discovered over the
  // retry topic, then the topic, then the POP retry topic (the first that
  // resolves wins, a failed probe is skipped), every master is asked at once,
  // offset tables are merged and consumeTps is SUMMED. Task failures are
  // logged, never propagated. An empty merged table is reported as
  // CONSUMER_NOT_ONLINE / BROADCAST_CONSUMPTION after the group's connections
  // have been probed, and a missing route as TOPIC_ROUTE_INFO_NOT_EXIST.
  async examineConsumeStatsConcurrent(consumerGroup: string,
    topic: string | null): Promise<AdminToolResult<ConsumeStats>> {
    return this._adminToolExecute(async () => {
      const routeTopics: string[] = [MixAll.getRetryTopic(consumerGroup)];
      if (topic != null) {
        routeTopics.push(topic);
        routeTopics.push(`${MixAll.RETRY_GROUP_TOPIC_PREFIX}${consumerGroup}${POP_RETRY_SEPARATOR_V1}${topic}`);
      }
      let route: TopicRouteData | null = null;
      for (const routeTopic of routeTopics) {
        try {
          route = await this.examineTopicRouteInfo(routeTopic);
          break;
        } catch (e) {
          continue; // the sweep reports a missing route, it does not throw
        }
      }
      if (route == null || route.brokerDatas.length === 0) {
        return AdminToolResult.failure(AdminToolsResultCodeEnum.TOPIC_ROUTE_INFO_NOT_EXIST,
          'topic router info not found');
      }
      const timeout = this._timeout();
      const result = new ConsumeStats();
      const settled = await Promise.allSettled(route.brokerDatas.map(async (bd) => {
        const addr = bd.selectBrokerAddr();
        if (addr == null) return null;
        return this._getConsumeStatsAt(addr, consumerGroup, topic, timeout);
      }));
      for (const s of settled) {
        if (s.status === 'rejected') {
          logger.warn(`getConsumeStats error. topic=${topic}, consumerGroup=${consumerGroup}: `
            + `${(s.reason as Error)?.message}`);
          continue;
        }
        if (s.value == null) continue;
        for (const [q, w] of s.value.offsetTable) result.offsetTable.set(q, w);
        result.consumeTps += s.value.consumeTps;
      }
      if (result.offsetTable.size === 0) {
        let connection: ConsumerConnection;
        try {
          connection = await this.examineConsumerConnectionInfo(consumerGroup);
        } catch (e) {
          return AdminToolResult.failure(AdminToolsResultCodeEnum.CONSUMER_NOT_ONLINE, 'Not found the '
            + 'consumer group consume stats, because return offset table is empty, maybe the consumer '
            + 'not consume any message');
        }
        if ((connection as any).messageModel === 'BROADCASTING') {
          return AdminToolResult.failure(AdminToolsResultCodeEnum.BROADCAST_CONSUMPTION, 'Not found the '
            + 'consumer group consume stats, because return offset table is empty, the consumer is '
            + 'under the broadcast mode');
        }
      }
      return result;
    });
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

  // QUERY_TOPICS_BY_CONSUMER(343) leaf for one address.
  private async _queryTopicsByConsumerAt(brokerAddr: string, group: string): Promise<TopicList> {
    const response = await this._invokeBroker(brokerAddr, RequestCode.QUERY_TOPICS_BY_CONSUMER,
      adminExt({ group }));
    return TopicList.decode(response.body as Buffer);
  }

  // QUERY_TOPICS_BY_CONSUMER(343) — fan out over the %RETRY%<group> route and
  // merge; the response's topicList is a Set, hence de-duplication.
  async queryTopicsByConsumer(group: string): Promise<TopicList> {
    const retryTopic = MixAll.getRetryTopic(group);
    const route = await this.examineTopicRouteInfo(retryTopic);
    const result = new TopicList();
    const seen = new Set<string>();
    for (const bd of route.brokerDatas) {
      const addr = bd.selectBrokerAddr();
      if (addr == null) continue;
      const part = await this._queryTopicsByConsumerAt(addr, group);
      for (const t of part.topicList) {
        if (!seen.has(t)) { seen.add(t); result.topicList.push(t); }
      }
    }
    return result;
  }

  // Fan-out flavor of queryTopicsByConsumer: route on the group's retry topic
  // and union only the topicList entries across the brokers. Task failures are
  // logged; a missing retry route is reported as TOPIC_ROUTE_INFO_NOT_EXIST.
  async queryTopicsByConsumerConcurrent(group: string): Promise<AdminToolResult<TopicList>> {
    return this._adminToolExecute(async () => {
      const retryTopic = MixAll.getRetryTopic(group);
      const route = await this.examineTopicRouteInfo(retryTopic);
      if (route == null || route.brokerDatas.length === 0) {
        return AdminToolResult.failure(AdminToolsResultCodeEnum.TOPIC_ROUTE_INFO_NOT_EXIST,
          'router info not found.');
      }
      const result = new TopicList();
      const seen = new Set<string>();
      const settled = await Promise.allSettled(route.brokerDatas.map(async (bd) => {
        const addr = bd.selectBrokerAddr();
        if (addr == null) return null;
        return this._queryTopicsByConsumerAt(addr, group);
      }));
      for (const s of settled) {
        if (s.status === 'rejected') {
          logger.warn(`queryTopicsByConsumer error. group=${group}: ${(s.reason as Error)?.message}`);
          continue;
        }
        if (s.value == null) continue;
        for (const t of s.value.topicList) {
          if (!seen.has(t)) { seen.add(t); result.topicList.push(t); }
        }
      }
      return result;
    });
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

  // INVOKE_BROKER_TO_RESET_OFFSET(222) → ResetOffsetBody (a MessageQueue-keyed
  // map whose keys the broker's fastjson codec inlines). `isC` selects the
  // C-compatible response shape via the `isC` ext field.
  // When `queueId` is given the reset is scoped to that ONE queue and the
  // broker reads the pair (queueId, offset) literally; without them the same
  // request code resets EVERY queue of the group at `timestamp`.
  async invokeBrokerToResetOffset(brokerAddr: string, topic: string, group: string,
    timestamp: number, isForce: boolean, isC = false,
    queueId: number | null = null, resetOffset: number | null = null): Promise<Map<MessageQueue, number> | null> {
    const ext = adminExt({ topic, consumerGroup: group, timestamp, isForce });
    if (queueId != null) {
      ext['queueId'] = extValue(queueId);
      ext['offset'] = extValue(resetOffset != null ? resetOffset : -1);
    }
    let code: number = RequestCode.INVOKE_BROKER_TO_RESET_OFFSET;
    if (isC) {
      // Same request code for both shapes; the `isC` flag tells the broker to
      // serialise the body in the C-compatible form.
      code = RequestCode.INVOKE_BROKER_TO_RESET_OFFSET;
      void isC;
    }
    const response = await this._invokeBroker(brokerAddr, code, ext);
    if (response.body == null || (response.body as Buffer).length === 0) return null;
    const body = ResetOffsetBody.decode(response.body as Buffer);
    return body.offsetTable;
  }

  // resetOffsetByTimestamp fans out over the topic route and merges.
  async resetOffsetByTimestamp(topic: string, group: string, timestamp: number,
    isForce = true, clusterName: string | null = null): Promise<Map<MessageQueue, number>> {
    void clusterName; // clusterName is only consulted for LMQ topics; unused here
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

  // resetOffsetNew prefers the broker-side reset; when the consumer is
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

  // Concurrent resetOffsetNew: the broker-side reset fans over every broker of
  // the route; a broker whose consumer is offline falls back to the old
  // per-queue walk on THAT broker alone, and the whole run only succeeds when
  // every broker did.
  async resetOffsetNewConcurrent(group: string, topic: string, timestamp: number): Promise<AdminToolResult<BrokerOperatorResult>> {
    return this._adminToolExecute(async () => {
      const route = await this.examineTopicRouteInfo(topic);
      if (route == null || route.brokerDatas == null || route.brokerDatas.length === 0) {
        return AdminToolResult.failure(AdminToolsResultCodeEnum.TOPIC_ROUTE_INFO_NOT_EXIST, 'topic router info not found');
      }
      const queueDataByBroker: Record<string, QueueData> = {};
      for (const qd of route.queueDatas) queueDataByBroker[qd.brokerName] = qd;

      const tasks = route.brokerDatas.map(async (bd) => {
        const addr = bd.selectBrokerAddr();
        if (addr == null) return { addr: null, ok: false };
        try {
          const offsetTable = await this.invokeBrokerToResetOffset(addr, topic, group, timestamp, true);
          return { addr, ok: offsetTable != null };
        } catch (e) {
          // The reset leaf raises with a responseCode whether it wraps the
          // answer in the broker or the client exception — match on the code.
          if ((e as any).responseCode === ResponseCode.CONSUMER_NOT_ONLINE) {
            try {
              const qd = queueDataByBroker[bd.brokerName];
              if (qd == null) {
                throw new MQClientException(`No route data of broker ${bd.brokerName} for topic ${topic}`);
              }
              await this._resetOffsetOldForBroker(addr, bd.brokerName, qd, group, topic, timestamp, true);
              return { addr, ok: true };
            } catch (e2) {
              logger.warn(`resetOffsetByTimestampOld error. addr=${addr}, topic=${topic}, group=${group}, `
                + `timestamp=${timestamp}: ${(e2 as Error)?.message}`);
              return { addr, ok: false };
            }
          }
          if ((e as any).responseCode === ResponseCode.SYSTEM_ERROR) {
            // The group never came online on this broker — nothing to reset.
            return { addr, ok: true };
          }
          logger.warn(`resetOffsetNewConcurrent error. addr=${addr}, topic=${topic}, group=${group}, `
            + `timestamp=${timestamp}: ${(e as Error)?.message}`);
          return { addr, ok: false };
        }
      });
      const outcomes = await Promise.allSettled(tasks);
      const result = new BrokerOperatorResult();
      for (const s of outcomes) {
        if (s.status === 'fulfilled') {
          if (s.value.addr != null) {
            if (s.value.ok) result.successList.push(s.value.addr);
            else result.failureList.push(s.value.addr);
          }
        } else {
          logger.warn(`resetOffsetNewConcurrent task rejected: ${(s.reason as Error)?.message}`);
        }
      }
      if (result.successList.length === route.brokerDatas.length) {
        return AdminToolResult.success(result);
      }
      return AdminToolResult.failure(AdminToolsResultCodeEnum.MQ_BROKER_ERROR, 'operator failure', result);
    });
  }

  // resetOffsetByTimestampOld: per queue — when the group consumed the
  // topic, reset each queue it touched; otherwise reset EVERY read queue.
  async resetOffsetByTimestampOld(consumerGroup: string, topic: string, timestamp: number,
    force: boolean): Promise<RollbackStats[]> {
    const route = await this.examineTopicRouteInfo(topic);
    const queueDataByBroker: Record<string, QueueData> = {};
    for (const qd of route.queueDatas) queueDataByBroker[qd.brokerName] = qd;

    const rollbackStatsList: RollbackStats[] = [];
    for (const bd of route.brokerDatas) {
      const addr = bd.selectBrokerAddr();
      if (addr == null) continue;
      rollbackStatsList.push(...await this._resetOffsetOldForBroker(addr, bd.brokerName,
        queueDataByBroker[bd.brokerName] != null ? queueDataByBroker[bd.brokerName] : null,
        consumerGroup, topic, timestamp, force));
    }
    return rollbackStatsList;
  }

  // One broker's leg of the old per-queue reset walk: the queues the group
  // consumed are reset from their consume offsets, an untouched group falls
  // back to every read queue of the broker's route entry.
  private async _resetOffsetOldForBroker(brokerAddr: string, brokerName: string,
    queueData: QueueData | null, consumerGroup: string, topic: string, timestamp: number,
    force: boolean): Promise<RollbackStats[]> {
    const rollbackStatsList: RollbackStats[] = [];
    const consumeStats = await this.examineConsumeStatsByBroker(brokerAddr, consumerGroup);
    let hasConsumed = false;
    for (const [queue, offsetWrapper] of consumeStats.offsetTable) {
      if (queue.getTopic() === topic) {
        hasConsumed = true;
        rollbackStatsList.push(
          await this._resetOffsetConsumeOffset(brokerAddr, consumerGroup, queue, offsetWrapper, timestamp, force));
      }
    }
    if (!hasConsumed) {
      const topicStatus = await this.examineTopicStatsByBroker(brokerAddr, topic);
      const readQueueNums = queueData != null ? queueData.readQueueNums : 0;
      for (let i = 0; i < readQueueNums; i++) {
        const queue = new MessageQueue(topic, brokerName, i);
        const to = topicStatus.offsetTable.get(queue);
        const offsetWrapper = {
          brokerOffset: to != null ? to.maxOffset : 0,
          consumerOffset: to != null ? to.minOffset : 0,
        };
        rollbackStatsList.push(
          await this._resetOffsetConsumeOffset(brokerAddr, consumerGroup, queue, offsetWrapper as any, timestamp, force));
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

  // resetOffsetByQueueId: direct UPDATE_CONSUMER_OFFSET, then a
  // broker-side reset of that single queue.
  async resetOffsetByQueueId(brokerAddr: string, consumeGroup: string, topicName: string,
    queueId: number, resetOffset: number): Promise<void> {
    const client = this._requireClient();
    await client.updateConsumerOffset(brokerAddr, consumeGroup,
      new MessageQueue(topicName, '', queueId), resetOffset);
    await this.invokeBrokerToResetOffset(brokerAddr, topicName, consumeGroup, 0, false, false,
      queueId, resetOffset);
  }

  // ---------------- Offset queries (admin-side offset surface) ----------------

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

  // updateConsumeOffset writes the committed offset for one queue at one
  // broker via UPDATE_CONSUMER_OFFSET(15).
  async updateConsumeOffset(brokerAddr: string, consumeGroup: string, mq: MessageQueue,
    offset: number): Promise<void> {
    await this._requireClient().updateConsumerOffset(brokerAddr, consumeGroup, mq, offset);
  }

  // examineConsumerOffset issues QUERY_CONSUMER_OFFSET(14). Returns the
  // committed offset; `hasOffset` is false when the broker answers
  // QUERY_NOT_FOUND (no offset recorded yet).
  async examineConsumerOffset(consumerGroup: string, mq: MessageQueue): Promise<{ offset: number; hasOffset: boolean }> {
    const brokerAddr = this._masterAddrForMq(mq);
    const r = await this._requireClient().queryConsumerOffset(brokerAddr, consumerGroup, mq);
    return { offset: r.offset, hasOffset: r.found };
  }

  // searchBoundaryOffset issues SEARCH_OFFSET_BY_TIMESTAMP(29) with the
  // `boundaryType` ext field. The lower/upper wrappers pick
  // BoundaryType.LOWER / UPPER.
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
    const target = this._vipChannel(brokerAddr);
    const response = await client.remotingClient.invokeSync(target, request, this._timeout());
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQBrokerException(response.code, response.remark || `CODE: ${response.code}`, target);
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

  // queryMessage issues QUERY_MESSAGE(12) against one broker's index.
  //
  // ⚠ WIRE: the response body is NOT JSON — the broker streams the matched
  // messages as CONCATENATED 17-segment stored records. Parsing it as
  // RemotingSerializable JSON was the bug behind "query by key always fails".
  // indexType: K (default) / U (uniq key); 'U' additionally sets
  // MixAll.UNIQUE_MSG_QUERY_FLAG so the broker takes the unique-key path.
  async queryMessage(brokerAddr: string, topic: string, key: string,
    maxNum: number = 32, beginTimestamp = 0, endTimestamp = 0,
    indexType: string = 'K'): Promise<QueryResult> {
    // The time window is unbounded when begin/end are unset: the broker
    // matches index FILES against [begin, end], so end=0 matches NOTHING
    // (every real index file has a start timestamp > 0) and every query would
    // answer QUERY_NOT_FOUND. Normalise: begin<0/0 → unbounded past,
    // end<=0 → now.
    const begin = beginTimestamp > 0 ? beginTimestamp : 0;
    // Long.MAX_VALUE as a STRING — as a JS Number it rounds up to 2^63 and
    // the broker's Long.parseLong rejects it, leaving the header field null
    // (NPE inside QueryMessageProcessor).
    const end = endTimestamp > 0 ? endTimestamp : '9223372036854775807';
    const ext: Record<string, string> = {
      topic,
      key,
      maxNum: String(maxNum),
      beginTimestamp: String(begin),
      endTimestamp: String(end),
      indexType,
    };
    if (indexType === MessageConst.INDEX_UNIQUE_TYPE) {
      // MixAll.UNIQUE_MSG_QUERY_FLAG — forces the U-index path and the
      // broker's defaultQueryMaxNum on the server side.
      ext['_UNIQUE_KEY_QUERY'] = 'true';
    }
    const response = await this._invokeBroker(brokerAddr, RequestCode.QUERY_MESSAGE, ext);
    const result = new QueryResult();
    // The response's QueryMessageResponseHeader fields ride the extFields.
    const extResp = response.extFields || {};
    if (extResp['indexLastUpdateTimestamp'] != null) {
      result.indexLastUpdateTimestamp = parseInt(extResp['indexLastUpdateTimestamp'], 10);
    }
    if (response.body != null && response.body.length > 0) {
      result.messageList = decodeMessages(response.body, true);
    }
    return result;
  }

  async queryMessageByKey(brokerAddr: string, topic: string, key: string, maxNum = 32): Promise<QueryResult> {
    return this.queryMessage(brokerAddr, topic, key, maxNum);
  }

  // queryMessageByUniqKey: the U-index query. The stored records
  // are already in the response body, so no per-hit viewMessage round trip is
  // needed; only the first hit is returned for this shape.
  async queryMessageByUniqKey(brokerAddr: string, topic: string, uniqKey: string,
    maxNum = 32): Promise<MessageExt | null> {
    const result = await this.queryMessage(brokerAddr, topic, uniqKey, maxNum, 0, 0, 'U');
    return result.messageList.length > 0 ? result.messageList[0] : null;
  }

  // queryMessageFromRoute resolves the topic route and queries EVERY
  // broker (selectBrokerAddr of each BrokerData: master preferred, else the
  // lowest id — a slave-only topology must still be queryable), merging the
  // hits. QUERY_NOT_FOUND from one broker is not an error (log and
  // continue).
  async queryMessageFromRoute(topic: string, key: string,
    maxNum: number = 32, beginTimestamp = 0, endTimestamp = 0,
    indexType: string = 'K'): Promise<QueryResult> {
    const client = this._requireClient();
    if (client.getTopicRouteData(topic) == null) {
      await client.updateTopicRouteInfoFromNameServer(topic, false).catch(() => {});
    }
    const route = client.getTopicRouteData(topic);
    const merged = new QueryResult();
    if (route == null) return merged;
    for (const bd of route.brokerDatas || []) {
      const addrs: Record<string, string> = bd.brokerAddrs || {};
      const addr = addrs['0'] != null ? addrs['0'] : Object.values(addrs)[0];
      if (addr == null) continue;
      try {
        const one = await this.queryMessage(addr, topic, key, maxNum, beginTimestamp, endTimestamp, indexType);
        merged.messageList.push(...one.messageList);
        if (one.indexLastUpdateTimestamp > merged.indexLastUpdateTimestamp) {
          merged.indexLastUpdateTimestamp = one.indexLastUpdateTimestamp;
        }
      } catch (e) {
        if (e instanceof MQBrokerException && e.responseCode === ResponseCode.QUERY_NOT_FOUND) continue;
        throw e;
      }
    }
    return merged;
  }

  // viewMessage: try the offset-msgId decode first; a client UNIQ_KEY is NOT
  // an offset id and the decode throws (or yields a bogus port) — the caller
  // catches that and falls back
  // to the U-index query across the route's brokers. Keep the same contract:
  // never let a uniq key die inside decodeMessageId.
  async viewMessage(topic: string, msgId: string): Promise<MessageExt> {
    let addr = '';
    let offset = BigInt(0);
    let offsetIdOk = true;
    try {
      const parsed = decodeMessageId(msgId);
      if (parsed.port <= 0 || parsed.port > 65535) {
        offsetIdOk = false;
      } else {
        addr = `${parsed.ip}:${parsed.port}`;
        offset = BigInt(parsed.offset);
      }
    } catch {
      offsetIdOk = false;
    }
    if (!offsetIdOk) {
      return this.viewMessageByUniqKey(topic, msgId);
    }
    const client = this._requireClient();
    const request = this._request(RequestCode.VIEW_MESSAGE_BY_ID, adminExt({
      offset: String(offset),
      topic,
    }));
    const target = this._vipChannel(addr);
    const response = await client.remotingClient.invokeSync(target, request, this._timeout());
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQBrokerException(response.code, response.remark || `CODE: ${response.code}`, target);
    }
    if (response.body == null || response.body.length === 0) {
      throw new MQClientException(`message not found: ${msgId}`);
    }
    const msg = decodeMessage(response.body, true, true);
    if (msg == null) throw new MQClientException(`message not found: ${msgId}`);
    return msg;
  }

  // The uniq-key leg of the viewMessage fallback: the
  // U-index query is sent across every broker in
  // the route, first hit wins, verified against the UNIQ_KEY property so a
  // stray key collision can't satisfy the lookup.
  async viewMessageByUniqKey(topic: string, uniqKey: string): Promise<MessageExt> {
    const result = await this.queryMessageFromRoute(topic, uniqKey, 32, 0, 0, 'U');
    for (const m of result.messageList) {
      if (m.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) === uniqKey) return m;
    }
    throw new MQClientException(`message not found by uniq key: ${uniqKey}`);
  }

  // consumeMessageDirectly sends CONSUME_MESSAGE_DIRECTLY(309)
  // to the broker that STORES the message; the broker relays it to the
  // named client and answers with the client's verdict body. The store host is
  // resolved through viewMessage and the OFFSET msgId is substituted when the
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

  // ---------------- Name-server config ----------------

  // UPDATE_NAMESRV_CONFIG(318) broadcast: the properties ride in the BODY as
  // Properties-format TEXT; the first failing nameserver decides the error.
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

  // GET_NAMESRV_CONFIG(319), one request per nameserver (null/empty = all
  // known servers); the Properties-text body is parsed back per server.
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

  // ---------------- Container membership ----------------

  // ADD_BROKER(902) hands a broker config path to a broker container. The
  // container address is dialled VERBATIM — the VIP port is a broker-listener
  // contract and a container is not a broker — and the timeout is fixed at
  // 20000 ms regardless of the admin-wide timeout.
  async addBrokerToContainer(brokerContainerAddr: string, brokerConfig: string): Promise<void> {
    await this._invokeChecked(brokerContainerAddr, RequestCode.ADD_BROKER,
      adminExt({ configPath: brokerConfig }), null, DefaultAdminTimeoutMillis);
  }

  // REMOVE_BROKER(903): brokerName + brokerClusterName + brokerId, no body,
  // same verbatim-address / fixed-timeout rules as the add.
  async removeBrokerFromContainer(brokerContainerAddr: string, clusterName: string,
    brokerName: string, brokerId: number): Promise<void> {
    await this._invokeChecked(brokerContainerAddr, RequestCode.REMOVE_BROKER,
      adminExt({ brokerName, brokerClusterName: clusterName, brokerId }), null, DefaultAdminTimeoutMillis);
  }

  // ---------------- Ordered-topic config (KV-config based) ----------------

  // createOrUpdateOrderConf stores one ordered-topic config entry in the
  // ORDER_TOPIC_CONFIG namespace. isCluster stores the value verbatim;
  // otherwise the stored value is a ';'-joined merge keyed by the part before
  // the first ':': existing segments are kept verbatim (a replaced key keeps
  // its position, a new key is appended) and the new segment wins. The merge
  // reads the stored value from one name server (a failed read is ignored and
  // the merge starts from the new value alone; the value rides the GET
  // response HEADER, there is no body) and broadcasts the PUT to every name
  // server, where the LAST failing answer is the one rethrown.
  async createOrUpdateOrderConf(key: string, value: string, isCluster: boolean): Promise<void> {
    if (isCluster) {
      await this.putKVConfig(NAMESPACE_ORDER_TOPIC_CONFIG, key, value);
      return;
    }
    let oldOrderConfs: string | null = null;
    try {
      oldOrderConfs = await this.getKVConfig(NAMESPACE_ORDER_TOPIC_CONFIG, key);
    } catch (e) {
      logger.warn(`read order conf for ${key} failed: ${(e as Error).message}`);
    }
    const orderConfMap: Record<string, string> = {};
    if (!isBlank(oldOrderConfs)) {
      for (const oldOrderConf of oldOrderConfs!.split(';')) {
        const items = oldOrderConf.split(':');
        orderConfMap[items[0]] = oldOrderConf;
      }
    }
    const newItems = value.split(':');
    orderConfMap[newItems[0]] = value;
    const segments = Object.keys(orderConfMap).map((k) => orderConfMap[k]);
    await this.putKVConfig(NAMESPACE_ORDER_TOPIC_CONFIG, key, segments.join(';'));
  }

  // ---------------- Controller (leader-routed) ----------------

  // CONTROLLER_GET_METADATA_INFO(1005): which controller of the group leads.
  // The answer rides the response extFields.
  async getControllerMetaData(controllerAddr: string): Promise<GetMetaDataResponseHeader> {
    const response = await this._invokeRaw(controllerAddr, RequestCode.CONTROLLER_GET_METADATA_INFO,
      null, null, ControllerInvokeTimeoutMillis);
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQBrokerException(response.code, response.remark || `CODE: ${response.code}`, controllerAddr);
    }
    return GetMetaDataResponseHeader.fromDict(response.extFields || {});
  }

  // CONTROLLER_GET_SYNC_STATE_DATA(1006), two-phase: resolve the leader first,
  // then ask the LEADER for the replica state of `brokers` — a raw JSON array
  // of broker-name strings in the body, no header at all.
  async getInSyncStateData(controllerAddress: string, brokers: string[]): Promise<BrokerReplicasInfo> {
    const meta = await this.getControllerMetaData(controllerAddress);
    const leaderAddress = meta.controllerLeaderAddress;
    if (leaderAddress == null) {
      throw new MQBrokerException(ResponseCode.SYSTEM_ERROR, 'controller leader address not found',
        controllerAddress);
    }
    const response = await this._invokeRaw(leaderAddress, RequestCode.CONTROLLER_GET_SYNC_STATE_DATA,
      null, RemotingSerializable.encode(brokers), ControllerInvokeTimeoutMillis);
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQBrokerException(response.code, response.remark || `CODE: ${response.code}`, leaderAddress);
    }
    const body = response.body as Buffer | null;
    if (body == null || body.length === 0) return new BrokerReplicasInfo();
    return BrokerReplicasInfo.decode(body);
  }

  // CLEAN_BROKER_DATA(1011) on the leader. The header's nullable strings are
  // omitted when unset, while the two primitives — isCleanLivingBroker and
  // invokeTime (stamped when the request is built) — are ALWAYS sent, even
  // when false / freshly stamped.
  async cleanControllerBrokerData(controllerAddr: string, clusterName: string | null, brokerName: string,
    brokerControllerIdsToClean: string | null = null, isCleanLivingBroker = false): Promise<void> {
    const meta = await this.getControllerMetaData(controllerAddr);
    const leaderAddress = meta.controllerLeaderAddress;
    if (leaderAddress == null) {
      throw new MQBrokerException(ResponseCode.SYSTEM_ERROR, 'controller leader address not found',
        controllerAddr);
    }
    const ext: Record<string, string> = {};
    if (clusterName != null) ext['clusterName'] = clusterName;
    ext['brokerName'] = brokerName;
    if (brokerControllerIdsToClean != null) ext['brokerControllerIdsToClean'] = brokerControllerIdsToClean;
    ext['isCleanLivingBroker'] = isCleanLivingBroker ? 'true' : 'false';
    ext['invokeTime'] = String(Date.now());
    const response = await this._invokeRaw(leaderAddress, RequestCode.CLEAN_BROKER_DATA, ext, null,
      ControllerInvokeTimeoutMillis);
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQBrokerException(response.code, response.remark || `CODE: ${response.code}`, leaderAddress);
    }
  }

  // GET_CONTROLLER_CONFIG(1010) from every controller server — the given list,
  // or every name server when none is given, null when there is no address at
  // all. The body is Properties TEXT. One failing controller aborts the sweep
  // with a client exception.
  async getControllerConfig(controllerServers: string[] | null, timeoutMillis?: number):
    Promise<Record<string, Record<string, string>> | null> {
    const client = this._requireClient();
    const targets = (controllerServers == null || controllerServers.length === 0)
      ? client.getNameServerAddressList()
      : controllerServers;
    if (targets == null || targets.length === 0) return null;
    const configMap: Record<string, Record<string, string>> = {};
    for (const controller of targets) {
      const response = await this._invokeRaw(controller, RequestCode.GET_CONTROLLER_CONFIG,
        null, null, timeoutMillis);
      if (response.code !== ResponseCode.SUCCESS) throw this._clientError(response.code, response.remark);
      configMap[controller] = string2Properties(response.body != null ? response.body.toString('utf8') : '');
    }
    return configMap;
  }

  // UPDATE_CONTROLLER_CONFIG(1009): broadcast to every controller. An empty
  // property set (or an empty controller list) is a silent no-op; the sweep
  // keeps going after a failure and the LAST failing answer is rethrown.
  async updateControllerConfig(properties: Record<string, string>, controllers: string[] | null,
    timeoutMillis?: number): Promise<void> {
    const text = propertiesToString(properties);
    if (text.length < 1 || controllers == null || controllers.length === 0) return;
    const body = Buffer.from(text, 'utf8');
    let errResponse: RemotingCommand | null = null;
    for (const controller of controllers) {
      const response = await this._invokeRaw(controller, RequestCode.UPDATE_CONTROLLER_CONFIG,
        null, body, timeoutMillis);
      if (response.code !== ResponseCode.SUCCESS) errResponse = response;
    }
    if (errResponse != null) throw this._clientError(errResponse.code, errResponse.remark);
  }

  // ---------------- Cold-data flow control ----------------

  // UPDATE_COLD_DATA_FLOW_CTR_CONFIG(2001), body = Properties TEXT on the fast
  // channel. An empty property set sends NOTHING.
  async updateColdDataFlowCtrGroupConfig(brokerAddr: string, properties: Record<string, string>,
    timeoutMillis?: number): Promise<void> {
    const text = propertiesToString(properties);
    if (!text) return;
    await this._invokeBroker(brokerAddr, RequestCode.UPDATE_COLD_DATA_FLOW_CTR_CONFIG, null,
      Buffer.from(text, 'utf8'), timeoutMillis);
  }

  // REMOVE_COLD_DATA_FLOW_CTR_CONFIG(2002): the body is the group name
  // VERBATIM, not JSON. An empty group sends nothing.
  async removeColdDataFlowCtrGroupConfig(brokerAddr: string, consumerGroup: string,
    timeoutMillis?: number): Promise<void> {
    if (consumerGroup == null || consumerGroup.length === 0) return;
    await this._invokeBroker(brokerAddr, RequestCode.REMOVE_COLD_DATA_FLOW_CTR_CONFIG, null,
      Buffer.from(consumerGroup, 'utf8'), timeoutMillis);
  }

  // GET_COLD_DATA_FLOW_CTR_INFO(2003) — the raw body text, null when the
  // broker answers without one. This read is dialled at the listening port
  // even when the VIP switch is on.
  async getColdDataFlowCtrInfo(brokerAddr: string, timeoutMillis?: number): Promise<string | null> {
    const response = await this._invokeChecked(brokerAddr, RequestCode.GET_COLD_DATA_FLOW_CTR_INFO,
      null, null, timeoutMillis);
    const body = response.body as Buffer | null;
    if (body != null && body.length > 0) return body.toString('utf8');
    return null;
  }

  // ---------------- CommitLog read-ahead ----------------

  // SET_COMMITLOG_READ_MODE(2004). The request has no header class — the
  // single field is written by hand — and the broker's answer rides the
  // response REMARK, not a body. Never translated to the VIP port.
  async setCommitLogReadAheadMode(brokerAddr: string, mode: string,
    timeoutMillis?: number): Promise<string | null> {
    const response = await this._invokeChecked(brokerAddr, RequestCode.SET_COMMITLOG_READ_MODE,
      { [FILE_READAHEAD_MODE_KEY]: mode }, null, timeoutMillis);
    if (response.remark != null && response.remark.length > 0) return response.remark;
    return null;
  }

  // ---------------- Pop record export ----------------

  // POP_ROLLBACK(200056) asks the broker to dump its pop records. The
  // CALLER's timeout is forwarded verbatim — the admin default does not apply
  // here — and the address is dialled as passed.
  async exportPopRecords(brokerAddr: string, timeout: number): Promise<void> {
    const client = this._requireClient();
    const response = await client.remotingClient.invokeSync(brokerAddr,
      this._request(RequestCode.POP_ROLLBACK, null), timeout);
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQBrokerException(response.code, response.remark || `CODE: ${response.code}`, brokerAddr);
    }
  }

  // ---------------- RocksDB consume-queue ops ----------------

  // CHECK_ROCKSDB_CQ_WRITE_PROGRESS(354). checkStoreTime is a primitive header
  // field and is ALWAYS sent — including 0 — and the request is never
  // translated to the VIP port.
  async checkRocksdbCqWriteProgress(brokerAddr: string, topic: string, checkStoreTime: number,
    timeoutMillis?: number): Promise<CheckRocksdbCqWriteResult> {
    const response = await this._invokeChecked(brokerAddr, RequestCode.CHECK_ROCKSDB_CQ_WRITE_PROGRESS,
      adminExt({ topic, checkStoreTime }), null, timeoutMillis);
    const body = response.body as Buffer | null;
    if (body == null || body.length === 0) {
      throw new MQClientException(`empty checkRocksdbCqWriteProgress body from ${brokerAddr}`);
    }
    return CheckRocksdbCqWriteResult.decode(body);
  }

  // EXPORT_ROCKSDB_CONFIG_TO_JSON(355): the requested config types ride the
  // header as lowercase names, each followed by the ';' separator. Code-only:
  // the broker answers without a body.
  async exportRocksDBConfigToJson(brokerAddr: string, configType: string[],
    timeoutMillis?: number): Promise<void> {
    const value = configType.map((t) => `${t.toLowerCase()};`).join('');
    await this._invokeChecked(brokerAddr, RequestCode.EXPORT_ROCKSDB_CONFIG_TO_JSON,
      { configType: value }, null, timeoutMillis);
  }

  // ---------------- Lite topic admin ----------------

  // Shared leaf of the lite-admin RPCs: VIP translate, raise a broker
  // exception on non-SUCCESS, and decode the body only when one actually
  // arrived (a code-only answer decodes as null, not as an empty object).
  private async _invokeBrokerMethod<T>(
    brokerAddr: string, code: number, ext: Record<string, string> | null,
    decode: ((body: Buffer) => T) | null, timeoutMillis?: number): Promise<T | null> {
    const response = await this._invokeBrokerRaw(brokerAddr, code, ext, null, timeoutMillis);
    if (response.code !== ResponseCode.SUCCESS) {
      throw new MQBrokerException(response.code, response.remark || `CODE: ${response.code}`, brokerAddr);
    }
    const body = response.body as Buffer | null;
    if (body == null || body.length === 0 || decode == null) return null;
    return decode(body);
  }

  // GET_BROKER_LITE_INFO(200074): broker-level lite-store summary. No header
  // fields at all.
  async getBrokerLiteInfo(brokerAddr: string, timeoutMillis?: number): Promise<GetBrokerLiteInfoResponseBody | null> {
    return this._invokeBrokerMethod(brokerAddr, RequestCode.GET_BROKER_LITE_INFO, null,
      GetBrokerLiteInfoResponseBody.decode, timeoutMillis);
  }

  // GET_PARENT_TOPIC_INFO(200075): one parent topic's lite-store summary.
  async getParentTopicInfo(brokerAddr: string, topic: string,
    timeoutMillis?: number): Promise<GetParentTopicInfoResponseBody | null> {
    return this._invokeBrokerMethod(brokerAddr, RequestCode.GET_PARENT_TOPIC_INFO, authExt({ topic }),
      GetParentTopicInfoResponseBody.decode, timeoutMillis);
  }

  // GET_LITE_TOPIC_INFO(200076): one lite topic's detail; liteTopic is
  // optional and an unset value is OMITTED, not sent empty.
  async getLiteTopicInfo(brokerAddr: string, parentTopic: string, liteTopic: string | null,
    timeoutMillis?: number): Promise<GetLiteTopicInfoResponseBody | null> {
    return this._invokeBrokerMethod(brokerAddr, RequestCode.GET_LITE_TOPIC_INFO,
      authExt({ parentTopic, liteTopic }), GetLiteTopicInfoResponseBody.decode, timeoutMillis);
  }

  // GET_LITE_CLIENT_INFO(200077): one client's lite view. maxCount is a
  // primitive header field and rides along ALWAYS (even 0).
  async getLiteClientInfo(brokerAddr: string, parentTopic: string, group: string,
    clientId: string | null, timeoutMillis?: number): Promise<GetLiteClientInfoResponseBody | null> {
    return this._invokeBrokerMethod(brokerAddr, RequestCode.GET_LITE_CLIENT_INFO,
      authExt({ parentTopic, group, clientId, maxCount: 1000 }),
      GetLiteClientInfoResponseBody.decode, timeoutMillis);
  }

  // GET_LITE_GROUP_INFO(200078): one group's lite lag view. topK is a
  // primitive and rides along ALWAYS; liteTopic is optional and omitted when
  // unset.
  async getLiteGroupInfo(brokerAddr: string, group: string, liteTopic: string | null, topK: number,
    timeoutMillis?: number): Promise<GetLiteGroupInfoResponseBody | null> {
    return this._invokeBrokerMethod(brokerAddr, RequestCode.GET_LITE_GROUP_INFO,
      authExt({ group, liteTopic, topK }), GetLiteGroupInfoResponseBody.decode, timeoutMillis);
  }

  // TRIGGER_LITE_DISPATCH(200079): fire-and-forget dispatch nudge, no body.
  async triggerLiteDispatch(brokerAddr: string, group: string, clientId: string | null,
    timeoutMillis?: number): Promise<void> {
    await this._invokeBrokerMethod(brokerAddr, RequestCode.TRIGGER_LITE_DISPATCH,
      authExt({ group, clientId }), null, timeoutMillis);
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

  // ---------------- Auth user / ACL management ----------------

  // createUser / updateUser: AUTH_CREATE_USER(3001) / AUTH_UPDATE_USER(3002).
  // Header carries `username`; the body is the user object. Either hand over a
  // UserInfo or the (username, password, userType) triple — the triple leaves
  // userStatus unset, so that key is ABSENT from the body.
  async createUser(addr: string, user: UserInfo | string, password: string | null = null,
    userType: string | null = null, timeoutMillis?: number): Promise<void> {
    await this._invokeUser(RequestCode.AUTH_CREATE_USER, addr, user, password, userType, timeoutMillis);
  }

  async updateUser(addr: string, user: UserInfo | string, password: string | null = null,
    userType: string | null = null, timeoutMillis?: number): Promise<void> {
    await this._invokeUser(RequestCode.AUTH_UPDATE_USER, addr, user, password, userType, timeoutMillis);
  }

  // AUTH_DELETE_USER(3003): header `username`, no body.
  async deleteUser(addr: string, username: string, timeoutMillis?: number): Promise<void> {
    await this._invokeChecked(addr, RequestCode.AUTH_DELETE_USER, authExt({ username }), null, timeoutMillis);
  }

  // AUTH_GET_USER(3004): header `username`; the body is one user object, an
  // empty body being no user at all.
  async getUser(addr: string, username: string, timeoutMillis?: number): Promise<UserInfo | null> {
    const response = await this._invokeChecked(addr, RequestCode.AUTH_GET_USER,
      authExt({ username }), null, timeoutMillis);
    const body = response.body as Buffer;
    if (body == null || body.length === 0) return null;
    return UserInfo.decode(body);
  }

  // AUTH_LIST_USER(3005): header `filter` (unset means no filter header at all);
  // the body is a JSON array of user objects.
  async listUser(addr: string, filter: string | null = null,
    timeoutMillis?: number): Promise<UserInfo[]> {
    const response = await this._invokeChecked(addr, RequestCode.AUTH_LIST_USER,
      authExt({ filter }), null, timeoutMillis);
    const body = response.body as Buffer;
    if (body == null || body.length === 0) return [];
    return UserInfo.decodeList(body);
  }

  // createAcl / updateAcl: AUTH_CREATE_ACL(3006) / AUTH_UPDATE_ACL(3007).
  // Header carries `subject`; the body is the acl object. Either hand over an
  // AclInfo or the (subject, resources, actions, sourceIps, decision) triple,
  // which builds one policy with policyType ABSENT and one entry per resource.
  async createAcl(addr: string, acl: AclInfo | string, resources: string[] | null = null,
    actions: string[] | null = null, sourceIps: string[] | null = null,
    decision: string | null = null, timeoutMillis?: number): Promise<void> {
    await this._invokeAcl(RequestCode.AUTH_CREATE_ACL, addr, acl, resources, actions,
      sourceIps, decision, timeoutMillis);
  }

  async updateAcl(addr: string, acl: AclInfo | string, resources: string[] | null = null,
    actions: string[] | null = null, sourceIps: string[] | null = null,
    decision: string | null = null, timeoutMillis?: number): Promise<void> {
    await this._invokeAcl(RequestCode.AUTH_UPDATE_ACL, addr, acl, resources, actions,
      sourceIps, decision, timeoutMillis);
  }

  // AUTH_DELETE_ACL(3008): headers `subject` + `resource` and NOTHING else —
  // the policy shape is not part of this request. An unset resource is
  // omitted, which asks for the subject's whole grant set.
  async deleteAcl(addr: string, subject: string, resource: string | null = null,
    timeoutMillis?: number): Promise<void> {
    await this._invokeChecked(addr, RequestCode.AUTH_DELETE_ACL,
      authExt({ subject, resource }), null, timeoutMillis);
  }

  // AUTH_GET_ACL(3009): header `subject`; the body is one acl object.
  async getAcl(addr: string, subject: string, timeoutMillis?: number): Promise<AclInfo | null> {
    const response = await this._invokeChecked(addr, RequestCode.AUTH_GET_ACL,
      authExt({ subject }), null, timeoutMillis);
    const body = response.body as Buffer;
    if (body == null || body.length === 0) return null;
    return AclInfo.decode(body);
  }

  // AUTH_LIST_ACL(3010): headers `subjectFilter` + `resourceFilter`; the body is
  // a JSON array of acl objects.
  async listAcl(addr: string, subjectFilter: string | null = null,
    resourceFilter: string | null = null, timeoutMillis?: number): Promise<AclInfo[]> {
    const response = await this._invokeChecked(addr, RequestCode.AUTH_LIST_ACL,
      authExt({ subjectFilter, resourceFilter }), null, timeoutMillis);
    const body = response.body as Buffer;
    if (body == null || body.length === 0) return [];
    return AclInfo.decodeList(body);
  }

  // QUERY_CONSUME_QUEUE(321) — the consume-queue slice the broker serves for a
  // topic/queue, optionally narrowed by the group's subscription.
  async queryConsumeQueue(brokerAddr: string, topic: string, queueId: number, index: number,
    count = 32, consumerGroup = '', timeoutMillis?: number): Promise<QueryConsumeQueueResponseBody> {
    const response = await this._invokeBroker(brokerAddr, RequestCode.QUERY_CONSUME_QUEUE,
      adminExt({ topic, queueId, index, count, consumerGroup }), null, timeoutMillis);
    const body = response.body as Buffer | null;
    if (body == null || body.length === 0) return new QueryConsumeQueueResponseBody();
    return QueryConsumeQueueResponseBody.decode(body);
  }

  // UPDATE_AND_GET_GROUP_FORBIDDEN(353) — flips (readable=null: query-only) a
  // group's read permission on one broker and returns the stored entry. An
  // unset readable is omitted from the request entirely: the broker treats
  // "absent" and "false" as different operations.
  async updateAndGetGroupReadForbidden(brokerAddr: string, group: string, topic: string,
    readable?: boolean | null, timeoutMillis?: number): Promise<GroupForbidden> {
    if (!group || !topic) {
      throw new MQClientException('updateAndGetGroupReadForbidden: group/topic required');
    }
    const ext: Record<string, string> = { group, topic };
    if (readable != null) ext['readable'] = readable ? 'true' : 'false';
    const response = await this._invokeBroker(brokerAddr, RequestCode.UPDATE_AND_GET_GROUP_FORBIDDEN,
      ext, null, timeoutMillis);
    const body = response.body as Buffer | null;
    if (body == null || body.length === 0) {
      throw new MQClientException('updateAndGetGroupReadForbidden: empty response body');
    }
    return GroupForbidden.decode(body);
  }

  // Whether the group has consumed past the message: only the consumer offset
  // of the message's own queue on its own master counts.
  private async _consumed(msg: MessageExt, group: string): Promise<boolean> {
    const cstats = await this.examineConsumeStats(group);
    const clusterInfo = await this.examineBrokerClusterInfo();
    for (const [mq, wrapper] of cstats.offsetTable) {
      if (mq.topic !== msg.topic || mq.queueId !== msg.queueId) continue;
      const brokerAddrs = clusterInfo.brokerAddrTable[mq.brokerName];
      if (brokerAddrs == null) continue;
      const addr = brokerAddrs[MixAll.MASTER_ID];
      const storeHost = msg.getStoreHostString();
      if (addr != null && storeHost != null && addr === storeHost
        && wrapper.consumerOffset > msg.getQueueOffset()) {
        return true;
      }
    }
    return false;
  }

  // Who consumes this topic, and what happened to the message in each group:
  // CONSUMED / CONSUMED_BUT_FILTERED / PULL / NOT_CONSUME_YET / NOT_ONLINE /
  // CONSUME_BROADCASTING. A group whose probing fails carries the failure in
  // exceptionDesc and stays UNKNOWN. Groups are sorted for deterministic output.
  async messageTrackDetail(msg: MessageExt): Promise<MessageTrack[]> {
    const groups = await this.queryTopicConsumeByWho(msg.topic);
    const names = (groups != null && groups.groupList != null) ? groups.groupList.slice() : [];
    names.sort();
    const result: MessageTrack[] = [];
    for (const group of names) {
      result.push(await this._trackOneGroup(msg, group));
    }
    return result;
  }

  // Concurrent flavor: every group's verdict is probed in parallel. The group
  // list is NOT sorted here — each track lands as its probe completes the
  // shared walk order, and the verdict logic is the per-group leaf above.
  async messageTrackDetailConcurrent(msg: MessageExt): Promise<MessageTrack[]> {
    const groups = await this.queryTopicConsumeByWho(msg.topic);
    const names = (groups != null && groups.groupList != null) ? groups.groupList.slice() : [];
    return Promise.all(names.map((group) => this._trackOneGroup(msg, group)));
  }

  // One group's leg of the message track: connection probe, pull/passive
  // split, consumed check, tag-filter verdict.
  private async _trackOneGroup(msg: MessageExt, group: string): Promise<MessageTrack> {
    const mt = new MessageTrack(group, TrackType.UNKNOWN);
    let cc: ConsumerConnection;
    try {
      cc = await this.examineConsumerConnectionInfo(group);
    } catch (e) {
      this._fillTrackFailure(mt, e);
      return mt;
    }

    if (cc.consumeType === ConsumeType.CONSUME_ACTIVELY) {
      mt.trackType = TrackType.PULL;
      return mt;
    }
    if (cc.consumeType !== ConsumeType.CONSUME_PASSIVELY) {
      return mt;
    }

    let ifConsumed = false;
    try {
      ifConsumed = await this._consumed(msg, group);
    } catch (e) {
      this._fillTrackFailure(mt, e);
      return mt;
    }

    if (ifConsumed) {
      mt.trackType = TrackType.CONSUMED;
      // A narrower subscription than the message's tag means this group
      // receives-but-filters: SQL92 subscriptions keep an empty tagsSet and
      // fall back to CONSUMED.
      const sub = cc.subscriptionTable[msg.topic];
      const tagsSet = (sub != null && Array.isArray(sub['tagsSet']))
        ? sub['tagsSet'].map((t: any) => String(t)) : [];
      const tag = msg.getTags();
      if (tagsSet.length > 0 && !tagsSet.includes('*') && !tagsSet.includes(String(tag))) {
        mt.trackType = TrackType.CONSUMED_BUT_FILTERED;
      }
    } else {
      mt.trackType = TrackType.NOT_CONSUME_YET;
    }
    return mt;
  }

  private _fillTrackFailure(mt: MessageTrack, e: unknown): void {
    const err = e as { responseCode?: number; message?: string };
    const code = err?.responseCode;
    if (code === ResponseCode.CONSUMER_NOT_ONLINE) mt.trackType = TrackType.NOT_ONLINE;
    else if (code === ResponseCode.BROADCAST_CONSUMPTION) mt.trackType = TrackType.CONSUME_BROADCASTING;
    mt.exceptionDesc = code != null
      ? `CODE:${code} DESC:${err?.message ?? ''}`
      : (err?.message ?? String(e));
  }

  private async _invokeUser(code: number, addr: string, user: UserInfo | string,
    password: string | null, userType: string | null, timeoutMillis?: number): Promise<void> {
    const info = typeof user === 'string' ? new UserInfo(user, password, userType, null) : user;
    await this._invokeChecked(addr, code, authExt({ username: info.username }),
      info.encode(), timeoutMillis);
  }

  private async _invokeAcl(code: number, addr: string, acl: AclInfo | string,
    resources: string[] | null, actions: string[] | null, sourceIps: string[] | null,
    decision: string | null, timeoutMillis?: number): Promise<void> {
    const info = typeof acl === 'string'
      ? AclInfo.fromResources(acl, resources, actions, sourceIps, decision)
      : acl;
    await this._invokeChecked(addr, code, authExt({ subject: info.subject }),
      info.encode(), timeoutMillis);
  }
}

export default DefaultMQAdminExt;

// Track verdicts for messageTrackDetail. Kept as string constants so the
// values travel unchanged through JSON.
export const TrackType = {
  CONSUMED: 'CONSUMED',
  CONSUMED_BUT_FILTERED: 'CONSUMED_BUT_FILTERED',
  PULL: 'PULL',
  NOT_CONSUME_YET: 'NOT_CONSUME_YET',
  NOT_ONLINE: 'NOT_ONLINE',
  CONSUME_BROADCASTING: 'CONSUME_BROADCASTING',
  UNKNOWN: 'UNKNOWN',
} as const;

export class MessageTrack {
  consumerGroup: string;
  trackType: string;
  exceptionDesc: string | null;
  constructor(consumerGroup: string, trackType: string = TrackType.UNKNOWN) {
    this.consumerGroup = consumerGroup;
    this.trackType = trackType;
    this.exceptionDesc = null;
  }
}
