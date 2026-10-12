// Static topic (unitized topic) support — offline wire test.
//
// 行为基线（对齐 Java）：
//   * TopicRouteData.topicQueueMappingByBroker 从路由 JSON 原样解码（null 与
//     {} 语义不同：null = 非静态主题；{} 空表 = 静态但暂无映射）。
//   * topicRouteData2EndpointsForStaticTopic：按 scope 分组 → epoch 降序 →
//     currIdMap 展开为逻辑队列 (topic, __syslo__<scope>, globalId) → 真实
//     broker 名；totalQueues 范围内无人托管的逻辑队列映射到
//     __syslo__none__（Java LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST）。
//   * 同 epoch 后写覆盖先写（Java oldEpoch <= epoch）；epoch 更小的信息
//     不得覆盖更大 epoch 的端点。
//   * __global__ 的 mock broker 名剥掉前两个下划线：__syslo__global__。
//   * TopicQueueMappingDetail.encode 的字段名必须是 Java 属性名（broker 用
//     fastjson2 反序列化，错一个静默丢字段）；currIdMap/hostedQueues 的
//     JSON 键是字符串化整数。
//   * 静态主题的发布/订阅队列集取逻辑端点（按 queueId 排序），不再走
//     queueDatas 的物理布局。
//   * MQClient.getBrokerNameFromMessageQueue 通过端点表把 mock 名解析回
//     真实 broker 名；未知队列原样透传。
//
// Run: node --experimental-strip-types test/static_topic_smoke.ts
import assert from 'node:assert';

import { MessageQueue } from '../src/common/message.ts';
import { TopicRouteData } from '../src/remoting/route.ts';
import {
  LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST,
  LogicQueueMappingItem,
  METADATA_SCOPE_GLOBAL,
  TopicQueueMappingDetail,
  TopicQueueMappingInfo,
  endpointsToKeyStrings,
  getMockBrokerName,
  isLogicalQueueMockBrokerName,
  topicRouteData2EndpointsForStaticTopic,
} from '../src/remoting/static_topic.ts';
import { MQClient } from '../src/client/mq_client.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

// ---------------------------------------------------------------------------
// 1. mock broker names
// ---------------------------------------------------------------------------
{
  check('getMockBrokerName global scope strips __',
    getMockBrokerName(METADATA_SCOPE_GLOBAL) === '__syslo__global__');
  check('getMockBrokerName other scope verbatim',
    getMockBrokerName('zone-a') === '__syslo__zone-a');
  check('isLogicalQueueMockBrokerName', isLogicalQueueMockBrokerName('__syslo__global__')
    && !isLogicalQueueMockBrokerName('broker-a'));
}

// ---------------------------------------------------------------------------
// 2. route decode: null vs {} preserved
// ---------------------------------------------------------------------------
{
  const trd = new TopicRouteData();
  check('default route has null mapping', trd.topicQueueMappingByBroker === null);
  const withEmpty = TopicRouteData.fromDict({
    queueDatas: [], brokerDatas: [], filterServerTable: {},
    topicQueueMappingByBroker: {},
  });
  check('empty {} mapping preserved (not nullified)',
    withEmpty.topicQueueMappingByBroker != null
    && Object.keys(withEmpty.topicQueueMappingByBroker).length === 0);
}

// ---------------------------------------------------------------------------
// 3. endpoints: single broker, global scope, with an unhosted logical queue
// ---------------------------------------------------------------------------
{
  const route = TopicRouteData.fromDict({
    orderTopicConf: null,
    queueDatas: [{ brokerName: 'broker-a', readQueueNums: 8, writeQueueNums: 8, perm: 6, topicSysFlag: 0 }],
    brokerDatas: [{ cluster: 'DefaultCluster', brokerName: 'broker-a', brokerAddrs: { '0': '127.0.0.1:10911' } }],
    filterServerTable: {},
    topicQueueMappingByBroker: {
      'broker-a': {
        topic: 'StaticTopic', scope: '__global__', totalQueues: 4, bname: 'broker-a',
        epoch: 0, dirty: false, currIdMap: { '0': 0, '1': 1, '2': 2 },
      },
    },
  });

  const endpoints = topicRouteData2EndpointsForStaticTopic('StaticTopic', route);
  check('endpoints cover totalQueues (hosted + unhosted)', endpoints.size === 4);
  const view = endpointsToKeyStrings(endpoints);
  check('global 0 -> broker-a', view.get('StaticTopic@__syslo__global__@0') === 'broker-a');
  check('global 1 -> broker-a', view.get('StaticTopic@__syslo__global__@1') === 'broker-a');
  check('global 2 -> broker-a', view.get('StaticTopic@__syslo__global__@2') === 'broker-a');
  check('global 3 unhosted -> NOT_EXIST sentinel',
    view.get('StaticTopic@__syslo__global__@3') === LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST);
}

// ---------------------------------------------------------------------------
// 4. endpoints: two scopes kept apart + epoch fencing
// ---------------------------------------------------------------------------
{
  const route = TopicRouteData.fromDict({
    queueDatas: [], brokerDatas: [], filterServerTable: {},
    topicQueueMappingByBroker: {
      'broker-a': {
        topic: 'T', scope: '__global__', totalQueues: 2, bname: 'broker-a',
        epoch: 5, dirty: false, currIdMap: { '0': 0, '1': 1 },
      },
      'broker-b': {
        topic: 'T', scope: 'zone-x', totalQueues: 1, bname: 'broker-b',
        epoch: 9, dirty: false, currIdMap: { '0': 0 },
      },
    },
  });
  const view = endpointsToKeyStrings(topicRouteData2EndpointsForStaticTopic('T', route));
  check('global scope endpoint', view.get('T@__syslo__global__@0') === 'broker-a');
  check('zone scope mock name verbatim', view.get('T@__syslo__zone-x@0') === 'broker-b');
  check('scopes do not merge (2 + 1 logical queues)', view.size === 3);
}

{
  // Two brokers claim the same global id; the HIGHER epoch wins even if the
  // map iteration puts it first — and equal epochs overwrite (last one wins).
  const route = TopicRouteData.fromDict({
    queueDatas: [], brokerDatas: [], filterServerTable: {},
    topicQueueMappingByBroker: {
      'broker-old': { topic: 'T', scope: '__global__', totalQueues: 1, bname: 'broker-old', epoch: 3, dirty: false, currIdMap: { '0': 0 } },
      'broker-new': { topic: 'T', scope: '__global__', totalQueues: 1, bname: 'broker-new', epoch: 7, dirty: false, currIdMap: { '0': 0 } },
    },
  });
  const view = endpointsToKeyStrings(topicRouteData2EndpointsForStaticTopic('T', route));
  check('higher epoch fences the endpoint', view.get('T@__syslo__global__@0') === 'broker-new');
}

// ---------------------------------------------------------------------------
// 5. TopicQueueMappingDetail encode — exact Java property names
// ---------------------------------------------------------------------------
{
  const detail = new TopicQueueMappingDetail('StaticTopic', 2, 'broker-a', 1,
    METADATA_SCOPE_GLOBAL, false, { '0': 0, '1': 1 }, {
      '0': [new LogicQueueMappingItem(0, 0, 'broker-a', 0, 0, -1, -1, -1)],
      '1': [new LogicQueueMappingItem(0, 1, 'broker-a', 1, 0, -1, -1, -1)],
    });
  const d = JSON.parse(detail.encode().toString('utf8'));
  check('encode: topic/scope/totalQueues/bname/epoch/dirty exact keys',
    d['topic'] === 'StaticTopic' && d['scope'] === '__global__'
    && d['totalQueues'] === 2 && d['bname'] === 'broker-a'
    && d['epoch'] === 1 && d['dirty'] === false);
  check('encode: currIdMap keys stringified', d['currIdMap']['0'] === 0 && d['currIdMap']['1'] === 1);
  check('encode: hostedQueues item fields exact (gen/queueId/bname/logicOffset/startOffset/endOffset/timeOfStart/timeOfEnd)',
    Array.isArray(d['hostedQueues']['0'])
    && d['hostedQueues']['0'][0]['gen'] === 0
    && d['hostedQueues']['0'][0]['queueId'] === 0
    && d['hostedQueues']['0'][0]['bname'] === 'broker-a'
    && d['hostedQueues']['0'][0]['logicOffset'] === 0
    && d['hostedQueues']['0'][0]['startOffset'] === 0
    && d['hostedQueues']['0'][0]['endOffset'] === -1
    && d['hostedQueues']['0'][0]['timeOfStart'] === -1
    && d['hostedQueues']['0'][0]['timeOfEnd'] === -1);
  const back = TopicQueueMappingDetail.fromDict(JSON.parse(detail.encode().toString('utf8')));
  check('encode -> fromDict round-trip', back.totalQueues === 2
    && back.currIdMap['0'] === 0 && back.hostedQueues['1'][0].queueId === 1);
  check('fromDict defaults scope to __global__',
    TopicQueueMappingInfo.fromDict({}).scope === METADATA_SCOPE_GLOBAL);
}

// ---------------------------------------------------------------------------
// 6. publish/subscribe queue sets take the static branch
// ---------------------------------------------------------------------------
{
  const route = TopicRouteData.fromDict({
    orderTopicConf: null,
    queueDatas: [{ brokerName: 'broker-a', readQueueNums: 8, writeQueueNums: 8, perm: 6, topicSysFlag: 0 }],
    brokerDatas: [{ cluster: 'DefaultCluster', brokerName: 'broker-a', brokerAddrs: { '0': '127.0.0.1:10911' } }],
    filterServerTable: {},
    topicQueueMappingByBroker: {
      'broker-a': { topic: 'StaticTopic', scope: '__global__', totalQueues: 4, bname: 'broker-a', epoch: 0, dirty: false, currIdMap: { '0': 0, '1': 1, '2': 2, '3': 3 } },
    },
  });
  const pub = route.getAllMessageQueue('StaticTopic');
  check('publish set = logical queues (not the 8 physical)',
    pub.length === 4 && pub.every((mq) => mq.brokerName === '__syslo__global__'));
  check('publish set sorted by queueId',
    pub.map((mq) => mq.queueId).join(',') === '0,1,2,3');
  const sub = route.getAllSubscribeMessageQueue('StaticTopic');
  check('subscribe set = logical queues', sub.length === 4 && sub[0].brokerName === '__syslo__global__');

  // A NON-static route keeps the physical layout.
  const plain = TopicRouteData.fromDict({
    orderTopicConf: null,
    queueDatas: [{ brokerName: 'broker-a', readQueueNums: 8, writeQueueNums: 8, perm: 6, topicSysFlag: 0 }],
    brokerDatas: [{ cluster: 'DefaultCluster', brokerName: 'broker-a', brokerAddrs: { '0': '127.0.0.1:10911' } }],
    filterServerTable: {},
  });
  check('non-static route still uses queueDatas (8 physical queues)',
    plain.getAllMessageQueue('Plain').length === 8 && plain.getStaticTopicEndPoints('Plain') === null);
}

// ---------------------------------------------------------------------------
// 7. MQClient.getBrokerNameFromMessageQueue via topicEndPointsTable
// ---------------------------------------------------------------------------
{
  const client = new MQClient('static-smoke@DEFAULT');
  const route = TopicRouteData.fromDict({
    orderTopicConf: null,
    queueDatas: [], brokerDatas:
      [{ cluster: 'DefaultCluster', brokerName: 'broker-a', brokerAddrs: { '0': '127.0.0.1:10911' } },
       { cluster: 'DefaultCluster', brokerName: 'broker-b', brokerAddrs: { '0': '127.0.0.2:10911' } }],
    filterServerTable: {},
    topicQueueMappingByBroker: {
      'broker-a': { topic: 'StaticTopic', scope: '__global__', totalQueues: 2, bname: 'broker-a', epoch: 0, dirty: false, currIdMap: { '0': 0 } },
      'broker-b': { topic: 'StaticTopic', scope: '__global__', totalQueues: 2, bname: 'broker-b', epoch: 0, dirty: false, currIdMap: { '1': 0 } },
    },
  });
  client.topicRouteData2TopicPublishInfo('StaticTopic', route);
  // global 0 hosted by broker-a, global 1 by broker-b (endpoints by queueId).
  check('endpoints table built on route refresh', client.topicEndPointsTable.has('StaticTopic'));
  check('mock global 0 resolves to broker-a',
    client.getBrokerNameFromMessageQueue(new MessageQueue('StaticTopic', '__syslo__global__', 0)) === 'broker-a');
  check('mock global 1 resolves to broker-b',
    client.getBrokerNameFromMessageQueue(new MessageQueue('StaticTopic', '__syslo__global__', 1)) === 'broker-b');
  check('unknown queue passes its own broker name through',
    client.getBrokerNameFromMessageQueue(new MessageQueue('StaticTopic', '__syslo__global__', 99)) === '__syslo__global__');
  check('non-static topic passes broker name through',
    client.getBrokerNameFromMessageQueue(new MessageQueue('Plain', 'broker-a', 0)) === 'broker-a');
  // route refresh on a non-static topic clears a stale endpoints table.
  const plain = TopicRouteData.fromDict({ queueDatas: [], brokerDatas: [], filterServerTable: {} });
  client.topicRouteData2TopicPublishInfo('StaticTopic', plain);
  check('stale endpoints table cleared on non-static refresh',
    !client.topicEndPointsTable.has('StaticTopic'));
}

// ---------------------------------------------------------------------------
console.log(`\nstatic_topic_smoke: PASS=${pass} FAIL=${fail}`);
if (fail > 0) process.exit(1);
