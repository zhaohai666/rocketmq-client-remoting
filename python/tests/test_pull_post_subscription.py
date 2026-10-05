# -*- coding: utf-8 -*-
"""P5：postSubscriptionWhenPull 与 updatePullFromWhichNode（PullAPIWrapper 移植）离线单测。

对齐 Java：
  - `DefaultMQPushConsumer#postSubscriptionWhenPull`（默认 false）+ `DefaultMQPushConsumerImpl
    .pullMessage:458-468`：默认**不发**订阅表达式（subscription 位清零），tag 过滤由客户端
    processPullResult 兜底；打开且非类过滤模式时才发 subString。
  - `PullAPIWrapper#pullKernelImpl:197-205`：按 `recalculatePullFromWhichNode(mq)` 选
    brokerId，对应 `MQClientInstance#findBrokerAddressInSubscribe`（命中 brokerId；从节点
    未命中按 brokerId+1 再试；不限定时回退到任一节点）；选中从节点时清 COMMIT_OFFSET 位
    （:219-221）。
  - `PullAPIWrapper#processPullResult:77`：每轮应答的 `suggestWhichBrokerId` 更新
    pullFromWhichNodeTable（缺省按 master=0）。
"""
import threading

from client.consumer import DefaultMQPushConsumer
from client.consumer_result import PullResult, PullStatus
from client.mq_client import MQClientInstance
from common.message import MessageQueue
from common.mix_all import MixAll
from common.subscription_data import SubscriptionData
from common.sysflag import PullSysFlag
from remoting.protocol.remoting_command import RemotingCommand
from remoting.protocol.route import BrokerData, TopicRouteData

GROUP = "GID_P5Unit"
TOPIC = "P5UnitTopic"
MQ = MessageQueue(TOPIC, "broker-a", 0)
MASTER = "127.0.0.1:10911"
SLAVE = "127.0.0.1:10921"
SLAVE2 = "127.0.0.1:10931"


# ---------------- find_broker_addr_in_subscribe（Java findBrokerAddressInSubscribe）----------------

def _addrs():
    return {0: MASTER, 1: SLAVE, 2: SLAVE2}


def test_find_broker_addr_in_subscribe_hits_exact_id():
    assert MQClientInstance.find_broker_addr_in_subscribe(_addrs(), 0) == (MASTER, False)
    assert MQClientInstance.find_broker_addr_in_subscribe(_addrs(), 1) == (SLAVE, True)
    assert MQClientInstance.find_broker_addr_in_subscribe(_addrs(), 2) == (SLAVE2, True)


def test_find_broker_addr_in_subscribe_slave_miss_tries_next_id():
    # Java 的从节点 id+1 约定：建议 id=1 不存在时试 id=2
    addrs = {0: MASTER, 2: SLAVE2}
    assert MQClientInstance.find_broker_addr_in_subscribe(addrs, 1) == (SLAVE2, True)


def test_find_broker_addr_in_subscribe_falls_back_to_any_node():
    # 不限定 brokerId（onlyThisBroker=false）时回退到 id 最小（=master）
    assert MQClientInstance.find_broker_addr_in_subscribe({1: SLAVE}, 3) == (SLAVE, True)
    assert MQClientInstance.find_broker_addr_in_subscribe(_addrs(), 9) == (MASTER, False)


def test_find_broker_addr_in_subscribe_empty_table():
    assert MQClientInstance.find_broker_addr_in_subscribe({}, 0) == (None, False)


# ---------------- pull_message 的 brokerId 选路与从节点 COMMIT_OFFSET 清位 ----------------

class _Instance:
    """最小可用 MQClientInstance：预置路由 + 记录 _invoke_sync 的地址与请求。"""

    def __init__(self, suggest, route):
        self.instance = MQClientInstance("ut@client", ["127.0.0.1:9876"],
                                         connect_timeout_millis=3000,
                                         invoke_timeout_millis=15000,
                                         tls_enable=False)
        self.instance.topic_route_table[TOPIC] = route
        self.requests = []
        self._suggest = suggest
        # 替掉传输层：记录 (addr, request)，回 SUCCESS + suggest 头
        def _fake_invoke(addr, request, timeout_millis=None):
            self.requests.append((addr, request))
            response = RemotingCommand()
            response.code = 0  # SUCCESS
            response.ext_fields = {
                "nextBeginOffset": "1", "minOffset": "0", "maxOffset": "10",
                "suggestWhichBrokerId": str(self._suggest),
            }
            return response
        self.instance._invoke_sync = _fake_invoke


def _route():
    route = TopicRouteData()
    route.broker_datas.append(BrokerData("DefaultCluster", "broker-a", _addrs()))
    return route


def test_pull_message_selects_slave_by_broker_id_and_clears_commit_offset():
    inst = _Instance(suggest=0, route=_route())
    sys_flag = PullSysFlag.build_sys_flag(commit_offset=True, suspend=True,
                                          subscription=False, class_filter=False)
    assert PullSysFlag.has_commit_offset_flag(sys_flag)

    result = inst.instance.pull_message(GROUP, MQ, 0, 32, sys_flag, 0, None, 0, "TAG",
                                        broker_id=1)

    # 选中了 brokerId=1（从节点），且 COMMIT_OFFSET 位被清掉
    assert inst.requests[0][0] == SLAVE
    req_header = inst.requests[0][1].custom_header
    assert not PullSysFlag.has_commit_offset_flag(req_header.sys_flag)
    assert PullSysFlag.has_suspend_flag(req_header.sys_flag)
    # 应答的 suggestWhichBrokerId 透传给消费者（本轮回 0）
    assert result.suggest_which_broker_id == 0


def test_pull_message_master_id_keeps_commit_offset_flag():
    inst = _Instance(suggest=1, route=_route())
    sys_flag = PullSysFlag.build_sys_flag(commit_offset=True, suspend=True,
                                          subscription=False, class_filter=False)
    inst.instance.pull_message(GROUP, MQ, 0, 32, sys_flag, 0, None, 0, "TAG", broker_id=0)
    assert inst.requests[0][0] == MASTER
    assert PullSysFlag.has_commit_offset_flag(inst.requests[0][1].custom_header.sys_flag)


# ---------------- push 消费者：postSubscriptionWhenPull 门控 + suggest 回写 ----------------

class _PullCapture:
    """替身 client：只实现拉取循环用到的 pull_message。"""

    def __init__(self, suggest=1):
        self.calls = []
        self._suggest = suggest

    def pull_message(self, group, mq, offset, max_nums, sys_flag, commit_offset,
                     subscription, sub_version, expression_type, **kwargs):
        self.calls.append({"sys_flag": sys_flag, "subscription": subscription,
                           "broker_id": kwargs.get("broker_id")})
        return PullResult(PullStatus.NO_NEW_MSG, 0, 0, 0, [],
                          suggest_which_broker_id=self._suggest)


def _push_consumer():
    c = DefaultMQPushConsumer(GROUP)
    c._started = True
    sub = SubscriptionData(topic=TOPIC, sub_string="TagA")
    sub.tags_set = {"TagA"}
    c.subscription_data[TOPIC] = sub
    c._offset_table[c._mq_key(MQ)] = 0
    # 拉取线程的持有判定替成恒真（真实现看 _queue_threads 注册表）
    c._owns_queue = lambda key: True
    return c


def _run_one_pull(c, client):
    """把拉取循环跑一轮：第一轮 pull 返回后因队列线程不匹配而退出。"""
    c._mq_client = client
    t = threading.Thread(target=c._queue_pull_loop, args=(MQ,), daemon=True)
    t.start()
    t.join(5.0)
    assert not t.is_alive()


def test_push_subscription_bit_off_by_default_like_java():
    c = _push_consumer()
    assert c.post_subscription_when_pull is False  # Java 5.x 默认 false

    client = _PullCapture()
    _run_one_pull(c, client)

    call = client.calls[0]
    # 默认不发订阅表达式：subscription 位清零、请求头 subscription 为 None
    assert not PullSysFlag.has_subscription_flag(call["sys_flag"])
    assert call["subscription"] is None
    # 首轮按 master 拉
    assert call["broker_id"] == MixAll.MASTER_ID


def test_push_subscription_bit_on_when_knob_enabled():
    c = _push_consumer()
    c.set_post_subscription_when_pull(True)

    client = _PullCapture()
    _run_one_pull(c, client)

    call = client.calls[0]
    assert PullSysFlag.has_subscription_flag(call["sys_flag"])
    assert call["subscription"] == "TagA"


def test_push_updates_pull_from_which_node_from_suggest():
    c = _push_consumer()
    client = _PullCapture(suggest=1)
    _run_one_pull(c, client)

    # Java processPullResult:77：本轮应答的 suggest 写回表，下一轮按它选路
    assert c._pull_from_which_node[MQ] == 1
    assert client.calls[0]["broker_id"] == MixAll.MASTER_ID

    # 缺省 suggest（老 broker）按 master=0 记账（Java long 原语口径）
    c2 = _push_consumer()
    client2 = _PullCapture(suggest=None)
    _run_one_pull(c2, client2)
    assert c2._pull_from_which_node[MQ] == MixAll.MASTER_ID


def test_pull_result_suggest_defaults_to_none():
    r = PullResult(PullStatus.NO_NEW_MSG, 0, 0, 0, [])
    assert r.suggest_which_broker_id is None
