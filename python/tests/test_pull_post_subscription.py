# -*- coding: utf-8 -*-
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



def _addrs():
    return {0: MASTER, 1: SLAVE, 2: SLAVE2}


def test_find_broker_addr_in_subscribe_hits_exact_id():
    assert MQClientInstance.find_broker_addr_in_subscribe(_addrs(), 0) == (MASTER, False)
    assert MQClientInstance.find_broker_addr_in_subscribe(_addrs(), 1) == (SLAVE, True)
    assert MQClientInstance.find_broker_addr_in_subscribe(_addrs(), 2) == (SLAVE2, True)


def test_find_broker_addr_in_subscribe_slave_miss_tries_next_id():
    addrs = {0: MASTER, 2: SLAVE2}
    assert MQClientInstance.find_broker_addr_in_subscribe(addrs, 1) == (SLAVE2, True)


def test_find_broker_addr_in_subscribe_falls_back_to_any_node():
    # when brokerId is not restricted (onlyThisBroker=false), fall back to the smallest id (=master)
    assert MQClientInstance.find_broker_addr_in_subscribe({1: SLAVE}, 3) == (SLAVE, True)
    assert MQClientInstance.find_broker_addr_in_subscribe(_addrs(), 9) == (MASTER, False)


def test_find_broker_addr_in_subscribe_empty_table():
    assert MQClientInstance.find_broker_addr_in_subscribe({}, 0) == (None, False)


# ---------------- pull_message's brokerId routing and slave COMMIT_OFFSET clearing ----------------

class _Instance:
    """A minimally usable MQClientInstance: preset route + records _invoke_sync's address and request."""

    def __init__(self, suggest, route):
        self.instance = MQClientInstance("ut@client", ["127.0.0.1:9876"],
                                         connect_timeout_millis=3000,
                                         invoke_timeout_millis=15000,
                                         tls_enable=False)
        self.instance.topic_route_table[TOPIC] = route
        self.requests = []
        self._suggest = suggest
        # replace the transport layer: record (addr, request), reply SUCCESS + suggest header
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

    # brokerId=1 (slave) is selected, and the COMMIT_OFFSET bit is cleared
    assert inst.requests[0][0] == SLAVE
    req_header = inst.requests[0][1].custom_header
    assert not PullSysFlag.has_commit_offset_flag(req_header.sys_flag)
    assert PullSysFlag.has_suspend_flag(req_header.sys_flag)
    # the reply's suggestWhichBrokerId is passed through to the consumer (this round 0)
    assert result.suggest_which_broker_id == 0


def test_pull_message_master_id_keeps_commit_offset_flag():
    inst = _Instance(suggest=1, route=_route())
    sys_flag = PullSysFlag.build_sys_flag(commit_offset=True, suspend=True,
                                          subscription=False, class_filter=False)
    inst.instance.pull_message(GROUP, MQ, 0, 32, sys_flag, 0, None, 0, "TAG", broker_id=0)
    assert inst.requests[0][0] == MASTER
    assert PullSysFlag.has_commit_offset_flag(inst.requests[0][1].custom_header.sys_flag)


# ---------------- push consumer: postSubscriptionWhenPull gating + suggest write-back ----------------

class _PullCapture:
    """A stand-in client: implements only the pull_message used by the pull loop."""

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
    # replace the pull thread's ownership check with a constant true (the real implementation consults the _queue_threads registry)
    c._owns_queue = lambda key: True
    return c


def _run_one_pull(c, client):
    """Run the pull loop once: after the first pull returns it exits because the queue thread does not match."""
    c._mq_client = client
    t = threading.Thread(target=c._queue_pull_loop, args=(MQ,), daemon=True)
    t.start()
    t.join(5.0)
    assert not t.is_alive()


def test_push_subscription_bit_off_by_default():
    c = _push_consumer()
    assert c.post_subscription_when_pull is False

    client = _PullCapture()
    _run_one_pull(c, client)

    call = client.calls[0]
    # by default no subscription expression is sent: the subscription bit is cleared and the request header's subscription is None
    assert not PullSysFlag.has_subscription_flag(call["sys_flag"])
    assert call["subscription"] is None
    # the first round pulls from master
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

    assert c._pull_from_which_node[MQ] == 1
    assert client.calls[0]["broker_id"] == MixAll.MASTER_ID

    c2 = _push_consumer()
    client2 = _PullCapture(suggest=None)
    _run_one_pull(c2, client2)
    assert c2._pull_from_which_node[MQ] == MixAll.MASTER_ID


def test_pull_result_suggest_defaults_to_none():
    r = PullResult(PullStatus.NO_NEW_MSG, 0, 0, 0, [])
    assert r.suggest_which_broker_id is None
