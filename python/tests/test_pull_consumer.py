# -*- coding: utf-8 -*-
from __future__ import annotations

import inspect
import threading
import time
from collections import deque
from types import SimpleNamespace

import pytest

from client.consumer import (DefaultMQPullConsumer, DefaultMQPushConsumer,
                             PullCallback)
from client.consumer_result import PullResult, PullStatus
from client.consume_executor import ConsumeExecutor
from client.exception import MQBrokerException, MQClientException
from common.message import MessageExt, MessageQueue
from common.sysflag import PullSysFlag
from remoting.exception import RemotingTooMuchRequestException
from remoting.protocol.heartbeat import (ConsumeFromWhere, ConsumeType,
                                                  MessageModel)


class TestLifecycle:
    def test_empty_consumer_group_rejected(self):
        with pytest.raises(MQClientException):
            DefaultMQPullConsumer("")
        with pytest.raises(MQClientException):
            DefaultMQPullConsumer("   ")

    def test_start_without_namesrv_raises(self):
        c = DefaultMQPullConsumer("PG_UnitTest")
        with pytest.raises(MQClientException):
            c.start()

    def test_operations_before_start_raise(self):
        c = DefaultMQPullConsumer("PG_UnitTest")
        c.set_namesrv_addr("127.0.0.1:9876")
        mq = MessageQueue("TopicUnitTest", "broker-a", 0)
        # without start() any operation needing the client must throw, not NPE / AttributeError
        with pytest.raises(MQClientException):
            c.fetch_subscribe_message_queues("TopicUnitTest")
        with pytest.raises(MQClientException):
            c.pull(mq, "*", 0, 32, 1000)
        with pytest.raises(MQClientException):
            c.update_consume_offset(mq, 0)

    def test_shutdown_before_start_is_noop(self):
        c = DefaultMQPullConsumer("PG_UnitTest")
        c.shutdown()  # must not throw

    def test_set_namesrv_addr_splits_semicolon(self):
        c = DefaultMQPullConsumer("PG_UnitTest")
        c.set_namesrv_addr(" 127.0.0.1:9876 ; 127.0.0.1:9877 ;; ")
        assert c.name_server_addrs == ["127.0.0.1:9876", "127.0.0.1:9877"]

    def test_register_message_queue_listener_records_topic(self):
        c = DefaultMQPullConsumer("PG_UnitTest")

        class _L:
            def message_queue_changed(self, topic, mq_all, mq_divided):
                pass

        listener = _L()
        c.set_message_queue_listener(listener)
        assert c.message_queue_listener is listener


class TestBroadcastOffsetStore:
    class _Client:
        def __init__(self):
            self.updates = []
            self.queries = []

        def update_consumer_offset(self, group, mq, offset):
            self.updates.append((mq, offset))

        def query_consumer_offset(self, group, mq):
            self.queries.append(mq)
            return None

    def test_broadcasting_offsets_use_the_local_file_not_the_broker(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        mq = MessageQueue("TBcast", "broker-a", 0)
        c = DefaultMQPullConsumer("PG_Broadcast", message_model=MessageModel.BROADCASTING)
        c.client_id = "cid@broadcast"
        fake = self._Client()
        c._mq_client = fake
        c._started = True

        c.update_consume_offset(mq, 17)
        assert c.fetch_consume_offset(mq) == 17
        assert fake.updates == [] and fake.queries == []

        push = DefaultMQPushConsumer("PG_Broadcast")
        push.client_id = c.client_id
        assert c._local_offset_path() == push._local_offset_path()

    def test_clustering_offsets_go_to_the_broker(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        mq = MessageQueue("TClust", "broker-a", 0)
        c = DefaultMQPullConsumer("PG_Cluster")
        c.client_id = "cid@cluster"
        fake = self._Client()
        c._mq_client = fake
        c._started = True

        c.update_consume_offset(mq, 5)
        c.fetch_consume_offset(mq)
        assert fake.updates == [(mq, 5)]
        assert fake.queries == [mq]


class TestPullSysFlagParity:
    pass

    def test_pull_is_short_poll_without_commit_offset(self):
        src = inspect.getsource(DefaultMQPullConsumer.pull)
        assert "suspend=False" in src, "pull() must be a short poll (suspend=False)"
        assert "commit_offset=False" in src, "pull() does not carry the commitOffset bit itself"

    def test_pull_block_if_not_found_is_long_poll(self):
        src = inspect.getsource(DefaultMQPullConsumer.pull_block_if_not_found)
        assert "suspend=True" in src, "pull_block_if_not_found() is the long poll"

    def test_flag_bits(self):
        short = PullSysFlag.build_sys_flag(commit_offset=False, suspend=False,
                                           subscription=True, class_filter=False)
        assert PullSysFlag.has_commit_offset_flag(short) is False
        assert PullSysFlag.has_suspend_flag(short) is False

        long_ = PullSysFlag.build_sys_flag(commit_offset=False, suspend=True,
                                           subscription=True, class_filter=False)
        assert PullSysFlag.has_commit_offset_flag(long_) is False
        assert PullSysFlag.has_suspend_flag(long_) is True

    def test_message_model_defaults_to_clustering(self):
        c = DefaultMQPullConsumer("PG_UnitTest")
        assert c.message_model == MessageModel.CLUSTERING

    def test_namespace_field_present(self):
        c = DefaultMQPullConsumer("PG_UnitTest")
        assert c.namespace == ""
        c.namespace = "NS1"
        assert c.namespace == "NS1"


class _RecordingClient:
    """A stand-in client overriding only the methods needed by the heartbeat path (recording every send/receive)."""

    def __init__(self, addrs=("127.0.0.1:10911", "127.0.0.1:10912")):
        self._addrs = list(addrs)
        self.heartbeats = []          # [(addr, HeartbeatData)]
        self.topics_in_use = []
        self.routed = []
        self.unregistered = []        # [(client_id, producer_group, consumer_group)]
        self.shutdown_called = 0
        self.start_called = 0
        self.remoting_client = SimpleNamespace(register_rpc_hook=lambda *a, **k: None)

    def start(self):
        self.start_called += 1

    def get_all_broker_addrs(self):
        return list(self._addrs)

    def send_heartbeat(self, addr, hb, timeout_millis):
        self.heartbeats.append((addr, hb))

    def register_topic_in_use(self, topic):
        self.topics_in_use.append(topic)

    def get_topic_publish_info(self, topic):
        self.routed.append(topic)
        return SimpleNamespace(msg_queue_list=[])

    def unregister_client_all_brokers(self, client_id, producer_group, consumer_group,
                                      timeout_millis=3000):
        self.unregistered.append((client_id, producer_group, consumer_group))

    def shutdown(self):
        self.shutdown_called += 1


def _started_pull(topics=("TopicHbA", "TopicHbB")):
    """Build a "started" pull-mode consumer whose client is a stand-in and whose heartbeat reports can be intercepted."""
    c = DefaultMQPullConsumer("PG_HbUnit")
    c.client_id = "cid@fixed"
    c.register_topics.update(topics)
    fake = _RecordingClient()
    c._mq_client = fake
    c._started = True
    return c, fake


class TestConsumerHeartbeat:
    pass

    def test_heartbeat_is_actively_typed_and_carries_register_topics(self):
        c, fake = _started_pull()
        assert c._send_heartbeat_to_all_broker() == 2, "one each to master + slave"
        assert [a for a, _ in fake.heartbeats] == ["127.0.0.1:10911", "127.0.0.1:10912"]
        hb = fake.heartbeats[0][1]
        assert hb.client_id == "cid@fixed"
        cd = list(hb.consumer_data_set)[0]
        assert cd.group_name == "PG_HbUnit"
        assert cd.consume_type == ConsumeType.CONSUME_ACTIVELY
        assert cd.consume_from_where == ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
        subs = {s.topic: s for s in cd.subscription_data_set}
        assert sorted(subs) == ["TopicHbA", "TopicHbB"]
        for sub in subs.values():
            assert sub.sub_string == "*"
            assert sub.sub_version == 0
        assert c.heartbeat_count() == 1

    def test_heartbeat_still_announces_the_group_without_register_topics(self):
        # when registerTopics is empty the subscription set is empty, but the ConsumerData itself is still sent -- otherwise
        # when a caller "pulls only one or two topics without pre-registering", the broker side would never see this group.
        c, fake = _started_pull(topics=())
        assert c._send_heartbeat_to_all_broker() == 2
        cd = list(fake.heartbeats[0][1].consumer_data_set)[0]
        assert cd.group_name == "PG_HbUnit"
        assert list(cd.subscription_data_set) == []

    def test_start_refreshes_routes_then_sends_the_first_heartbeat(self, monkeypatch):
        import client.consumer as consumer_mod

        holder = {}

        def _factory(*args, **kwargs):
            holder["client"] = _RecordingClient()
            return holder["client"]

        monkeypatch.setattr(consumer_mod, "MQClientInstance", _factory)
        c = DefaultMQPullConsumer("PG_HbStart")
        c.set_namesrv_addr("127.0.0.1:9876")
        c.register_topics.add("TopicHbStart")
        c.heartbeat_interval_millis = 3600_000  # only test the startup round, do not let the loop interfere
        c.start()
        try:
            fake = holder["client"]
            assert fake.topics_in_use == ["TopicHbStart"]
            assert fake.routed == ["TopicHbStart"]
            assert len(fake.heartbeats) == 2
            assert c.heartbeat_count() == 1
        finally:
            c.shutdown()

    def test_shutdown_stops_the_loop_and_unregisters_the_group(self, monkeypatch):
        import client.consumer as consumer_mod

        holder = {}

        def _factory(*args, **kwargs):
            holder["client"] = _RecordingClient()
            return holder["client"]

        monkeypatch.setattr(consumer_mod, "MQClientInstance", _factory)
        c = DefaultMQPullConsumer("PG_HbStop")
        c.set_namesrv_addr("127.0.0.1:9876")
        c.register_topics.add("TopicHbStop")
        c.heartbeat_interval_millis = 3600_000
        c.start()
        c.shutdown()
        fake = holder["client"]
        assert fake.unregistered == [(c.client_id, "", "PG_HbStop")]
        assert fake.shutdown_called == 1
        assert c._heartbeat_stop.is_set()
        # idempotent: one more shutdown sends no more reports
        c.shutdown()
        assert fake.shutdown_called == 1


class _BalanceClient:
    """A stand-in client: provides only the two query mouths fetchMessageQueuesInBalance needs.

    subscribe / cidList can both be set to None to simulate "route unavailable" / "broker did not answer",
    at which point the method must fall back to local bookkeeping (keeping the current assignment) and must never monopolize all queues.
    """

    def __init__(self, subscribe=None, cid_list=None, raise_on_route=False):
        self._subscribe = subscribe or []
        self._cid_list = cid_list
        self.raise_on_route = raise_on_route
        self.remoting_client = SimpleNamespace(register_rpc_hook=lambda *a, **k: None)

    def get_topic_subscribe_info(self, topic):
        if self.raise_on_route:
            raise MQClientException("route failed")
        return [q for q in self._subscribe if q.topic == topic]

    def get_consumer_id_list_by_group(self, topic, consumer_group, timeout_millis=5000):
        return None if self._cid_list is None else list(self._cid_list)


def _balance_consumer(mq_all, cid_all, model=MessageModel.CLUSTERING, pulled=()):
    c = DefaultMQPullConsumer("PG_BalUnit")
    c.client_id = "cid-A"
    c.message_model = model
    c._mq_client = _BalanceClient(subscribe=mq_all, cid_list=cid_all)
    c._started = True
    for mq in pulled:
        c._pull_from_which_node[mq] = 0
    return c


class TestFetchMessageQueuesInBalance:
    pass

    _TOPIC = "TopicBalUnit"

    def _queues(self, n):
        return [MessageQueue(self._TOPIC, "broker-a", i) for i in range(n)]

    def test_not_started_raises(self):
        c = DefaultMQPullConsumer("PG_BalUnit")
        with pytest.raises(MQClientException):
            c.fetch_message_queues_in_balance(self._TOPIC)

    def test_null_topic_raises_illegal_argument(self):
        c = _balance_consumer(self._queues(1), ["cid-A"])
        with pytest.raises(ValueError):
            c.fetch_message_queues_in_balance(None)

    def test_returns_only_this_instance_share(self):
        got = _balance_consumer(self._queues(4), ["cid-A", "cid-B"]) \
            .fetch_message_queues_in_balance(self._TOPIC)
        assert [q.queue_id for q in got] == [0, 1], "under the averagely strategy A only takes the first half"

    def test_single_instance_takes_all(self):
        got = _balance_consumer(self._queues(3), ["cid-A"]) \
            .fetch_message_queues_in_balance(self._TOPIC)
        assert [q.queue_id for q in got] == [0, 1, 2]

    def test_result_is_sorted_and_deterministic(self):
        mq_all = [MessageQueue(self._TOPIC, "broker-b", 0),
                  MessageQueue(self._TOPIC, "broker-a", 1),
                  MessageQueue(self._TOPIC, "broker-a", 0)]
        got = _balance_consumer(mq_all, ["cid-A"]).fetch_message_queues_in_balance(self._TOPIC)
        assert [(q.broker_name, q.queue_id) for q in got] == [
            ("broker-a", 0), ("broker-a", 1), ("broker-b", 0)]

    def test_other_topics_are_not_leaked(self):
        other = [MessageQueue("TopicOther", "broker-a", i) for i in range(2)]
        got = _balance_consumer(other + self._queues(2), ["cid-A"]) \
            .fetch_message_queues_in_balance(self._TOPIC)
        assert all(q.topic == self._TOPIC for q in got)
        assert [q.queue_id for q in got] == [0, 1]

    def test_broadcasting_takes_every_queue(self):
        got = _balance_consumer(self._queues(3), ["cid-A", "cid-B"],
                                model=MessageModel.BROADCASTING) \
            .fetch_message_queues_in_balance(self._TOPIC)
        assert [q.queue_id for q in got] == [0, 1, 2]

    def test_no_consumer_list_keeps_current_assignment(self):
        # the broker did not answer the consumer list -> keep the queues this instance is pulling, do not monopolize all (the same group would double-consume).
        pulled = [MessageQueue(self._TOPIC, "broker-a", 2)]
        got = _balance_consumer(self._queues(4), None, pulled=pulled) \
            .fetch_message_queues_in_balance(self._TOPIC)
        assert [q.queue_id for q in got] == [2]

    def test_route_failure_falls_back_to_pulled_queues(self):
        c = _balance_consumer([], None, pulled=[MessageQueue(self._TOPIC, "broker-a", 5)])
        c._mq_client.raise_on_route = True
        got = c.fetch_message_queues_in_balance(self._TOPIC)
        assert [q.queue_id for q in got] == [5]

    def test_fresh_consumer_without_broker_returns_empty(self):
        assert _balance_consumer([], None).fetch_message_queues_in_balance(self._TOPIC) == []


_WAIT_TIMEOUT = 30.0


class _ScriptedPullClient:
    """A stand-in client: pull_message replays per the script (values / exceptions), recording thread names and inputs."""

    def __init__(self):
        self.remoting_client = SimpleNamespace(register_rpc_hook=lambda *a, **k: None)
        self.script = deque()
        self.calls = []               # [(mq, offset, timeout_millis, worker_thread)]

    def pull_message(self, consumer_group, mq, queue_offset, max_msg_nums, sys_flag,
                     commit_offset, subscription, sub_version, expression_type,
                     timeout_millis=30000, max_msg_bytes=-1,
                     suspend_timeout_millis=15000, addr=None, request_source=0,
                     broker_id=None):
        self.calls.append((mq, queue_offset, timeout_millis,
                           threading.current_thread().name))
        if self.script:
            item = self.script.popleft()
            if isinstance(item, BaseException):
                raise item
            return item
        return PullResult(PullStatus.FOUND, queue_offset + 1, 0, queue_offset + 1, [])

    def start(self):
        pass

    def shutdown(self):
        pass


def _started_async_pull(client=None):
    c = DefaultMQPullConsumer("PG_AsyncUnit")
    c.client_id = "cid@fixed"
    c._mq_client = client or _ScriptedPullClient()
    c._started = True
    return c


class _CollectingCallback(PullCallback):
    def __init__(self):
        self.success = []
        self.errors = []
        self.thread_names = []
        self.done = threading.Event()

    def on_success(self, pull_result):
        self.thread_names.append(threading.current_thread().name)
        self.success.append(pull_result)
        self.done.set()

    def on_exception(self, e):
        self.thread_names.append(threading.current_thread().name)
        self.errors.append(e)
        self.done.set()


MQ = MessageQueue("TopicAsyncUnit", "broker-a", 0)


class TestPullAsync:
    def test_before_start_raises_synchronously(self):
        c = DefaultMQPullConsumer("PG_AsyncUnit")
        cb = _CollectingCallback()
        # consistent with the intentional difference from the local send_async: not started -> throw synchronously, no callback.
        with pytest.raises(MQClientException):
            c.pull_async(MQ, "*", 0, 32, cb, 1000)
        assert cb.done.is_set() is False

    def test_missing_callback_raises(self):
        c = _started_async_pull()
        try:
            with pytest.raises(MQClientException):
                c.pull_async(MQ)
        finally:
            c.shutdown()

    def test_success_delivers_on_callback_thread(self):
        m = MessageExt("TopicAsyncUnit", b"hi")
        expected = PullResult(PullStatus.FOUND, 9, 0, 9, [m])
        client = _ScriptedPullClient()
        client.script.append(expected)
        c = _started_async_pull(client)
        cb = _CollectingCallback()
        try:
            begun = time.monotonic()
            c.pull_async(MQ, "TagA", 4, 16, cb, 3000)
            # returns immediately: the caller's thread is not blocked by the RPC.
            assert time.monotonic() - begun < 1.0
            assert cb.done.wait(_WAIT_TIMEOUT)
            assert cb.errors == []
            assert cb.success[0] is expected
            assert cb.success[0].msg_found_list == [m]
            # RPC runs on AsyncPullExecutor_, callbacks on PullCallbackExecutor_ (same split as the producer's two pools).
            assert client.calls[0][3].startswith("AsyncPullExecutor_"), client.calls
            assert cb.thread_names[0].startswith("PullCallbackExecutor_"), cb.thread_names
            # inputs passed through + budget not inflated (timeout-cost <= 3000)
            assert client.calls[0][1] == 4
            assert client.calls[0][2] <= 3000
        finally:
            c.shutdown()

    def test_rpc_failure_goes_to_on_exception(self):
        client = _ScriptedPullClient()
        client.script.append(MQBrokerException(11, "pull rejected"))
        c = _started_async_pull(client)
        cb = _CollectingCallback()
        try:
            c.pull_async(MQ, "*", 0, 32, cb, 3000)
            assert cb.done.wait(_WAIT_TIMEOUT)
            assert cb.success == []
            assert isinstance(cb.errors[0], MQBrokerException)
        finally:
            c.shutdown()

    def test_offset_illegal_status_is_a_success_callback(self):
        # all four PullStatus values mean "RPC succeeded"; OFFSET_ILLEGAL is not an exception (correction is left to the caller).
        expected = PullResult(PullStatus.OFFSET_ILLEGAL, 42, 40, 90, [])
        client = _ScriptedPullClient()
        client.script.append(expected)
        c = _started_async_pull(client)
        cb = _CollectingCallback()
        try:
            c.pull_async(MQ, "*", 7, 32, cb, 3000)
            assert cb.done.wait(_WAIT_TIMEOUT)
            assert cb.errors == []
            assert cb.success[0] is expected
        finally:
            c.shutdown()

    def test_queue_wait_timeout_skips_rpc(self):
        """Queueing eats the budget -> no request is sent, going straight to on_exception(RemotingTooMuchRequestException).

        Windows loopback ports have a ~2s connection-refused delay and thread scheduling jitters, so this does not really wait for a network timeout:
        a **single-threaded** pull pool is preset and a placeholder task is stuffed in first to block the worker, then pull_async with timeout=50ms is submitted; after waiting 0.2s the placeholder task is released -- at dequeue the cost is necessarily > the budget (it can only be larger, not
        smaller), taking the queue-timeout branch with **the client receiving no RPC at all**, fully deterministic throughout.
        """
        client = _ScriptedPullClient()
        c = _started_async_pull(client)
        pool = ConsumeExecutor(1, 1, thread_name_prefix="AsyncPullExecutor",
                               thread_name_sep="_", thread_index_from=1)
        c._async_pull_executor = pool
        blocker_in = threading.Event()
        hold = threading.Event()

        def _blocker():
            blocker_in.set()
            assert hold.wait(_WAIT_TIMEOUT)

        pool.submit(_blocker)
        assert blocker_in.wait(_WAIT_TIMEOUT)
        cb = _CollectingCallback()
        try:
            c.pull_async(MQ, "*", 1, 32, cb, 50)
            time.sleep(0.2)  # the budget is eaten up in the queue
            hold.set()
            assert cb.done.wait(_WAIT_TIMEOUT)
            assert cb.success == []
            assert isinstance(cb.errors[0], RemotingTooMuchRequestException), cb.errors
            assert client.calls == [], "a queue timeout must not send any request"
        finally:
            hold.set()
            c.shutdown()

    def test_rejected_when_pool_closed(self):
        c = _started_async_pull()
        pool = ConsumeExecutor(1, 1, thread_name_prefix="AsyncPullExecutor",
                               thread_name_sep="_", thread_index_from=1)
        c._async_pull_executor = pool
        cb = _CollectingCallback()
        pool.shutdown()
        try:
            # queue full / pool closed -> throw synchronously (same criterion as send_async's "executor rejected").
            with pytest.raises(MQClientException):
                c.pull_async(MQ, "*", 0, 32, cb, 3000)
            assert cb.done.is_set() is False
        finally:
            c.shutdown()

    def test_callback_exception_does_not_break_later_rounds(self):
        class _Boom(PullCallback):
            def __init__(self):
                self.done = threading.Event()

            def on_success(self, pull_result):
                self.done.set()
                raise RuntimeError("user callback bug")

            def on_exception(self, e):
                self.done.set()

        c = _started_async_pull()
        try:
            boom = _Boom()
            c.pull_async(MQ, "*", 0, 32, boom, 3000)
            assert boom.done.wait(_WAIT_TIMEOUT)
            # a user exception in the callback thread is only logged, the pool stays alive: the next one is delivered as usual.
            third = _CollectingCallback()
            c.pull_async(MQ, "*", 0, 32, third, 3000)
            assert third.done.wait(_WAIT_TIMEOUT)
            assert third.success and third.errors == []
        finally:
            c.shutdown()

    def test_shutdown_closes_pools_and_blocks_new_requests(self):
        client = _ScriptedPullClient()
        c = _started_async_pull(client)
        cb = _CollectingCallback()
        c.pull_async(MQ, "*", 0, 32, cb, 3000)
        assert cb.done.wait(_WAIT_TIMEOUT)
        assert c._async_pull_executor is not None
        c.shutdown()
        assert c._async_pull_executor is None and c._pull_callback_executor is None
        # already shutdown: _require_client intercepts first (consumer not started), throwing synchronously.
        with pytest.raises(MQClientException):
            c.pull_async(MQ, "*", 0, 32, _CollectingCallback(), 3000)
