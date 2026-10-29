# -*- coding: utf-8 -*-
from __future__ import annotations

import time
from types import SimpleNamespace
from typing import Dict, List, Tuple

import pytest

from client.consumer import DefaultLitePullConsumer, MessageSelector
from client.consumer_result import PullResult, PullStatus
from client.exception import MQBrokerException, MQClientException
from client.mq_client import MQClientInstance
from common.message import MessageExt, MessageQueue
from common.subscription_data import ExpressionType
from common.sysflag import PullSysFlag
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.heartbeat import ConsumeFromWhere, ConsumeType, MessageModel
from remoting.protocol.remoting_command import RemotingCommand


def _make_msg(topic: str, broker: str, qid: int, offset: int, body: str) -> MessageExt:
    m = MessageExt(topic, body.encode())
    m.broker_name = broker
    m.queue_id = qid
    m.queue_offset = offset
    # Both setting the attribute and going through set_tags (which writes properties), so that it
    # behaves like a MessageExt really decoded from the wire:
    # the tag of a real message lives in properties["TAGS"], is read through get_tags(), and the
    # .tags attribute is None.
    m.tags = "TagA" if "A" in body else "TagB"
    m.set_tags(m.tags)
    return m


class MockClient:
    """An in-memory MQClientInstance: it holds the message lists per (broker,queue_id) and simulates pull."""

    def __init__(self, client_id: str, store: Dict[Tuple[str, int], List[str]]):
        self.client_id = client_id
        self.remoting_client = SimpleNamespace(register_rpc_hook=lambda *a, **k: None)
        self._store = store
        self.committed: Dict[Tuple[str, int], int] = {}
        # The commit stream: what the offset contract cares about is not "what does it end up equal
        # to" but "was anything sent, in what order, and for which queues", so every entry is
        # recorded.
        self.updates: List[Tuple[str, int, int]] = []
        self.queries: List[Tuple[str, int]] = []
        # start() must first register the topic as "in use" and fetch the route once, and only then
        # send the first heartbeat (the heartbeat targets come solely from the route table). The
        # order is recorded in an event stream so that it can be asserted rather than merely observed
        # to have happened.
        self.events: List[str] = []
        # #105: the scripted replies (pull_message pops the script first and only falls back to the
        # in-memory store once it is empty), plus the queue_offset of every pull request -- whether
        # "the cursor kept up" can only be proven by the requested offsets.
        self.scripted: List = []
        self.pull_requests: List[int] = []
        # #107: the sysFlag of every pull -- only the request itself can prove whether the lite bit (0x10) is set.
        self.pull_sys_flags: List[int] = []
        # The expression type + subString of every pull: the heartbeat alone is not enough -- the
        # broker only evaluates SQL92 when the **pull request** carries that type.
        self.pull_expression_types: List[str] = []
        self.pull_subscriptions: List[str] = []
        # The subVersion of every pull: a TAG subscription carries none, a broker-evaluated
        # expression carries the version its subscription data was registered with.
        self.pull_sub_versions: List[int] = []
        # The broker-suspend budget of every pull: the knob has to reach the wire, not just
        # pass the start-up validation.
        self.pull_suspend_timeouts: List[int] = []

    def start(self) -> None:
        pass

    def shutdown(self) -> None:
        pass

    def get_route_of_all_brokers(self):
        self.events.append("brokers")
        return []  # there is no real broker, so the heartbeat goes to an empty set (a no-op)

    def get_all_broker_addrs(self):
        # The heartbeat (including the consumer heartbeat) uses the "one machine per brokerId"
        # address table; this Mock has no real broker, so both return empty.
        self.events.append("brokers")
        return []

    def register_topic_in_use(self, topic):
        self.events.append("register:%s" % topic)

    def get_topic_publish_info(self, topic):
        self.events.append("route:%s" % topic)

        class _Info:
            msg_queue_list = [MessageQueue(topic, b, q) for (b, q) in self._store.keys()]
        return _Info()

    def get_topic_subscribe_info(self, topic):
        self.events.append("route:%s" % topic)
        return [MessageQueue(topic, b, q) for (b, q) in self._store.keys()]

    def get_consumer_id_list_by_group(self, topic, consumer_group, timeout_millis=5000):
        return [self.client_id]

    def get_max_offset(self, mq):
        return len(self._store.get((mq.broker_name, mq.queue_id), []))

    def get_min_offset(self, mq):
        return 0

    def query_consumer_offset(self, consumer_group, mq):
        # It does the bookkeeping just like a broker: whatever is in the offset table is returned,
        # and when there is nothing it is QUERY_NOT_FOUND (None). Only like this can the "look in
        # the in-memory table first" behaviour of committed() be tested -- if everything returned None,
        # the in-memory hit and the fallback-to-broker implementation would give the same answer.
        self.queries.append((mq.broker_name, mq.queue_id))
        return self.committed.get((mq.broker_name, mq.queue_id))

    def update_consumer_offset(self, consumer_group, mq, offset):
        self.committed[(mq.broker_name, mq.queue_id)] = offset
        self.updates.append((mq.broker_name, mq.queue_id, offset))

    def search_offset_by_timestamp(self, mq, timestamp):
        return 0

    def pull_message(self, consumer_group, mq, queue_offset, max_msg_nums, sys_flag,
                     commit_offset, subscription, sub_version, expression_type,
                     timeout_millis=30000, max_msg_bytes=-1, suspend_timeout_millis=15000,
                     addr=None, request_source=0) -> PullResult:
        self.pull_requests.append(queue_offset)
        self.pull_sys_flags.append(sys_flag)
        self.pull_expression_types.append(expression_type)
        self.pull_subscriptions.append(subscription)
        self.pull_sub_versions.append(sub_version)
        self.pull_suspend_timeouts.append(suspend_timeout_millis)
        if self.scripted:
            entry = self.scripted.pop(0)
            # The callable runs "in the middle of a request": it simulates a concurrent seek / queue withdrawal
            return entry() if callable(entry) else entry
        msgs = self._store.get((mq.broker_name, mq.queue_id), [])
        window = msgs[queue_offset:queue_offset + max_msg_nums]
        found = []
        for i, body in enumerate(window):
            found.append(_make_msg(mq.topic, mq.broker_name, mq.queue_id,
                                   queue_offset + i, body))
        if found:
            return PullResult(PullStatus.FOUND, queue_offset + len(found), 0,
                              len(msgs), found)
        return PullResult(PullStatus.NO_NEW_MSG, queue_offset, 0, len(msgs), [])


class _Lite(DefaultLitePullConsumer):
    """Replace the underlying client with an in-memory Mock so that tests need no cluster."""

    def __init__(self, *args, **kwargs):
        store = kwargs.pop("_store", {})
        super().__init__(*args, **kwargs)
        self.client_id = "DEFAULT@testclient"
        self.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
        self._mock = MockClient(self.client_id, store)

    def _create_client(self):
        return self._mock


STORE = {("broker-a", 0): ["A0", "B1", "A2", "B3", "A4"]}


class TestLifecycle:
    def test_empty_consumer_group_rejected(self):
        with pytest.raises(MQClientException):
            DefaultLitePullConsumer("")
        with pytest.raises(MQClientException):
            DefaultLitePullConsumer("   ")

    def test_start_without_namesrv_raises(self):
        c = DefaultLitePullConsumer("LitePG_UT")
        with pytest.raises(MQClientException):
            c.start()

    def test_start_without_subscription_succeeds_and_late_subscribe_wires_route_and_heartbeat(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        beats = []

        def _record_heartbeat():
            beats.append(c._build_heartbeat())
            return 0

        c._send_heartbeat_to_all_broker = _record_heartbeat
        c._start_heartbeat_loop = lambda: None
        c.start()
        try:
            assert c._started is True
            c._mock.events.clear()
            beats.clear()
            c.subscribe("T", "TagA")
            assert c.subscription["T"] == "TagA"
            assert c._mock.events == ["register:T", "route:T"]
            assert len(beats) == 1
            subs = list(beats[0].consumer_data_set)[0].subscription_data_set
            assert [s.topic for s in subs] == ["T"]
        finally:
            c.shutdown()

    def test_start_state_machine_messages(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.start()
        try:
            with pytest.raises(MQClientException) as running:
                c.start()
            assert str(running.value) == (
                "The PullConsumer service state not OK, maybe started once, RUNNING")
        finally:
            c.shutdown()
        with pytest.raises(MQClientException) as stopped:
            c.start()
        assert str(stopped.value) == (
            "The PullConsumer service state not OK, maybe started once, SHUTDOWN_ALREADY")

    def test_failed_start_is_latched(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.set_consume_timestamp("1700000000000")
        with pytest.raises(MQClientException):
            c.start()
        with pytest.raises(MQClientException) as failed:
            c.start()
        assert str(failed.value) == (
            "The PullConsumer service state not OK, maybe started once, START_FAILED")

    def test_check_config_rejects_invalid_model_and_timeout(self):
        bad_model = _Lite("LitePG_UT", _store=STORE)
        bad_model.set_namesrv_addr("127.0.0.1:9876")
        bad_model.message_model = "NOT_A_MODEL"
        with pytest.raises(MQClientException, match="messageModel is null"):
            bad_model.start()

        bad_timeout = _Lite("LitePG_UT", _store=STORE)
        bad_timeout.set_namesrv_addr("127.0.0.1:9876")
        bad_timeout.set_consumer_timeout_millis_when_suspend(1000)
        with pytest.raises(MQClientException, match="consumerTimeoutMillisWhenSuspend"):
            bad_timeout.start()

    def test_shutdown_before_start_is_noop(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.shutdown()  # must not throw

    def test_config_defaults(self):
        # Use the real class (not going through the _Lite fixture that overrides consume_from_where) to check
        # the default configuration
        c = DefaultLitePullConsumer("LitePG_UT")
        assert c.auto_commit is True
        # The lite default is the small one: the aggregate cache gate budgets a whole batch per pending
        # pull round, so the batch size is a multiplier of that budget, not just how much one pull takes.
        assert c.pull_batch_size == 10
        assert c.pull_threshold_for_all == 10000
        assert c.poll_timeout_millis == 5000
        assert c.consume_from_where == "CONSUME_FROM_LAST_OFFSET"
        assert len(c.consume_timestamp) == 14
        assert c.consume_timestamp.isdigit()

    def test_auto_commit_interval_refuses_a_setting_below_the_floor(self):
        # 1000ms is the shortest interval the knob accepts; a shorter one is ignored outright, so
        # the previously accepted value keeps running instead of a clamped floor.
        c = DefaultLitePullConsumer("LitePG_UT")
        assert c.auto_commit_interval_millis == 5000
        c.set_auto_commit_interval_millis(999)
        assert c.auto_commit_interval_millis == 5000
        c.set_auto_commit_interval_millis(-1)
        assert c.auto_commit_interval_millis == 5000
        c.set_auto_commit_interval_millis(1000)
        assert c.auto_commit_interval_millis == 1000
        c.set_auto_commit_interval_millis(2000)
        assert c.auto_commit_interval_millis == 2000
        c.set_auto_commit_interval_millis(500)
        assert c.auto_commit_interval_millis == 2000

    def test_consume_timestamp_must_be_wall_clock(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.assign([MessageQueue("T", "broker-a", 0)])
        # A plain numeric epoch in milliseconds must be rejected (the old implementation interpreted it as an
        # epoch and silently computed a misaligned start)
        c.set_consume_timestamp("1700000000000")
        with pytest.raises(MQClientException, match="consumeTimestamp is invalid"):
            c.start()
        # A legal value must not be killed by this startup guard
        fresh = _Lite("LitePG_UT", _store=STORE)
        fresh.set_namesrv_addr("127.0.0.1:9876")
        fresh.assign([MessageQueue("T", "broker-a", 0)])
        fresh.set_consume_timestamp("20230101000000")
        fresh.start()
        try:
            assert fresh._started is True
        finally:
            fresh.shutdown()

    def test_heartbeat_declares_consume_actively_per_the_wire_contract(self):
        c = _Lite("LitePG_UT", _store=STORE)
        cd = list(c._build_heartbeat().consumer_data_set)[0]
        assert cd.consume_type == ConsumeType.CONSUME_ACTIVELY

    def test_start_pulls_route_before_the_first_heartbeat(self):
        # The heartbeat only goes to "the brokers already known in the route table", so refreshing
        # the route has to be ordered before that synchronous heartbeat; otherwise the first heartbeat
        # has no target, the broker side does not see this instance, and a multi-instance rebalance
        # lets each of them exclusively own all the queues (which shows up as duplicate consumption
        # on a real cluster).
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.subscribe("T", "*")
        c.start()
        try:
            events = c._mock.events
            assert "register:T" in events and "route:T" in events, events
            assert events.index("route:T") < events.index("brokers"), events
        finally:
            c.shutdown()

        assigned = _Lite("LitePG_UT", _store=STORE)
        assigned.set_namesrv_addr("127.0.0.1:9876")
        assigned.assign([MessageQueue("T", "broker-a", 0)])
        assigned.start()
        try:
            # In assign mode the subscription is empty, so the route has to be derived from the assigned queues
            assert "register:T" in assigned._mock.events, assigned._mock.events
        finally:
            assigned.shutdown()


class TestBroadcastOffsetStore:
    def test_commit_and_read_stay_in_the_local_file(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        mq = MessageQueue("TBcast", "broker-a", 0)
        c = _Lite("LitePG_Bcast", _store=STORE,
                  message_model=MessageModel.BROADCASTING)
        c.set_auto_commit(False)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.assign([mq])
        c.start()
        try:
            c.commit({mq: 4})
            assert c._mock.updates == []
            assert c._load_local_offsets()["TBcastbroker-a0"] == 4
            c._offset_table.clear()
            assert c.committed(mq) == 4
            assert c._mock.queries == []
        finally:
            c.shutdown()
        assert c._mock.updates == []


class TestAssignMode:
    def test_assign_then_poll_drains_all(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.set_pull_batch_size(2)
        c.assign([MessageQueue("T", "broker-a", 0)])
        c.start()
        try:
            collected = []
            # 5 messages with a batch of 2 -> 2+2+1, three polls
            for _ in range(3):
                collected.extend(c.poll(timeout=2000))
            assert [m.body.decode() for m in collected] == ["A0", "B1", "A2", "B3", "A4"]
            # The buffer is already empty, so the next poll times out and returns an empty list (it does not block forever)
            assert c.poll(timeout=200) == []
        finally:
            c.shutdown()

    def test_assign_mode_sets_assignment(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        mq = MessageQueue("T", "broker-a", 0)
        c.assign([mq])
        assert c.assignment() == [mq]

    def test_assign_before_start_leaves_the_start_point_to_start(self):
        pass
        c = _Lite("LitePG_UT", _store=STORE)
        mq = MessageQueue("T", "broker-a", 0)
        c.assign([mq])
        assert mq not in c._next_offset
        assert c._mock.queries == []                     # not started, not a single RPC is sent
        with pytest.raises(MQClientException, match="not running"):
            c._resolve_initial_offset(mq)
        # The resolution is filled in inside start(); the start point is the offset already committed on the
        # broker (not 0)
        c._mock.committed[(mq.broker_name, mq.queue_id)] = 3
        c.set_namesrv_addr("127.0.0.1:9876")
        c.start()
        try:
            assert ("broker-a", 0) in c._mock.queries      # it really asked the broker
            # The first one handed out is B3: the start point is 3. If the start point were pinned to 0
            # (the FIRST_OFFSET literal without asking the broker) this would be A0; if it fell back to
            # the max offset (5) there would be nothing at all.
            got = c.poll(timeout=2000)
            assert [m.body.decode() for m in got] == ["B3", "A4"]
        finally:
            c.shutdown()

    def test_seek_resets_next_offset_and_drops_buffered(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.set_pull_batch_size(2)
        c.assign([MessageQueue("T", "broker-a", 0)])
        c.start()
        try:
            first = c.poll(timeout=2000)
            assert [m.body.decode() for m in first] == ["A0", "B1"]
            # Go back to the head of the queue and consume again
            c.seek(MessageQueue("T", "broker-a", 0), 0)
            again = c.poll(timeout=2000)
            assert [m.body.decode() for m in again][:2] == ["A0", "B1"]
        finally:
            c.shutdown()

    def test_commit_records_offset(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.set_pull_batch_size(2)
        c.set_auto_commit(False)  # turn auto-commit off to verify the explicit commit
        c.assign([MessageQueue("T", "broker-a", 0)])
        c.start()
        try:
            c.poll(timeout=2000)  # pulled A0,B1 -> next_offset=2
            c.commit()
            assert c._mock.committed[("broker-a", 0)] == 2
        finally:
            c.shutdown()


class TestSubscribeMode:
    def test_single_consumer_gets_all_queues(self):
        # subscribe mode: in a single-instance group it is the only one -> the rebalance assigns all the queues to it
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.set_pull_batch_size(2)
        c.subscribe("T", "*")
        c.start()
        try:
            collected = []
            for _ in range(3):
                collected.extend(c.poll(timeout=2000))
            assert [m.body.decode() for m in collected] == ["A0", "B1", "A2", "B3", "A4"]
            # subscribe mode rebalances automatically, so the assignment is non-empty
            assert len(c.assignment()) == 1
        finally:
            c.shutdown()

    def test_subscribe_tag_filter(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.set_pull_batch_size(5)
        c.subscribe("T", "TagA")  # only consume the TagA messages
        c.start()
        try:
            collected = []
            for _ in range(3):
                collected.extend(c.poll(timeout=2000))
            # A0 A2 A4 match TagA
            assert [m.body.decode() for m in collected] == ["A0", "A2", "A4"]
        finally:
            c.shutdown()


class TestPauseResume:
    def test_pause_stops_pulling(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.set_pull_batch_size(2)
        mq = MessageQueue("T", "broker-a", 0)
        c.assign([mq])
        c.pause([mq])
        c.start()
        try:
            # While paused the background does not pull and poll times out returning nothing
            assert c.poll(timeout=300) == []
            c.resume([mq])
            got = c.poll(timeout=2000)
            assert [m.body.decode() for m in got][:2] == ["A0", "B1"]
        finally:
            c.shutdown()


class TestCommitOffsetTable:
    pass

    def _two_queue_store(self):
        return {("broker-a", 0): ["A0", "B1", "A2"], ("broker-a", 1): ["C0", "D1"]}

    def _consumer(self, store=None):
        c = _Lite("LitePG_UT", _store=store or self._two_queue_store())
        c.set_namesrv_addr("127.0.0.1:9876")
        c.set_auto_commit(False)          # the commit timing is controlled by the test case
        return c

    def test_commit_map_persists_only_the_given_queues(self):
        c = self._consumer()
        q0 = MessageQueue("T", "broker-a", 0)
        q1 = MessageQueue("T", "broker-a", 1)
        c.assign([q0, q1])
        c.start()
        try:
            c.commit({q0: 2, q1: 7}, persist=False)
            # persist=False: both in-memory places have it, the broker received not a single one
            assert c._offset_table == {q0: 2, q1: 7}
            assert c._mock.updates == []
            c.commit({q0: 9}, persist=True)
            assert c._mock.updates == [("broker-a", 0, 9)]
            assert list(c._offset_table) == [q0]
        finally:
            c.shutdown()

    def test_committed_reads_memory_first_then_broker(self):
        c = self._consumer()
        q0 = MessageQueue("T", "broker-a", 0)
        c.assign([q0])
        c.start()
        try:
            c._mock.committed[("broker-a", 0)] = 4
            assert c.committed(q0) == 4
            queries = len(c._mock.queries)
            assert c.committed(q0) == 4
            assert len(c._mock.queries) == queries, "the broker must not be asked again after the backfill"
            # A commit with persist=False has not reached the broker yet, and committed() must still see it
            c.commit({q0: 6}, persist=False)
            assert c.committed(q0) == 6
            assert c._mock.committed[("broker-a", 0)] == 4
        finally:
            c.shutdown()

    def test_commit_map_does_not_move_the_pull_cursor(self):
        c = self._consumer()
        c.set_pull_batch_size(2)
        q0 = MessageQueue("T", "broker-a", 0)
        c.assign([q0])
        c.start()
        try:
            first = c.poll(timeout=2000)
            assert [m.body.decode() for m in first] == ["A0", "B1"]
            assert c._next_offset[q0] == 2
            c.commit({q0: 0})
            # The committed offset is "the ledger others read", not "where I pull from next time": the cursor
    # does not move and the buffer stays as it is
            assert c._next_offset[q0] == 2
            again = c.poll(timeout=2000)
            assert [m.body.decode() for m in again] == ["A2"]
        finally:
            c.shutdown()

    def test_commit_uses_the_polled_cursor_not_the_pull_cursor(self):
        # While messages that have not been handed out are still sitting in the buffer, the commit can only stop
        # at the cell that poll handed out
        c = self._consumer({("broker-a", 0): ["A0", "B1", "A2", "B3", "A4"]})
        c.set_pull_batch_size(5)
        q0 = MessageQueue("T", "broker-a", 0)
        c.assign([q0])
        c.start()
        try:
            got = c.poll(timeout=2000)
            assert [m.body.decode() for m in got] == ["A0", "B1", "A2", "B3", "A4"]
            assert c._next_offset[q0] == 5
            assert c._consume_offset[q0] == 5
            c.commit()
            assert c._mock.updates == [("broker-a", 0, 5)]
        finally:
            c.shutdown()

    def test_auto_commit_never_commits_what_poll_never_handed_out(self):
        c = self._consumer({("broker-a", 0): ["A0", "B1", "A2", "B3", "A4"]})
        c.set_pull_batch_size(2)
        c.set_auto_commit(True)
        c.set_auto_commit_interval_millis(1000)
        q0 = MessageQueue("T", "broker-a", 0)
        c.assign([q0])
        c.start()
        try:
            # First confirm that the background really pulled (otherwise updates == [] is just a false negative of
            # "it did not get around to it yet")
            for _ in range(200):
                if c._next_offset.get(q0, -1) >= 2:
                    break
                time.sleep(0.01)
            assert c._next_offset.get(q0, -1) >= 2, "the background never pulled, so this test case loses its meaning"
            assert c._mock.updates == [], "nothing was handed out => there is no offset to commit"

            handed = 0
            for _ in range(5):
                got = c.poll(timeout=2000)
                if got:
                    handed = max(handed, max(m.queue_offset + 1 for m in got))
                for _, _, off in c._mock.updates:
                    assert off <= handed, \
                        "it committed messages that poll has not handed out yet: %d > %d" % (off, handed)
            assert handed == 5, handed
            # Manually move the deadline into the past to force that gate once: the landing point is the cell
            # that has already been handed out
            c._next_auto_commit_deadline = -1
            c._maybe_auto_commit()
            assert c._mock.updates[-1] == ("broker-a", 0, 5)
            assert c._next_auto_commit_deadline > 0, "after committing the deadline must be pushed to the next period"
        finally:
            c.shutdown()

    def test_commit_set_form_only_touches_given_queues(self):
        c = self._consumer()
        c.set_pull_batch_size(2)
        q0 = MessageQueue("T", "broker-a", 0)
        q1 = MessageQueue("T", "broker-a", 1)
        c.assign([q0, q1])
        c.start()
        try:
            drained = []
            for _ in range(4):
                drained.extend(c.poll(timeout=1000))
            assert len(drained) == 5, drained
            c.commit([q1], persist=True)
            assert [(b, q, o) for b, q, o in c._mock.updates] == [("broker-a", 1, 2)]
        finally:
            c.shutdown()

    def test_commit_skips_minus_one_and_unassigned_queues(self):
        c = self._consumer()
        q0 = MessageQueue("T", "broker-a", 0)
        stranger = MessageQueue("T", "broker-a", 7)
        c.assign([q0])
        c.start()
        try:
            c.commit({q0: -1, stranger: 3})
            assert c._offset_table == {}
            assert c._mock.updates == []
        finally:
            c.shutdown()

    def test_empty_map_and_empty_set_are_ignored(self):
        c = self._consumer()
        q0 = MessageQueue("T", "broker-a", 0)
        c.assign([q0])
        c.start()
        try:
            c.commit({q0: 2}, persist=False)
            c.commit({})
            c.commit([])
            # An empty set returns right away: no message is sent and the table clearing of persistAll is not triggered
            assert c._offset_table == {q0: 2}
            assert c._mock.updates == []
        finally:
            c.shutdown()

    def test_seek_moves_the_consume_cursor_too(self):
        c = self._consumer()
        c.set_pull_batch_size(2)
        q0 = MessageQueue("T", "broker-a", 0)
        c.assign([q0])
        c.start()
        try:
            c.poll(timeout=2000)
            assert c._consume_offset[q0] == 2
            c.seek(q0, 0)
            assert c._consume_offset[q0] == 0
            c.commit()
            assert c._mock.updates == [("broker-a", 0, 0)]
        finally:
            c.shutdown()

    def test_revoked_queue_is_persisted_then_dropped(self):
        store = {("broker-a", 0): ["A0", "B1", "A2"], ("broker-a", 1): ["C0", "D1"]}
        c = _Lite("LitePG_UT", _store=store)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.set_auto_commit(False)
        c.subscribe("T", "*")
        c.start()
        try:
            q0 = MessageQueue("T", "broker-a", 0)
            q1 = MessageQueue("T", "broker-a", 1)
            assert {q1} <= set(c.assignment()), c.assignment()
            c.commit({q1: 1})
            assert c._mock.updates == [("broker-a", 1, 1)]
            c._mock.updates.clear()
            # The broker side withdraws q1 (locally simulating a rebalance with one queue fewer)
            del store[("broker-a", 1)]
            c._rebalance()
            assert c._mock.updates == [("broker-a", 1, 1)], \
                "before withdrawing a queue the last commit must be sent out"
            assert q1 not in c._offset_table and q1 not in c._consume_offset
            assert q1 not in c._next_offset and q1 not in set(c.assignment())
            # The remaining queue must not be cleared as a side effect
            assert q0 in c._next_offset or q0 in c._offset_table
        finally:
            c.shutdown()

    def test_assign_mode_revoke_does_not_persist(self):
        c = self._consumer()
        q0 = MessageQueue("T", "broker-a", 0)
        q1 = MessageQueue("T", "broker-a", 1)
        c.assign([q0, q1])
        c.start()
        try:
            c.commit({q1: 1}, persist=False)
            assert c._offset_table[q1] == 1
            c._mock.updates.clear()
            c.assign([q0])
            assert c._mock.updates == [], "assign mode does not send commits on behalf of the caller"
            # Both cursors disappear together with the state; the entry in offsetTable is left for persistAll to clear
            assert q1 not in c._next_offset and q1 not in c._consume_offset
            assert c._offset_table[q1] == 1
        finally:
            c.shutdown()

    def test_three_cursors_stay_three_separate_numbers(self):
        # The offline version of the live S8: one queue is filled with 1200 messages and the per-poll
        # limit is 1024 => the three numbers "how much was pulled", "how much was handed out" and
        # "where it was committed to" must all differ.
        # Using the pull cursor as the commit source (the bug fixed in #68) turns red here: one commit
        # pushes all 1200 to the broker and the caller dies before those 176 => messages are silently
        # lost.
        c = self._consumer({("broker-a", 0): ["A%d" % i for i in range(1200)]})
        c.set_pull_batch_size(1200)
        q0 = MessageQueue("T", "broker-a", 0)
        stranger = MessageQueue("T", "broker-a", 9)
        # Three observation points: as long as the queue is not in the table it is always -1, not 0 (0 is the
        # valid offset for "no messages before the head of the queue")
        assert (c.pull_cursor_of(q0), c.consume_cursor_of(q0), c.pending_commit_of(q0)) \
            == (-1, -1, -1)
        c.assign([q0])
        c.start()
        try:
            # The background pull thread races this test case for the very same pull, so it only waits for the
            # result and does not assert who pulled it
            pull_deadline = time.time() + 10
            while c.pull_cursor_of(q0) < 1200 and time.time() < pull_deadline:
                c._pull_one(q0)
            assert c.pull_cursor_of(q0) == 1200, c.pull_cursor_of(q0)
            assert c.consume_cursor_of(q0) == -1, "nothing was handed out"
            c.commit()
            assert c._mock.updates == [], "nothing was ever handed out so there is no offset to commit"

            first = c.poll(timeout=2000)
            assert len(first) == 1024, len(first)
            assert c.consume_cursor_of(q0) == 1024
            assert c.pull_cursor_of(q0) == 1200, "the last 176 messages are still sitting in the buffer"
            c.commit()
            assert c._mock.updates == [("broker-a", 0, 1024)], \
                "what is committed is the consumed cursor, not the pull cursor"
            # persist=False only goes into the in-memory table: committed() sees it while the broker side is still 1024
            c.commit({q0: 777}, persist=False)
            assert c.pending_commit_of(q0) == 777
            assert c.committed(q0) == 777
            assert c._mock.committed[("broker-a", 0)] == 1024
            updates_before = len(c._mock.updates)
            c.commit([stranger], persist=True)
            assert len(c._mock.updates) == updates_before, "a queue that was not named must not be sent out"
            assert c.pending_commit_of(q0) == -1
            assert c.committed(q0) == 1024
        finally:
            c.shutdown()


class TestPullCursorFollowsNextBeginOffset:
    pass

    def _paused_consumer(self, store=None, batch=2):
        """After start() the queue is paused: the background loop does not touch it and the cursor is driven
        only by the test case itself."""
        c = _Lite("LitePG_UT", _store=store if store is not None else STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.set_pull_batch_size(batch)
        c.set_auto_commit(False)
        q0 = MessageQueue("T", "broker-a", 0)
        c.assign([q0])
        c.start()
        c.pause([q0])
        return c, q0

    def test_no_matched_msg_jumps_the_cursor_past_the_scanned_window(self):
        # A real broker replies NO_MATCHED_MSG (PULL_RETRY_IMMEDIATELY) when "nothing in the
        # whole range matches the tag", and its nextBeginOffset has already moved past every message
        # scanned in this round. The old implementation only advanced the cursor on FOUND, so on
        # this path it rescanned the same stretch round after round (O(n) per round over a long range,
        # and never able to advance).
        c, q0 = self._paused_consumer()
        try:
            c._mock.scripted.append(PullResult(PullStatus.NO_MATCHED_MSG, 5, 0, 5, []))
            assert c._pull_one(q0) is False, "nothing was handed out"
            assert c.pull_cursor_of(q0) == 5, "the cursor must follow nextBeginOffset and skip the whole stretch"
            assert c._pull_one(q0) is False
            assert c._mock.pull_requests[-1] == 5, \
                "the next round pulls from 5 and no longer rescans 0..4: %s" % c._mock.pull_requests
        finally:
            c.shutdown()

    def test_offset_illegal_adopts_the_brokers_correction(self):
        # When the offset is out of range (the offset table holds a value larger than
        # maxOffset or smaller than minOffset) the broker replies OFFSET_ILLEGAL and its
        # nextBeginOffset is exactly the corrected offset it gives (the three OFFSET_* branches at
        # PullMessageProcessor composeResponseHeader:631 + :664-676). Following the upstream cursor
        # is therefore self-healing; the old implementation did not move the cursor on this path and
        # the next round hit the same out-of-range value again.
        c, q0 = self._paused_consumer()
        try:
            c._mock.scripted.append(PullResult(PullStatus.OFFSET_ILLEGAL, 5, 0, 5, []))
            assert c._pull_one(q0) is False
            assert c.pull_cursor_of(q0) == 5, "it self-heals to the offset corrected by the broker"
            assert c._pull_one(q0) is False
            assert c._mock.pull_requests[-1] == 5
        finally:
            c.shutdown()

    def test_in_flight_seek_wins_over_the_pull_result(self):
        c, q0 = self._paused_consumer()
        try:
            def seek_mid_flight():
                # Offset 5 is this queue's max: Seek holds the offset to the queue's own
                # [min, max] range, and this test is about the in-flight window, not a range refusal.
                c.seek(q0, 5)
                return PullResult(PullStatus.FOUND, 2, 0, 5, [
                    _make_msg("T", "broker-a", 0, 0, "A0"),
                    _make_msg("T", "broker-a", 0, 1, "B1"),
                ])

            c._mock.scripted.append(seek_mid_flight)
            assert c._pull_one(q0) is False, "the seek invalidated the window, nothing may be handed out"
            assert c.pull_cursor_of(q0) == 5, "the cursor stays on the value the seek pinned"
            assert c.poll(timeout=100) == [], "the buffer must not hold messages from before the seek window"
        finally:
            c.shutdown()


class TestLitePullWireContract:
    pass

    def test_lite_pull_sets_the_lite_flag_and_only_that(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.set_auto_commit(False)
        q0 = MessageQueue("T", "broker-a", 0)
        c.assign([q0])
        c.start()
        try:
            c._pull_one(q0)
            flag = c._mock.pull_sys_flags[-1]
            assert PullSysFlag.has_lite_pull_flag(flag) is True, \
                "lite pull must set FLAG_LITE_PULL_MESSAGE(0x10): flag=%#x" % flag
            assert PullSysFlag.has_commit_offset_flag(flag) is False
            assert PullSysFlag.has_suspend_flag(flag) is False
            assert PullSysFlag.has_subscription_flag(flag) is True
            assert PullSysFlag.has_class_filter_flag(flag) is False
        finally:
            c.shutdown()

    def test_classic_pull_flag_has_no_lite_bit(self):
        # The reverse control: the classic pull (DefaultMQPullConsumerImpl.pullSyncImpl:248)
        # uses the 4-argument buildSysFlag, so the lite bit must be 0 -- otherwise ordinary consumers
        # would be hit by the 361 switch as well.
        classic = PullSysFlag.build_sys_flag(commit_offset=False, suspend=False,
                                             subscription=True, class_filter=False)
        assert PullSysFlag.has_lite_pull_flag(classic) is False

    def _wire_pull(self, sys_flag, response=None):
        """Drive MQClient.pull_message directly (passing addr saves the route lookup) and capture the outgoing requests."""
        client = MQClientInstance("DEFAULT@wiretest", ["127.0.0.1:9876"])
        captured = {}

        def fake_invoke(addr, request, timeout_millis=None):
            captured["code"] = request.code
            captured["sys_flag"] = request.custom_header.sys_flag
            if response is not None:
                return response
            resp = RemotingCommand.create_response_command(ResponseCode.SUCCESS)
            resp.ext_fields = {"nextBeginOffset": "1", "minOffset": "0", "maxOffset": "5"}
            return resp

        client._invoke_sync = fake_invoke
        mq = MessageQueue("T", "broker-a", 0)
        client.pull_message("LitePG_UT", mq, 0, 32, sys_flag, 0, "*", 0, "TAG",
                            addr="127.0.0.1:10911")
        return captured

    def test_client_api_switches_to_361_on_the_lite_bit(self):
        lite = PullSysFlag.build_sys_flag(commit_offset=False, suspend=False,
                                          subscription=True, class_filter=False,
                                          lite_pull=True)
        captured = self._wire_pull(lite)
        assert captured["code"] == RequestCode.LITE_PULL_MESSAGE == 361
        # The bit really has to reach the wire (header.sysFlag); the broker side has yet another check in
        # hasLitePullFlag
        assert PullSysFlag.has_lite_pull_flag(captured["sys_flag"]) is True

    def test_client_api_keeps_11_without_the_lite_bit(self):
        classic = PullSysFlag.build_sys_flag(commit_offset=False, suspend=False,
                                             subscription=True, class_filter=False)
        captured = self._wire_pull(classic)
        assert captured["code"] == RequestCode.PULL_MESSAGE == 11
        assert PullSysFlag.has_lite_pull_flag(captured["sys_flag"]) is False

    def test_lite_361_no_permission_surfaces_as_broker_exception(self):
        # When the switch is off the broker replies NO_PERMISSION with a fixed remark
        # (PullMessageProcessor:325-331).
        # The live discriminator relies on that remark to assert that it really was "the lite switch
        # that blocked it"; do not swallow it into a generic error.
        lite = PullSysFlag.build_sys_flag(commit_offset=False, suspend=False,
                                          subscription=True, class_filter=False,
                                          lite_pull=True)
        denial = RemotingCommand.create_response_command(
            ResponseCode.NO_PERMISSION,
            "the broker[127.0.0.1:10911] for lite pull consumer is forbidden")
        with pytest.raises(MQBrokerException) as ei:
            self._wire_pull(lite, response=denial)
        assert "for lite pull consumer is forbidden" in str(ei.value)


class TestLitePullSelectorExpressionType:
    """subscribe_with_selector must carry the expression type end-to-end: subscription data ->
    heartbeat -> pull request. A selector silently degraded to a tag string matches nothing on
    the broker and then gets dropped by the client-side tag pass as well."""

    def _sql_consumer(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.set_auto_commit(False)
        c.subscribe_with_selector("T", MessageSelector.by_sql("a > 1"))
        # Wire the in-memory client directly (no start -> no background pull racing the script).
        # The started flag is set by hand: poll() refuses a consumer that never started.
        c._mq_client = c._mock
        c._started = True
        return c

    def test_sql92_selector_builds_selector_typed_subscription_data(self):
        c = self._sql_consumer()
        try:
            sd = c.subscription_data["T"]
            assert sd.expression_type == ExpressionType.SQL92
            assert sd.sub_string == "a > 1"
            # The SQL text must NOT be parsed as a tag list
            assert sd.tags_set == set() and sd.code_set == set()
            cd = list(c._build_heartbeat().consumer_data_set)[0]
            hb_sub = list(cd.subscription_data_set)[0]
            assert hb_sub.expression_type == ExpressionType.SQL92
            assert hb_sub.sub_string == "a > 1"
        finally:
            c.shutdown()

    def test_sql92_selector_type_reaches_the_pull_request_and_skips_tag_filter(self):
        c = self._sql_consumer()
        try:
            q0 = MessageQueue("T", "broker-a", 0)
            c._next_offset[q0] = 0
            # "B1" carries TagB: a client-side tag pass reading "a > 1" as a tag would drop it.
            c._mock.scripted.append(PullResult(PullStatus.FOUND, 1, 0, 5,
                                               [_make_msg("T", "broker-a", 0, 0, "B1")]))
            assert c._pull_one(q0) is True
            assert c._mock.pull_expression_types[-1] == ExpressionType.SQL92
            assert c._mock.pull_subscriptions[-1] == "a > 1"
            # A broker-evaluated expression is compiled once per version, so the pull repeats
            # the version the subscription data was registered with -- not 0.
            assert c._mock.pull_sub_versions[-1] == c.subscription_data["T"].sub_version
            assert c._mock.pull_sub_versions[-1] != 0
            got = c.poll(timeout=500)
            assert [m.body.decode() for m in got] == ["B1"], \
                "SQL92 matches are filtered by the broker; the client must deliver them"
        finally:
            c.shutdown()

    def test_tag_selector_keeps_tag_semantics(self):
        # The control: a tag selector behaves exactly like subscribe(topic, tag) -- TAG on the
        # wire, tagsSet filled for the heartbeat, and the client-side tag pass still applies.
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.set_auto_commit(False)
        c.subscribe_with_selector("T", MessageSelector.by_tag("TagA"))
        c._mq_client = c._mock
        try:
            sd = c.subscription_data["T"]
            assert sd.expression_type == ExpressionType.TAG
            assert sd.tags_set == {"TagA"}
            q0 = MessageQueue("T", "broker-a", 0)
            c._next_offset[q0] = 0
            c._mock.scripted.append(PullResult(PullStatus.FOUND, 1, 0, 5,
                                               [_make_msg("T", "broker-a", 0, 0, "B1")]))
            assert c._pull_one(q0) is False, "TagB must still be dropped by the client tag pass"
            assert c._mock.pull_expression_types[-1] == ExpressionType.TAG
            assert c._mock.pull_sub_versions[-1] == 0, \
                "a TAG subscription carries no version on the wire: the tag string is the whole filter"
        finally:
            c.shutdown()

    def test_plain_subscribe_and_unsubscribe_keep_the_type_table_consistent(self):
        # The control for the default path: subscribe() is TAG, and unsubscribe removes the
        # type entry too (a stale SQL92 type under a re-subscribed tag would leak to the wire).
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.subscribe("T", "TagA")
        assert c.subscription_expr_type["T"] == ExpressionType.TAG
        c.unsubscribe("T")
        assert "T" not in c.subscription_expr_type
        c.subscribe_with_selector("T", MessageSelector.by_sql("a > 1"))
        assert c.subscription_expr_type["T"] == ExpressionType.SQL92
        c.unsubscribe("T")
        assert "T" not in c.subscription_expr_type
        c.shutdown()




def test_lite_surface_refuses_misuse():
    """The lite surface answers misuse with an error, not with a plausible-looking result.

    An empty ``assign()`` would revoke every held queue, a blank assign expression would quietly
    mean "everything", and a poll or a committed-offset read on a consumer that never started looks
    exactly like a queue at its tail -- which is how a missing ``start()`` stays hidden forever.
    """
    c = DefaultLitePullConsumer("G_lite_guards")
    mq = MessageQueue("T", "broker-a", 0)

    with pytest.raises(ValueError, match="can not be null or empty"):
        c.assign([])
    with pytest.raises(ValueError, match="can not be null or empty"):
        c.assign(None)
    assert c.assignment() == []                 # a refused assign changed nothing
    assert c.is_assign_mode() is False

    with pytest.raises(ValueError, match="Topic can not be null or empty"):
        c.subscribe("   ")

    with pytest.raises(ValueError, match="subExpression can not be null or empty"):
        c.set_sub_expression_for_assign("T", "  ")

    with pytest.raises(ValueError, match="Topic can not be null or empty"):
        c.set_sub_expression_for_assign("   ", "TagA")
    assert c.subscription == {}                  # a refused assign expression registers nothing

    with pytest.raises(MQClientException, match="not running"):
        c.poll()
    with pytest.raises(MQClientException, match="not running"):
        c.poll(timeout=-1)                      # the state check comes first
    with pytest.raises(MQClientException, match="not running"):
        c.committed(mq)


def test_lite_mode_is_mutually_exclusive():
    """A lite consumer is subscribed or assigning, never both: the two modes build different
    subscription tables, pull targets and heartbeats, so a mid-flight switch would leave the
    running halves describing two different consumers."""
    mq = MessageQueue("T", "broker-a", 0)

    subscribed = DefaultLitePullConsumer("G_lite_mode_sub")
    subscribed.subscribe("T", "*")
    with pytest.raises(MQClientException, match="Subscribe and assign are mutually exclusive"):
        subscribed.assign([mq])
    with pytest.raises(MQClientException, match="Subscribe and assign are mutually exclusive"):
        subscribed.set_sub_expression_for_assign("T", "TagA")
    assert subscribed.assignment() == [] and subscribed.is_assign_mode() is False

    assigned = DefaultLitePullConsumer("G_lite_mode_assign")
    assigned.assign([mq])
    with pytest.raises(MQClientException, match="Subscribe and assign are mutually exclusive"):
        assigned.subscribe("T", "*")
    assert assigned.subscription == {} and assigned.is_assign_mode() is True
    # Same mode stays open: an assign-mode consumer may take more topics or queues.
    assigned.set_sub_expression_for_assign("T", "TagA")
    assigned.assign([mq, MessageQueue("T", "broker-a", 1)])


def test_seek_refuses_what_it_cannot_honour():
    """Seek only moves a cursor the consumer can honour: a queue it does not hold is
    refused before anything is written, and the offset is held to the queue's own
    [minOffset, maxOffset] range."""
    c = _Lite("LitePG_UT", _store=STORE)
    c.set_namesrv_addr("127.0.0.1:9876")
    c.set_auto_commit(False)
    q0 = MessageQueue("T", "broker-a", 0)
    stranger = MessageQueue("T", "broker-a", 7)
    c.assign([q0])
    # Before start the queue is held, so it is the running-state check that refuses.
    with pytest.raises(MQClientException, match="not running"):
        c.seek(q0, 1)
    c.start()
    try:
        c.pause([q0])
        # Assign mode: a queue that was never assigned never will be.
        with pytest.raises(MQClientException, match="not in assigned list, message queue") as assign_err:
            c.seek(stranger, 0)
        assert "may be rebalancing" not in str(assign_err.value)

        # The store holds 5 messages: offsets 0..5 are legal, both outside points are refused.
        for legal in (0, 5):
            c.seek(q0, legal)
        for illegal in (-1, 6):
            with pytest.raises(MQClientException, match="Seek offset illegal") as range_err:
                c.seek(q0, illegal)
            assert "min offset = 0, max offset = 5" in str(range_err.value)
        assert c.pull_cursor_of(q0) == 5, "a refused Seek must not have moved the cursor"
    finally:
        c.shutdown()

    # Subscribe mode names the same situation differently: there the queue may merely
    # be waiting for the rebalance that would hand it over.
    s = _Lite("LitePG_UT", _store=STORE)
    s.set_namesrv_addr("127.0.0.1:9876")
    s.subscribe("T", "*")
    s.start()
    try:
        with pytest.raises(MQClientException, match="may be rebalancing"):
            s.seek(stranger, 0)
    finally:
        s.shutdown()


def test_assign_revoke_drops_buffered_rows_park_and_pause():
    """A revoked queue leaves nothing behind. Poll must never hand out messages from a
    queue the consumer has given up, and its parking window and paused flag go with the
    assignment they described."""
    store = {("broker-a", 0): ["A0", "B1"], ("broker-a", 1): ["C0"]}
    c = _Lite("LitePG_UT", _store=store)
    c.set_namesrv_addr("127.0.0.1:9876")
    c.set_auto_commit(False)
    q0 = MessageQueue("T", "broker-a", 0)
    q1 = MessageQueue("T", "broker-a", 1)
    c.assign([q0, q1])
    c.pause([q0, q1])
    c.start()
    try:
        assert c._pull_one(q0) is True, "the staged batch should reach the buffer"
        assert len(c._local_buffer) == 2
        c._pull_not_before[q0] = time.monotonic() + 60

        c.assign([q1])          # shrink: q0 is revoked
        assert list(c._local_buffer) == [], "rows from a revoked queue must not stay pollable"
        assert q0 not in c._paused, "the revoked queue's paused flag goes with it"
        assert q0 not in c._pull_not_before, "the revoked queue's parking time goes with it"
        assert q0 not in c._queue_cache, "the revoked queue's accumulator goes with it"
        assert c.poll(timeout=100) == []
    finally:
        c.shutdown()


def test_assign_mode_heartbeat_announces_no_subscription():
    """An assign expression filters the queues this instance was handed; it is not a
    broker-side subscription. Announcing it would make the broker treat the group as a
    subscriber of the whole topic, so the heartbeat carries no subscription entry at all
    while the local registration still drives the pull request."""
    c = DefaultLitePullConsumer("G_lite_hb_assign")
    c.set_sub_expression_for_assign("T", "TagA")
    c.assign([MessageQueue("T", "broker-a", 0)])
    cd = list(c._build_heartbeat().consumer_data_set)[0]
    assert list(cd.subscription_data_set) == [], \
        "an assign-mode heartbeat advertises no subscription"
    assert c.subscription_data["T"].sub_string == "TagA", \
        "the local registration is untouched: it is what the pull request repeats"

    # The control: subscribe mode still announces its subscription.
    s = DefaultLitePullConsumer("G_lite_hb_sub")
    s.subscribe("T", "TagA")
    subs = list(list(s._build_heartbeat().consumer_data_set)[0].subscription_data_set)
    assert [sub.topic for sub in subs] == ["T"]
    assert subs[0].sub_string == "TagA"


def test_unsubscribe_revokes_the_topics_queues_and_state():
    """Unsubscribe does not wait for the next rebalance: the topic's queues leave at once,
    with their cursors, seek pin, paused flag and buffered rows, and the next rebalance
    cannot hand them back."""
    c = _Lite("LitePG_UT", _store=STORE)
    c.set_namesrv_addr("127.0.0.1:9876")
    c.set_auto_commit(False)
    c.subscribe("T", "*")
    c.start()
    try:
        deadline = time.monotonic() + 5
        while not c.assignment() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert c.assignment(), "the rebalance assigned the topic's queue"
        mq = c.assignment()[0]
        c.pause([mq])
        c.seek(mq, 3)
        assert c.pull_cursor_of(mq) == 3

        c.unsubscribe("T")

        assert c.assignment() == [], "the topic's queues leave with the subscription"
        assert c.pull_cursor_of(mq) == -1, "the pull cursor went with the revoked queue"
        assert c.consume_cursor_of(mq) == -1, "and the consume cursor too"
        assert mq not in c._paused, "and so did the paused flag"
        assert mq not in c._queue_cache, "and its cache accumulator"
        assert c.subscription == {}

        # The subscription table is gone, so the running rebalance (about once a second)
        # cannot hand the topic back.
        time.sleep(1.5)
        assert c.assignment() == [], "the rebalance did not re-add the unsubscribed topic"
    finally:
        c.shutdown()

    # The assign expression table is not a rebalance subscription: unsubscribe revokes
    # the topic's queues but leaves the caller's assign filter table alone.
    g = DefaultLitePullConsumer("G_lite_unsub_assign")
    g.set_sub_expression_for_assign("T", "TagA")
    g.assign([MessageQueue("T", "broker-a", 0)])
    g.unsubscribe("T")
    assert g.assignment() == []
    assert g._assign_sub_expr == {"T": "TagA"}
    # A blank topic is a no-op, not a namespace-wrapped key.
    g.unsubscribe("   ")
    assert g._assign_sub_expr == {"T": "TagA"}


def test_lite_pull_request_carries_the_configured_suspend_budget():
    """The broker-suspend budget is a live knob: the LITE_PULL request has to carry the
    configured value. A fixed wire constant would pass the start-up validation and then
    ignore the setting for the life of the consumer."""
    c = _Lite("LiteSuspend_UT", _store=STORE)
    c.set_namesrv_addr("127.0.0.1:9876")
    c.set_auto_commit(False)
    c.set_broker_suspend_max_time_millis(8000)
    c.set_consumer_timeout_millis_when_suspend(20000)
    c.assign([MessageQueue("T", "broker-a", 0)])
    c.start()
    try:
        deadline = time.monotonic() + 5
        while not c._mock.pull_requests and time.monotonic() < deadline:
            time.sleep(0.02)
        assert c._mock.pull_requests, "no lite pull went out"
        assert c._mock.pull_suspend_timeouts[0] == 8000, \
            "the pull carried %d, want the configured 8000" % c._mock.pull_suspend_timeouts[0]
        assert not PullSysFlag.has_suspend_flag(c._mock.pull_sys_flags[0]), \
            "the suspend bit stays off: a synchronous lite loop asks for a short poll"
    finally:
        c.shutdown()


def test_heartbeat_carries_the_built_subscription_set():
    """An assign-mode consumer has no topic-wide subscription, so the built set is how it
    announces the filters its queues need; every build replaces the whole set, and a
    refused build leaves the previous one in place."""
    c = DefaultLitePullConsumer("G_lite_hb_built")
    c.assign([MessageQueue("A", "broker-a", 0), MessageQueue("B", "broker-a", 0)])
    c.build_subscriptions_for_heartbeat({
        "A": MessageSelector.by_tag("TagA"),
        "B": MessageSelector.by_sql("a > 1"),
    })
    cd = list(c._build_heartbeat().consumer_data_set)[0]
    subs = {sub.topic: sub for sub in cd.subscription_data_set}
    assert subs["A"].sub_string == "TagA"
    assert subs["A"].tags_set == {"TagA"}, "a TAG selector still fills the tag set"
    assert subs["B"].sub_string == "a > 1"
    assert subs["B"].expression_type == ExpressionType.SQL92
    assert [s.topic for s in c.get_subscriptions_for_heartbeat()] == ["A", "B"]

    # A rebuild replaces the whole set: B is not announced any more.
    c.build_subscriptions_for_heartbeat({"A": MessageSelector.by_tag("TagB")})
    cd = list(c._build_heartbeat().consumer_data_set)[0]
    assert [sub.topic for sub in cd.subscription_data_set] == ["A"]
    assert list(cd.subscription_data_set)[0].sub_string == "TagB"

    # A refused build must not damage the set already in place.
    with pytest.raises(ValueError):
        c.build_subscriptions_for_heartbeat({"": MessageSelector.by_tag("TagA")})
    with pytest.raises(ValueError):
        c.build_subscriptions_for_heartbeat({"A": MessageSelector(ExpressionType.SQL92, "")})
    assert [s.topic for s in c.get_subscriptions_for_heartbeat()] == ["A"]


def test_built_set_yields_to_the_rebalance_subscription():
    """A topic the rebalance subscriptions cover keeps their expression: the built set
    only fills the topics they do not."""
    c = DefaultLitePullConsumer("G_lite_hb_both")
    c.subscribe("T", "TagA")
    c.build_subscriptions_for_heartbeat({"T": MessageSelector.by_tag("TagZ")})
    cd = list(c._build_heartbeat().consumer_data_set)[0]
    subs = list(cd.subscription_data_set)
    assert len(subs) == 1, "one entry per topic, the rebalance one"
    assert subs[0].sub_string == "TagA"
    assert len(c.get_subscriptions_for_heartbeat()) == 1, \
        "the built set keeps what the application added for its own diagnostics"
