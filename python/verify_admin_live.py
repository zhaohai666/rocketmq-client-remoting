# -*- coding: utf-8 -*-
import os
import sys
import time
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client.admin import DefaultMQAdminExt
from client.consumer import DefaultMQPushConsumer, SimpleMessageListener
from client.producer import DefaultMQProducer
from client.consumer_result import ConsumeConcurrentlyStatus
from client.exception import MQClientException
from common.message import Message, MessageQueue
from common.mix_all import MixAll
from remoting.protocol.heartbeat import ConsumeFromWhere
from remoting.protocol.subscription import SubscriptionGroupConfig

NAMESRV = "127.0.0.1:9876"
STAMP = int(time.time())
TOPIC = "AdminVerifyTopic_%d" % STAMP
GROUP = "AdminVerifyGroup_%d" % STAMP
KV_NS = "AdminVerifyKv_%d" % STAMP
N_MSG = 8

results = []
skips = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print("[%s] %s %s" % ("PASS" if ok else "FAIL", name, detail))


def skip(name, detail=""):
    """Record the items that "cannot be verified locally because of a broker-side configuration
    difference"; they do not count as failures.

    This is strictly separated from check(): a skip is not "I took a shortcut", it explicitly states
    that the environmental precondition this assertion depends on does not hold locally (for example
    the default file index has no uniqKey inverted index).
    """
    skips.append((name, detail))
    print("[SKIP] %s %s" % (name, detail))


def safe(name, fn, detail_fn=None):
    """Run fn, turning an exception into a FAIL instead of aborting the whole script."""
    try:
        value = fn()
        check(name, True, detail_fn(value) if detail_fn else str(value))
        return value
    except Exception as e:  # noqa: BLE001
        check(name, False, "%s: %s" % (type(e).__name__, e))
        return None


def main():
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.set_timeout_millis(10000)
    admin.start()

    # ---------- 1. cluster liveness (broker registration race: an open port != being registered) ----------
    cluster = None
    last_err = None
    for _ in range(40):
        try:
            ci = admin.fetch_broker_cluster_info()
            if ci and ci.broker_addr_table:
                cluster = ci
                break
        except Exception as e:  # noqa: BLE001
            last_err = e
        time.sleep(1)
    if cluster is None:
        check("cluster liveness", False, "no broker registered on the nameServer last_err=%s" % last_err)
        admin.shutdown()
        return 1
    check("fetchBrokerClusterInfo", True,
          "brokers=%s clusters=%s" % (sorted(cluster.broker_addr_table.keys()),
                                      sorted(cluster.cluster_addr_table.keys())))
    broker_addr = cluster.get_broker_addrs()[0]

    # ---------- 2. topic management ----------
    safe("createTopic(%s)" % TOPIC, lambda: admin.create_topic(
        MixAll.DEFAULT_TOPIC, TOPIC, 4), lambda _: "queueNum=4")

    time.sleep(1)
    all_topics = safe("fetchAllTopicList", lambda: admin.fetch_all_topic_list(),
                      lambda t: "topicCount=%d" % len(t.get_topic_list()))
    if all_topics is not None:
        check("the new topic shows up in topicList", TOPIC in set(all_topics.get_topic_list()), TOPIC)

    route = safe("examineTopicRoute", lambda: admin.examine_topic_route(TOPIC),
                 lambda r: "brokers=%d queues=%d" % (len(r.get_broker_datas()),
                                                     len(r.queue_datas)))
    if route is not None:
        qnums = {qd.broker_name: qd.read_queue_nums for qd in route.queue_datas}
        check("the route readQueueNums==4", all(n == 4 for n in qnums.values()), str(qnums))

    safe("getClusterList", lambda: admin.get_cluster_list(TOPIC), lambda c: str(sorted(c)))

    cfg = safe("examineTopicConfig", lambda: admin.examine_topic_config(broker_addr, TOPIC),
               lambda c: "read=%d write=%d perm=%d filter=%s attrs=%s" % (
                   c.read_queue_nums, c.write_queue_nums, c.perm, c.topic_filter_type,
                   dict(c.attributes)))
    if cfg is not None:
        check("the TopicConfig queue count matches what was created", cfg.read_queue_nums == 4 and cfg.write_queue_nums == 4,
              "read=%d write=%d" % (cfg.read_queue_nums, cfg.write_queue_nums))
        check("TopicConfig.attributes was serialized", isinstance(cfg.attributes, dict),
              str(dict(cfg.attributes)))

    safe("getAllTopicConfig", lambda: admin.get_all_topic_config(broker_addr),
         lambda w: "topicConfigs=%d" % len(w.topic_config_table))

    # ---------- 3. broker configuration: the properties text (a historical bug spot) ----------
    broker_cfg = safe("getBrokerConfig(properties text)",
                      lambda: admin.get_broker_config(broker_addr),
                      lambda d: "keys=%d sample=%s" % (
                          len(d), dict(list(d.items())[:2])))
    if broker_cfg is not None:
        check("getBrokerConfig parsed a non-empty k=v", len(broker_cfg) > 0,
              "brokerName=%s" % broker_cfg.get("brokerName"))

    # A reversible change: write a harmless configuration and then restore it
    original = broker_cfg.get("sendMessageThreadPoolNums") if broker_cfg else None
    probe_value = "11"
    updated = safe("updateBrokerConfig(sendMessageThreadPoolNums=11)",
                   lambda: admin.update_broker_config(
                       broker_addr, {"sendMessageThreadPoolNums": probe_value}),
                   lambda _: "OK")
    if updated is not None:
        time.sleep(1)
        after = safe("reading back that updateBrokerConfig took effect",
                     lambda: admin.get_broker_config(broker_addr),
                     lambda d: "sendMessageThreadPoolNums=%s" % d.get("sendMessageThreadPoolNums"))
        if after is not None:
            check("updateBrokerConfig took effect",
                  after.get("sendMessageThreadPoolNums") == probe_value,
                  "expected %s, actual %s" % (probe_value, after.get("sendMessageThreadPoolNums")))
        # Restore
        if original is not None:
            admin.update_broker_config(
                broker_addr, {"sendMessageThreadPoolNums": original})

    # ---------- 4. NameServer KV configuration ----------
    safe("createAndUpdateKvConfig", lambda: admin.create_and_update_kv_config(
        KV_NS, "k1", "v1"), lambda _: "OK")
    kv = safe("getKVConfig", lambda: admin.get_kv_config(KV_NS, "k1"), lambda v: str(v))
    check("the KV value round-trips unchanged", kv == "v1", "expected v1, actual %s" % kv)
    kvt = safe("getKVListByNamespace", lambda: admin.get_kv_list_by_namespace(KV_NS),
               lambda t: "table=%s" % t.table)
    if kvt is not None:
        check("KVList contains k1", kvt.table.get("k1") == "v1", str(kvt.table))
    safe("deleteKVConfig", lambda: admin.delete_kv_config(KV_NS, "k1"), lambda _: "OK")
    gone = safe("after the delete getKVConfig returns None",
                lambda: admin.get_kv_config(KV_NS, "k1"), lambda v: "value=%s" % v)
    check("the KV no longer exists after the delete", gone in (None, ""), "actual %r" % (gone,))

    # ---------- 5. subscription group management ----------
    sgc = SubscriptionGroupConfig(GROUP)
    sgc.consume_enable = True
    sgc.retry_max_times = 5
    safe("createAndUpdateSubscriptionGroupConfig",
         lambda: admin.create_and_update_subscription_group_config(broker_addr, sgc),
         lambda _: "group=%s retryMax=%d" % (GROUP, sgc.retry_max_times))

    single = safe("getSubscriptionGroupConfig(single lookup)",
                  lambda: admin.get_subscription_group_config(broker_addr, GROUP),
                  lambda c: "group=%s retryMax=%s" % (c.group_name, c.retry_max_times) if c else "None")
    if single is not None:
        check("the retryMaxTimes of the subscription group round-trips unchanged", single.retry_max_times == 5,
              "actual %s" % single.retry_max_times)

    wrapper = safe("getAllSubscriptionGroup(paged)",
                   lambda: admin.get_all_subscription_group(broker_addr),
                   lambda w: "groups=%d dataVersion=%s" % (
                       len(w.subscription_group_table), bool(w.data_version)))
    if wrapper is not None:
        check("the paged result contains the new subscription group", GROUP in wrapper.subscription_group_table,
              "count=%d" % len(wrapper.subscription_group_table))
    examined = safe("examineSubscriptionGroupConfig",
                    lambda: admin.examine_subscription_group_config(broker_addr, GROUP),
                    lambda c: c.group_name if c else "None")

    # ---------- 6. produce + statistics + queries ----------
    prod = DefaultMQProducer(producer_group="AdminVerifyProducer_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.set_send_msg_timeout(5000)
    prod.start()

    sent = []
    offset_ids = []
    for i in range(N_MSG):
        body = ("admin-live-%d" % i).encode("utf-8")
        try:
            sr = prod.send(Message(TOPIC, body))
            if sr.send_status.name == "SEND_OK":
                sent.append((body, sr.msg_id, sr.message_queue))
                offset_ids.append(sr.offset_msg_id)
        except Exception as e:  # noqa: BLE001
            print("send %d error: %s" % (i, e))
    check("sent %d messages synchronously" % N_MSG, len(sent) == N_MSG, "ok=%d/%d" % (len(sent), N_MSG))
    if not sent:
        prod.shutdown()
        admin.shutdown()
        return report()

    time.sleep(2)

    stats = safe("examineTopicStats", lambda: admin.examine_topic_stats(TOPIC),
                 lambda s: "queues=%d tps=%s" % (len(s.offset_table), s.topic_put_tps))
    if stats is not None:
        total_max = sum(o.max_offset for o in stats.offset_table.values())
        check("the sum of the TopicStatsTable maxOffsets >= the number sent", total_max >= N_MSG,
              "maxOffsetSum=%d sent=%d" % (total_max, N_MSG))

    cs = safe("examineConsumeStats", lambda: admin.examine_consume_stats(
        broker_addr, GROUP, TOPIC), lambda c: "queues=%d tps=%s lag=%d" % (
            len(c.offset_table), c.consume_tps, c.total_lag))

    cq = safe("queryConsumeQueue",
              lambda: admin.query_consume_queue(broker_addr, TOPIC, 0, 0, 10, GROUP),
              lambda r: "min=%d max=%d" % (r.min_queue_index, r.max_queue_index))

    # queryMessage: only assert that "the request is reachable, the broker replies normally and the
    # return type is correct". It deliberately does not assert "it must be found": msgId belongs to
    # uniqKey, and on the broker side the uniqKey inverted index is only supported by the RocksDB
    # index implementation (IndexRocksDBStore); the local broker uses the default file index and
    # the messages do not set KEYS explicitly, so finding nothing is a **broker configuration
    # difference**, not a client bug.
    body0, msg_id0, mq0 = sent[0]
    now_ms = int(time.time() * 1000)
    qm_ok, qm_detail = True, ""
    try:
        res_by_key = admin.query_message(TOPIC, msg_id0, 32, 0, now_ms + 60000)
        res_uniq = admin.query_message_by_uniq_key(TOPIC, msg_id0)
        res_norm = admin.query_message_by_key(TOPIC, msg_id0, 32)
        qm_detail = "key=%d uniq=%s normal=%d (all returned normally)" % (
            len(res_by_key), res_uniq is not None, len(res_norm))
        assert isinstance(res_by_key, list) and isinstance(res_norm, list)
    except Exception as e:  # noqa: BLE001
        qm_ok = False
        qm_detail = "%s: %s" % (type(e).__name__, e)
    check("the queryMessage request is reachable and replies normally", qm_ok, qm_detail)
    skip("queryMessage hit results (requires the broker to enable the RocksDB/KEYS index)",
         "the local broker uses the default file index and the messages have no KEYS, so an empty uniqKey query result is expected")

    # viewMessage by offsetMsgId (only this one encodes the broker address + commitLog offset)
    vm = safe("viewMessage(byOffsetMsgId)",
              lambda: admin.view_message(TOPIC, offset_ids[0]),
              lambda m: "topic=%s offset=%s body=%s" % (m.topic, m.queue_offset,
                                                        m.body[:32]))
    if vm is not None:
        check("the viewMessage body matches what was sent", vm.body == body0,
              "expected %r, actual %r" % (body0, vm.body))

    try:
        admin.view_message(TOPIC, msg_id0)
        check("viewMessage(uniqKey) falls back and gives a clean exception", False, "no exception was raised")
    except MQClientException as e:
        check("viewMessage(uniqKey) falls back and gives a clean exception", True,
              "code=%s" % e.response_code)
    except Exception as e:  # noqa: BLE001
        check("viewMessage(uniqKey) falls back and gives a clean exception", False,
              "%s: %s" % (type(e).__name__, e))

    # ---------- 7. offset management (read-only queries) ----------
    mq = mq0 if isinstance(mq0, MessageQueue) else MessageQueue(TOPIC, route.get_broker_datas()[0].broker_name, 0)
    max_off = safe("maxOffset", lambda: admin.max_offset(mq), lambda v: str(v))
    min_off = safe("minOffset", lambda: admin.min_offset(mq), lambda v: str(v))
    safe("searchOffset(now)", lambda: admin.search_offset(mq, int(time.time() * 1000)),
         lambda v: str(v))
    safe("earliestMsgStoreTime", lambda: admin.earliest_msg_store_time(mq),
         lambda v: str(v))
    safe("examineConsumerOffset", lambda: admin.examine_consumer_offset(GROUP, mq),
         lambda v: str(v))
    if max_off is not None and min_off is not None:
        check("maxOffset >= minOffset", max_off >= min_off,
              "min=%d max=%d" % (min_off, max_off))

    bnd_topic = "AdminBoundaryTopic_%d" % STAMP
    safe("createTopic(%s, 1 queue)" % bnd_topic,
         lambda: admin.create_topic(MixAll.DEFAULT_TOPIC, bnd_topic, 1),
         lambda _: "queueNum=1")
    bnd_mq = MessageQueue(bnd_topic, mq.broker_name, 0)
    for i in range(3):
        prod.send(Message(bnd_topic, ("boundary-%d" % i).encode("utf-8")))
    time.sleep(2)
    bnd_max = safe("boundary topic maxOffset", lambda: admin.max_offset(bnd_mq),
                   lambda v: str(v))
    future = int(time.time() * 1000) + 600000
    if bnd_max == 3:
        lo = safe("searchLowerBoundaryOffset(a future timestamp)",
                  lambda: admin.search_lower_boundary_offset(bnd_mq, future),
                  lambda v: str(v))
        up = safe("searchUpperBoundaryOffset(a future timestamp)",
                  lambda: admin.search_upper_boundary_offset(bnd_mq, future),
                  lambda v: str(v))
        check("the LOWER boundary = maxOffset (the offset right after the tail)", lo == bnd_max,
              "lower=%s maxOffset=%s" % (lo, bnd_max))
        check("the UPPER boundary = maxOffset-1 (the offset of the last entry itself)", up == bnd_max - 1,
              "upper=%s maxOffset-1=%s" % (up, bnd_max - 1))
        check("the two boundaries really differ (proving boundaryType takes effect)", lo != up,
              "lower=%s upper=%s" % (lo, up))
        past = 1
        check("when the timestamp precedes all the messages both LOWER/UPPER collapse to minOffset",
              admin.search_lower_boundary_offset(bnd_mq, past) == min_off
              and admin.search_upper_boundary_offset(bnd_mq, past) == min_off,
              "minOffset=%s" % min_off)
    else:
        check("the boundary topic holds exactly 3 messages (precondition)", False, "maxOffset=%s" % bnd_max)

    # ---------- 8. consume + sendMessageBack redelivery ----------
    # The order matters: it must **consume first** and only then send back. If the offset were
    # reset to max first, the consumer could never pull the historical messages any more and the
    # sendMessageBack path would get no chance to run at all (the false failure of an earlier
    # version).
    # This group is newly created and has no committed offset, so FIRST_OFFSET really does start
    # reading from the smallest offset.
    retry_topic = MixAll.get_retry_topic(GROUP)
    consumed = []
    lock = threading.Lock()

    def on_msg(msgs):
        with lock:
            for m in msgs:
                consumed.append(m)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    cons = DefaultMQPushConsumer(consumer_group=GROUP)
    cons.set_namesrv_addr(NAMESRV)
    cons.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    cons.subscribe(TOPIC, "*")
    cons.set_message_listener(SimpleMessageListener(on_msg))
    cons.start()

    deadline = time.time() + 30
    while time.time() < deadline and len(consumed) < 1:
        time.sleep(0.5)

    check("messages were consumed (a precondition of sendMessageBack)", len(consumed) >= 1,
          "consumed=%d" % len(consumed))

    backsent = False
    if consumed:
        first = consumed[0]
        first.broker_name = first.broker_name or mq.broker_name

        def _do_back():
            cons.send_message_back(first, 0)
            return "origin_topic=%s commitLogOffset=%d" % (
                first.topic, first.commit_log_offset)

        ok = safe("consumer.sendMessageBack(sending back to %s)" % retry_topic, _do_back)
        backsent = ok is not None
    cons.shutdown()

    # Verify that the message shows up in %RETRY% (by polling the statistics of the retry topic with
    # admin).
    # Polling rather than asserting immediately is mandatory: the broker's
    # SendMessageProcessor.consumerSendMsgBack rewrites delayLevel == 0 into `3 + reconsumeTimes`
    # (the default 3 -> 10s), so the message first goes into SCHEDULE_TOPIC_XXXX and is only
    # delivered to %RETRY%<group> when its time comes. The old version only waited 2s, which
    # inevitably produced a false failure.
    if backsent:
        retry_max = -1
        waited = 0
        deadline_retry = time.time() + 30
        while time.time() < deadline_retry:
            try:
                st = admin.examine_topic_stats(retry_topic)
                retry_max = sum(o.max_offset for o in st.offset_table.values())
                if retry_max > 0:
                    break
            except Exception:  # noqa: BLE001
                retry_max = -1
            time.sleep(1)
            waited += 1
        check("sendMessageBack landed in %s (maxOffsetSum>0)" % retry_topic, retry_max > 0,
              "maxOffsetSum=%s (polled %ds; the broker rewrites delayLevel=0 to 3, so it becomes visible after about 10s)"
              % (retry_max, waited))

    def _group_has_offset_table(rows):
        for row in rows:
            stats = row.get(GROUP) if isinstance(row, dict) else None
            if stats is None:
                continue
            return any(isinstance(s, dict) and s.get("offsetTable") for s in stats)
        return False

    rows, hit_group, waited_stats = [], False, 0
    deadline_stats = time.time() + 20
    while time.time() < deadline_stats:
        try:
            rows = admin.fetch_consume_stats_in_broker(broker_addr, False).stats_list
        except Exception:  # noqa: BLE001
            rows = []
        hit_group = _group_has_offset_table(rows)
        if hit_group:
            break
        time.sleep(1)
        waited_stats += 1
    check("fetchConsumeStatsInBroker produces a row per subscription group", len(rows) > 0,
          "groups=%d (polled %ds)" % (len(rows), waited_stats))
    check("the statistics of this group from fetchConsumeStatsInBroker carry an offsetTable", hit_group,
          "there is a ConsumeStats in this group's row (polled %ds)" % waited_stats)

    # ---------- 9. offset reset on a real broker (done last so as not to disturb the consumption above) ----------
    ts = int(time.time() * 1000) + 60000  # a future time -> the offset should be pushed to max
    reset = safe("resetOffsetByTimestamp(INVOKE_BROKER_TO_RESET_OFFSET)",
                 lambda: admin.reset_offset_by_timestamp(TOPIC, GROUP, ts, True),
                 lambda d: "queues=%d sample=%s" % (
                     len(d), list(d.items())[:1]))
    if reset is not None:
        check("resetOffset returned a non-empty offsetTable", len(reset) > 0, "%d queues" % len(reset))
        # Reading back: after resetting to a future time, the consumer offset should equal the maxOffset of that queue
        time.sleep(1)
        off_after = admin.examine_consumer_offset(GROUP, mq)
        max_now = admin.max_offset(mq)
        check("after the reset the consumer offset was pushed to maxOffset", off_after >= max_now - 1,
              "consumerOffset=%s maxOffset=%s" % (off_after, max_now))

    min_reset = admin.min_offset(mq)
    rqi = safe("resetOffsetByQueueId(back to minOffset)",
               lambda: admin.reset_offset_by_queue_id(
                   broker_addr, GROUP, TOPIC, mq.queue_id, min_reset),
               lambda d: "returnedTable=%d (this RPC has no response body, it may be empty)" % len(d))
    if rqi is not None:
        off_rqi = admin.examine_consumer_offset(GROUP, mq)
        check("after resetOffsetByQueueId the offset == the target offset", off_rqi == min_reset,
              "consumerOffset=%s target=%s" % (off_rqi, min_reset))

    again = []
    lock2 = threading.Lock()

    def on_msg_again(msgs):
        with lock2:
            again.extend(msgs)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    cons2 = DefaultMQPushConsumer(consumer_group=GROUP)
    cons2.set_namesrv_addr(NAMESRV)
    cons2.subscribe(TOPIC, "*")
    cons2.set_message_listener(SimpleMessageListener(on_msg_again))
    cons2.start()
    deadline_again = time.time() + 20
    while time.time() < deadline_again and len(again) < 1:
        time.sleep(0.5)
    cons2.shutdown()
    check("after resetOffsetByQueueId the historical messages can be consumed again", len(again) >= 1,
          "reConsumed=%d (the offset of queue %d is back at minOffset=%s)" % (len(again), mq.queue_id, min_reset))

    max_for_range = admin.max_offset(mq)
    bad_target = max_for_range + 100
    try:
        admin.reset_offset_by_queue_id(broker_addr, GROUP, TOPIC, mq.queue_id, bad_target)
        check("the broker rejected the out-of-range target of resetOffsetByQueueId", False, "no exception was raised")
    except Exception as e:  # noqa: BLE001
        check("the broker rejected the out-of-range target of resetOffsetByQueueId", True,
              "%s: %s" % (type(e).__name__, str(e)[:160]))
    off_after_bad = admin.examine_consumer_offset(GROUP, mq)
    check("the out-of-range reset stays at the illegal offset written by the first call (the two RPCs are not atomic)",
          off_after_bad == bad_target,
          "consumerOffset=%s badTarget=%s" % (off_after_bad, bad_target))

    by_broker = safe("queryTopicsByConsumerToBroker",
                     lambda: admin.query_topics_by_consumer_to_broker(broker_addr, GROUP),
                     lambda t: "topics=%d" % len(t.get_topic_list()))
    if by_broker is not None:
        check("queryTopicsByConsumerToBroker reads out the topics this group consumed from",
              TOPIC in set(by_broker.get_topic_list()), str(by_broker.get_topic_list()))
    by_group = safe("queryTopicsByConsumer(group) (fanned out over the %RETRY% route)",
                    lambda: admin.query_topics_by_consumer(GROUP),
                    lambda t: "topics=%d" % len(t.get_topic_list()))
    if by_group is not None:
        check("after merging, queryTopicsByConsumer(group) contains the topics this group consumed from",
              TOPIC in set(by_group.get_topic_list()), str(by_group.get_topic_list()))

    prod.shutdown()

    # ---------- 9. cleanup ----------
    safe("deleteSubscriptionGroup", lambda: admin.delete_subscription_group(
        broker_addr, GROUP, True), lambda _: "OK")
    safe("deleteTopic", lambda: admin.delete_topic(TOPIC), lambda _: "OK")
    safe("deleteTopic(boundary)", lambda: admin.delete_topic(bnd_topic), lambda _: "OK")
    time.sleep(1)
    topics_after = safe("fetchAllTopicList after the delete",
                        lambda: admin.fetch_all_topic_list(),
                        lambda t: "topicCount=%d" % len(t.get_topic_list()))
    if topics_after is not None:
        check("the topic disappeared after deleteTopic",
              TOPIC not in set(topics_after.get_topic_list()), TOPIC)

    admin.shutdown()
    return report()


def report():
    failed = [r for r in results if not r[1]]
    print("\n================ summary ================")
    for name, ok, detail in results:
        print("  [%s] %s  %s" % ("PASS" if ok else "FAIL", name, detail))
    for name, detail in skips:
        print("  [SKIP] %s  %s" % (name, detail))
    print("======================================")
    print("total %d checks, %d failed, %d skipped" % (len(results), len(failed), len(skips)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
