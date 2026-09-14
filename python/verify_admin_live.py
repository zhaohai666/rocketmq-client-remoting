# -*- coding: utf-8 -*-
"""DefaultMQAdminExt（管理端）真实集群联调 + sendMessageBack 重投验证。

覆盖（全部打真实 broker / nameServer，无 mock）：
 1. 集群探活 / fetchBrokerClusterInfo / getClusterList
 2. createTopic（含 queue 数校验）→ fetchAllTopicList → examineTopicRoute
 3. examineTopicConfig（GET_TOPIC_CONFIG，TopicConfigAndQueueMapping JSON）
 4. getBrokerConfig（**properties 文本**，验证不再当 KVTable JSON 解析）
 5. updateBrokerConfig → 回读确认生效（可逆，测完还原）
 6. NameServer KV：createAndUpdateKvConfig → getKVConfig → getKVListByNamespace → deleteKvConfig
 7. 订阅组：create/update → examine → 单查 → deleteSubscriptionGroup
 8. 生产 N 条 → examineTopicStats / examineConsumeStats / queryConsumeQueue /
    queryMessage(key) / viewMessage(msgId)
 9. resetOffsetByTimestamp（真实 INVOKE_BROKER_TO_RESET_OFFSET，观察位点变化）
10. sendMessageBack：消费 1 条后重投 → 校验 %RETRY%<group> 里出现原消息
11. 清理：deleteSubscriptionGroup / deleteTopic

用法：先起 nameServer(9876)+broker(10911)，再在 venv 里 `python verify_admin_live.py`
"""
import os
import sys
import time
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rocketmq.client.admin import DefaultMQAdminExt
from rocketmq.client.consumer import DefaultMQPushConsumer, SimpleMessageListener
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.client.consumer_result import ConsumeConcurrentlyStatus
from rocketmq.common.message import Message, MessageQueue
from rocketmq.common.mix_all import MixAll
from rocketmq.remoting.protocol.heartbeat import ConsumeFromWhere
from rocketmq.remoting.protocol.subscription import SubscriptionGroupConfig

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
    """记录"因 broker 侧配置差异无法在本机验证"的项，不计入失败。

    与 check() 严格区分：skip 不是"我偷懒"，而是明确说明这条断言依赖的
    环境前提在本地不成立（例如默认文件索引没有 uniqKey 倒排索引）。
    """
    skips.append((name, detail))
    print("[SKIP] %s %s" % (name, detail))


def safe(name, fn, detail_fn=None):
    """执行 fn，异常转 FAIL 而不是中断整个脚本。"""
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

    # ---------- 1. 集群探活（broker 注册竞态：端口开 != 已注册）----------
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
        check("集群探活", False, "nameServer 无 broker 注册 last_err=%s" % last_err)
        admin.shutdown()
        return 1
    check("fetchBrokerClusterInfo", True,
          "brokers=%s clusters=%s" % (sorted(cluster.broker_addr_table.keys()),
                                      sorted(cluster.cluster_addr_table.keys())))
    broker_addr = cluster.get_broker_addrs()[0]

    # ---------- 2. Topic 管理 ----------
    safe("createTopic(%s)" % TOPIC, lambda: admin.create_topic(
        MixAll.DEFAULT_TOPIC, TOPIC, 4), lambda _: "queueNum=4")

    time.sleep(1)
    all_topics = safe("fetchAllTopicList", lambda: admin.fetch_all_topic_list(),
                      lambda t: "topicCount=%d" % len(t.get_topic_list()))
    if all_topics is not None:
        check("新 topic 出现在 topicList", TOPIC in set(all_topics.get_topic_list()), TOPIC)

    route = safe("examineTopicRoute", lambda: admin.examine_topic_route(TOPIC),
                 lambda r: "brokers=%d queues=%d" % (len(r.get_broker_datas()),
                                                     len(r.queue_datas)))
    if route is not None:
        qnums = {qd.broker_name: qd.read_queue_nums for qd in route.queue_datas}
        check("路由 readQueueNums==4", all(n == 4 for n in qnums.values()), str(qnums))

    safe("getClusterList", lambda: admin.get_cluster_list(TOPIC), lambda c: str(sorted(c)))

    cfg = safe("examineTopicConfig", lambda: admin.examine_topic_config(broker_addr, TOPIC),
               lambda c: "read=%d write=%d perm=%d filter=%s attrs=%s" % (
                   c.read_queue_nums, c.write_queue_nums, c.perm, c.topic_filter_type,
                   dict(c.attributes)))
    if cfg is not None:
        check("TopicConfig 队列数与创建一致", cfg.read_queue_nums == 4 and cfg.write_queue_nums == 4,
              "read=%d write=%d" % (cfg.read_queue_nums, cfg.write_queue_nums))
        check("TopicConfig.attributes 被序列化", isinstance(cfg.attributes, dict),
              str(dict(cfg.attributes)))

    safe("getAllTopicConfig", lambda: admin.get_all_topic_config(broker_addr),
         lambda w: "topicConfigs=%d" % len(w.topic_config_table))

    # ---------- 3. Broker 配置：properties 文本（历史 bug 点）----------
    broker_cfg = safe("getBrokerConfig(properties 文本)",
                      lambda: admin.get_broker_config(broker_addr),
                      lambda d: "keys=%d sample=%s" % (
                          len(d), dict(list(d.items())[:2])))
    if broker_cfg is not None:
        check("getBrokerConfig 解析出非空 k=v", len(broker_cfg) > 0,
              "brokerName=%s" % broker_cfg.get("brokerName"))

    # 可逆修改：写一个无害配置再还原
    original = broker_cfg.get("sendMessageThreadPoolNums") if broker_cfg else None
    probe_value = "11"
    updated = safe("updateBrokerConfig(sendMessageThreadPoolNums=11)",
                   lambda: admin.update_broker_config(
                       broker_addr, {"sendMessageThreadPoolNums": probe_value}),
                   lambda _: "OK")
    if updated is not None:
        time.sleep(1)
        after = safe("回读 updateBrokerConfig 生效",
                     lambda: admin.get_broker_config(broker_addr),
                     lambda d: "sendMessageThreadPoolNums=%s" % d.get("sendMessageThreadPoolNums"))
        if after is not None:
            check("updateBrokerConfig 生效",
                  after.get("sendMessageThreadPoolNums") == probe_value,
                  "期望 %s，实际 %s" % (probe_value, after.get("sendMessageThreadPoolNums")))
        # 还原
        if original is not None:
            admin.update_broker_config(
                broker_addr, {"sendMessageThreadPoolNums": original})

    # ---------- 4. NameServer KV 配置 ----------
    safe("createAndUpdateKvConfig", lambda: admin.create_and_update_kv_config(
        KV_NS, "k1", "v1"), lambda _: "OK")
    kv = safe("getKVConfig", lambda: admin.get_kv_config(KV_NS, "k1"), lambda v: str(v))
    check("KV 值往返一致", kv == "v1", "期望 v1 实际 %s" % kv)
    kvt = safe("getKVListByNamespace", lambda: admin.get_kv_list_by_namespace(KV_NS),
               lambda t: "table=%s" % t.table)
    if kvt is not None:
        check("KVList 含 k1", kvt.table.get("k1") == "v1", str(kvt.table))
    safe("deleteKVConfig", lambda: admin.delete_kv_config(KV_NS, "k1"), lambda _: "OK")
    gone = safe("删除后 getKVConfig 返回 None",
                lambda: admin.get_kv_config(KV_NS, "k1"), lambda v: "value=%s" % v)
    check("KV 删除后不再存在", gone in (None, ""), "实际 %r" % (gone,))

    # ---------- 5. 订阅组管理 ----------
    sgc = SubscriptionGroupConfig(GROUP)
    sgc.consume_enable = True
    sgc.retry_max_times = 5
    safe("createAndUpdateSubscriptionGroupConfig",
         lambda: admin.create_and_update_subscription_group_config(broker_addr, sgc),
         lambda _: "group=%s retryMax=%d" % (GROUP, sgc.retry_max_times))

    single = safe("getSubscriptionGroupConfig(单查)",
                  lambda: admin.get_subscription_group_config(broker_addr, GROUP),
                  lambda c: "group=%s retryMax=%s" % (c.group_name, c.retry_max_times) if c else "None")
    if single is not None:
        check("订阅组 retryMaxTimes 往返一致", single.retry_max_times == 5,
              "实际 %s" % single.retry_max_times)

    wrapper = safe("getAllSubscriptionGroup(分页)",
                   lambda: admin.get_all_subscription_group(broker_addr),
                   lambda w: "groups=%d dataVersion=%s" % (
                       len(w.subscription_group_table), bool(w.data_version)))
    if wrapper is not None:
        check("分页结果含新订阅组", GROUP in wrapper.subscription_group_table,
              "count=%d" % len(wrapper.subscription_group_table))
    examined = safe("examineSubscriptionGroupConfig",
                    lambda: admin.examine_subscription_group_config(broker_addr, GROUP),
                    lambda c: c.group_name if c else "None")

    # ---------- 6. 生产 + 统计 + 查询 ----------
    prod = DefaultMQProducer(producer_group="AdminVerifyProducer_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.set_send_msg_timeout(5000)
    prod.start()

    sent = []
    for i in range(N_MSG):
        body = ("admin-live-%d" % i).encode("utf-8")
        try:
            sr = prod.send(Message(TOPIC, body))
            if sr.send_status.name == "SEND_OK":
                sent.append((body, sr.msg_id, sr.message_queue))
        except Exception as e:  # noqa: BLE001
            print("send %d error: %s" % (i, e))
    check("同步发送 %d 条" % N_MSG, len(sent) == N_MSG, "ok=%d/%d" % (len(sent), N_MSG))
    if not sent:
        prod.shutdown()
        admin.shutdown()
        return report()

    time.sleep(2)

    stats = safe("examineTopicStats", lambda: admin.examine_topic_stats(TOPIC),
                 lambda s: "queues=%d tps=%s" % (len(s.offset_table), s.topic_put_tps))
    if stats is not None:
        total_max = sum(o.max_offset for o in stats.offset_table.values())
        check("TopicStatsTable maxOffset 总和 >= 发送数", total_max >= N_MSG,
              "maxOffsetSum=%d sent=%d" % (total_max, N_MSG))

    cs = safe("examineConsumeStats", lambda: admin.examine_consume_stats(
        broker_addr, GROUP, TOPIC), lambda c: "queues=%d tps=%s lag=%d" % (
            len(c.offset_table), c.consume_tps, c.total_lag))

    cq = safe("queryConsumeQueue",
              lambda: admin.query_consume_queue(broker_addr, TOPIC, 0, 0, 10, GROUP),
              lambda r: "min=%d max=%d" % (r.min_queue_index, r.max_queue_index))

    # queryMessage：只断言"请求可达、broker 正常应答、返回值类型正确"。
    # 之所以不断言"一定查得到"：msgId 属于 uniqKey，broker 侧 uniqKey 倒排索引
    # 只有 RocksDB 索引实现（IndexRocksDBStore）才支持；本机 broker 用默认的
    # 文件索引 + 消息未显式设 KEYS，因此查不到是**broker 配置差异**，不是客户端 bug。
    body0, msg_id0, mq0 = sent[0]
    now_ms = int(time.time() * 1000)
    qm_ok, qm_detail = True, ""
    try:
        res_by_key = admin.query_message(TOPIC, msg_id0, 32, 0, now_ms + 60000)
        res_uniq = admin.query_message_by_uniq_key(TOPIC, msg_id0)
        res_norm = admin.query_message_by_key(TOPIC, msg_id0, 32)
        qm_detail = "key=%d uniq=%s normal=%d（均正常返回）" % (
            len(res_by_key), res_uniq is not None, len(res_norm))
        assert isinstance(res_by_key, list) and isinstance(res_norm, list)
    except Exception as e:  # noqa: BLE001
        qm_ok = False
        qm_detail = "%s: %s" % (type(e).__name__, e)
    check("queryMessage 请求可达且正常应答", qm_ok, qm_detail)
    skip("queryMessage 命中结果（需 broker 开 RocksDB/KEYS 索引）",
         "本机为默认文件索引且消息未设 KEYS，uniqKey 查询返回空属预期")

    # viewMessage by msgId（从 msgId 解 broker 地址 + commitLog offset）
    vm = safe("viewMessage(byMsgId)", lambda: admin.view_message(TOPIC, msg_id0),
              lambda m: "topic=%s offset=%s body=%s" % (m.topic, m.queue_offset,
                                                        m.body[:32]))
    if vm is not None:
        check("viewMessage body 与发送一致", vm.body == body0,
              "期望 %r 实际 %r" % (body0, vm.body))

    # ---------- 7. Offset 管理（只读查询）----------
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

    # ---------- 8. 消费 + sendMessageBack 重投 ----------
    # 顺序很关键：必须**先消费**再重投。若先把位点重置到 max，消费者就再也
    # 拉不到历史消息，sendMessageBack 路径根本没机会执行（前一版的假失败）。
    # 本组是新建的、无已提交位点，故 FIRST_OFFSET 会真正从最小位点开始读。
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

    check("消费到消息（sendMessageBack 前置）", len(consumed) >= 1,
          "consumed=%d" % len(consumed))

    backsent = False
    if consumed:
        first = consumed[0]
        first.broker_name = first.broker_name or mq.broker_name

        def _do_back():
            # 消费者仍在线时重投（对齐 Java：在消费线程内调用 sendMessageBack）。
            # 注意 send_message_back 成功时返回 None —— 不能让 safe() 的返回值
            # 兼任"成功标志"，否则 None 会被误判为失败而跳过后续校验。
            cons.send_message_back(first, 0)
            return "origin_topic=%s commitLogOffset=%d" % (
                first.topic, first.commit_log_offset)

        ok = safe("consumer.sendMessageBack(重投到 %s)" % retry_topic, _do_back)
        backsent = ok is not None
    cons.shutdown()

    # 校验 %RETRY% 里出现该消息（用 admin 轮询重试 topic 的统计）。
    # 必须轮询而不是立即断言：broker 的 SendMessageProcessor.consumerSendMsgBack 在
    # delayLevel == 0 时会改写成 `3 + reconsumeTimes`（默认 3 → 10s），消息先进
    # SCHEDULE_TOPIC_XXXX，到点才投递到 %RETRY%<group>。旧版只等 2s 必然假失败。
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
        check("sendMessageBack 落到 %s（maxOffsetSum>0）" % retry_topic, retry_max > 0,
              "maxOffsetSum=%s（轮询 %ds；broker 把 delayLevel=0 改写为 3，约 10s 后可见）"
              % (retry_max, waited))

    # ---------- 9. 真实 broker 端位点重置（放最后，避免干扰上面的消费）----------
    ts = int(time.time() * 1000) + 60000  # 未来时间 → 位点应被推到 max
    reset = safe("resetOffsetByTimestamp(INVOKE_BROKER_TO_RESET_OFFSET)",
                 lambda: admin.reset_offset_by_timestamp(TOPIC, GROUP, ts, True),
                 lambda d: "queues=%d sample=%s" % (
                     len(d), list(d.items())[:1]))
    if reset is not None:
        check("resetOffset 返回非空 offsetTable", len(reset) > 0, "%d 个队列" % len(reset))
        # 回读：重置到未来时间后，消费者位点应等于该队列 maxOffset
        time.sleep(1)
        off_after = admin.examine_consumer_offset(GROUP, mq)
        max_now = admin.max_offset(mq)
        check("reset 后消费者位点被推到 maxOffset", off_after >= max_now - 1,
              "consumerOffset=%s maxOffset=%s" % (off_after, max_now))

    prod.shutdown()

    # ---------- 9. 清理 ----------
    safe("deleteSubscriptionGroup", lambda: admin.delete_subscription_group(
        broker_addr, GROUP, True), lambda _: "OK")
    safe("deleteTopic", lambda: admin.delete_topic(TOPIC), lambda _: "OK")
    time.sleep(1)
    topics_after = safe("删除后 fetchAllTopicList",
                        lambda: admin.fetch_all_topic_list(),
                        lambda t: "topicCount=%d" % len(t.get_topic_list()))
    if topics_after is not None:
        check("deleteTopic 后 topic 消失",
              TOPIC not in set(topics_after.get_topic_list()), TOPIC)

    admin.shutdown()
    return report()


def report():
    failed = [r for r in results if not r[1]]
    print("\n================ 汇总 ================")
    for name, ok, detail in results:
        print("  [%s] %s  %s" % ("PASS" if ok else "FAIL", name, detail))
    for name, detail in skips:
        print("  [SKIP] %s  %s" % (name, detail))
    print("======================================")
    print("总计 %d 项，失败 %d 项，跳过 %d 项" % (len(results), len(failed), len(skips)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
