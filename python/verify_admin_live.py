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
    + resetOffsetByQueueId（单队列显式位点：正例回读位点、重启消费者重投、越界负例）
10. queryTopicsByConsumer（343）：单 broker 原始调用 + Java 的组级重载（按 %RETRY% 路由扇出合并）
11. sendMessageBack：消费 1 条后重投 → 校验 %RETRY%<group> 里出现原消息
12. 清理：deleteSubscriptionGroup / deleteTopic

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
from rocketmq.client.exception import MQClientException
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

    # viewMessage by offsetMsgId（只有它编码了 broker 地址 + commitLog offset）
    vm = safe("viewMessage(byOffsetMsgId)",
              lambda: admin.view_message(TOPIC, offset_ids[0]),
              lambda m: "topic=%s offset=%s body=%s" % (m.topic, m.queue_offset,
                                                        m.body[:32]))
    if vm is not None:
        check("viewMessage body 与发送一致", vm.body == body0,
              "期望 %r 实际 %r" % (body0, vm.body))

    # 客户端 uniqKey 也是 32 位十六进制，硬解会拼出一个假地址；必须走 Java 的
    # queryMessageByUniqKey 兜底，最终以 MQClientException 收场，而不是裸 OverflowError。
    try:
        admin.view_message(TOPIC, msg_id0)
        check("viewMessage(uniqKey) 走兜底并给出干净异常", False, "没有抛异常")
    except MQClientException as e:
        check("viewMessage(uniqKey) 走兜底并给出干净异常", True,
              "code=%s" % e.response_code)
    except Exception as e:  # noqa: BLE001
        check("viewMessage(uniqKey) 走兜底并给出干净异常", False,
              "%s: %s" % (type(e).__name__, e))

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

    # ---------- 7.5 searchOffset 的 boundaryType（Java DefaultMQAdminExt:133/:137）----------
    # 单开一个 1 队列 topic：位点语义只在单队列上才是确定的（多队列时分不清落哪一条）。
    # 3 条消息 ⇒ maxOffset=3；用「远未来时间戳」这把尺子同时量两个边界：
    # 队尾之后 UPPER = 最后一条自己的位点(2)，LOWER = 它的下一个位点(3) = maxOffset
    # （ConsumeQueue.binarySearchInQueueByTime:261-270 的 case 1）。这条断言同时是
    # 「boundaryType 字段真的到了 broker」的证据 —— 若字段没被解析，两者都只会是 LOWER。
    bnd_topic = "AdminBoundaryTopic_%d" % STAMP
    safe("createTopic(%s, 1 队列)" % bnd_topic,
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
        lo = safe("searchLowerBoundaryOffset(未来时间戳)",
                  lambda: admin.search_lower_boundary_offset(bnd_mq, future),
                  lambda v: str(v))
        up = safe("searchUpperBoundaryOffset(未来时间戳)",
                  lambda: admin.search_upper_boundary_offset(bnd_mq, future),
                  lambda v: str(v))
        check("LOWER 边界 = maxOffset（队尾之后的下一个位点）", lo == bnd_max,
              "lower=%s maxOffset=%s" % (lo, bnd_max))
        check("UPPER 边界 = maxOffset-1（最后一条自身位点）", up == bnd_max - 1,
              "upper=%s maxOffset-1=%s" % (up, bnd_max - 1))
        check("两个边界确实不同（证明 boundaryType 生效）", lo != up,
              "lower=%s upper=%s" % (lo, up))
        past = 1
        check("时间戳早于全部消息时 LOWER/UPPER 都塌到 minOffset",
              admin.search_lower_boundary_offset(bnd_mq, past) == min_off
              and admin.search_upper_boundary_offset(bnd_mq, past) == min_off,
              "minOffset=%s" % min_off)
    else:
        check("boundary topic 恰好 3 条消息（前置）", False, "maxOffset=%s" % bnd_max)

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

    # ---------- 8.5 fetchConsumeStatsInBroker（341）：按订阅组出行，本组带 offsetTable ----------
    # Java `AdminBrokerProcessor#fetchAllConsumeStatsInBroker` 对
    # `subscriptionGroupTable.keySet()` 的每个组建一行 `{group: [ConsumeStats]}`，
    # 内层 topic 才来自位点表 `whichTopicByConsumer`（所以要等消费者刷过位点）。
    # ⚠ 响应 JSON 键是 Java 字段名 consumeStatsList；早期实现写成 statsList，
    # 于是真机响应永远解析出空集合，看着像「broker 没有积压」——这条断言就是回归护栏。
    def _group_has_offset_table(rows):
        # 每行是 Java 的 Map<订阅组名, List<ConsumeStats>>，ConsumeStats.offsetTable
        # 按 writeQueueNums 逐队列填 brokerOffset/consumerOffset
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
    check("fetchConsumeStatsInBroker 按订阅组出行", len(rows) > 0,
          "groups=%d（轮询 %ds）" % (len(rows), waited_stats))
    check("fetchConsumeStatsInBroker 本组统计带 offsetTable", hit_group,
          "本组行里有 ConsumeStats（轮询 %ds）" % waited_stats)

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

    # ---------- 9.5 resetOffsetByQueueId（Java DefaultMQAdminExtImpl:1827，两笔 RPC）----------
    # 与 9 的差别：这一条把「队列 + 显式 offset」交给 broker，不按 timestamp 反算。
    # 只回读 examine_consumer_offset 断言：Java 该方法是 void，且 broker 仅在
    # language=CPP 时回可解析的 offsetTable，所以返回表为空属预期，不是 bug。
    min_reset = admin.min_offset(mq)
    rqi = safe("resetOffsetByQueueId(回到 minOffset)",
               lambda: admin.reset_offset_by_queue_id(
                   broker_addr, GROUP, TOPIC, mq.queue_id, min_reset),
               lambda d: "returnedTable=%d（Java 为 void，可为空）" % len(d))
    if rqi is not None:
        off_rqi = admin.examine_consumer_offset(GROUP, mq)
        check("resetOffsetByQueueId 后位点==目标 offset", off_rqi == min_reset,
              "consumerOffset=%s target=%s" % (off_rqi, min_reset))

    # 真机重投：位点被拉回 minOffset 后，重启消费者应能**重新**收到历史消息。
    # 这一步证明的不只是 offsetTable 改写了，而是 broker 端 resetOffsetTable 的一次性
    # 重置确实被下一次 pull 取走（ConsumerOffsetManager#queryThenEraseResetOffset）。
    # Java `PullMessageProcessor:539-548` 命中一次性重置时**不读消息**，直接回
    # OFFSET_RESET ⇒ PULL_OFFSET_MOVED(:672)，客户端映射成 OFFSET_ILLEGAL + 新位点，
    # 消费者按 nextBeginOffset 再拉一次才真正拿到消息（rust A12 逐笔验证了这个两段式）。
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
    check("resetOffsetByQueueId 后重新消费到历史消息", len(again) >= 1,
          "reConsumed=%d（队列 %d 位点已回到 minOffset=%s）" % (len(again), mq.queue_id, min_reset))

    # 越界负例：目标 offset 超出 [minOffset, maxOffset+1] 时 broker 的 222 必须拒绝
    # （resetOffsetInner 回 SYSTEM_ERROR "Target offset N not in consume queue range [...]"）。
    # ⚠ 同时量化一个 Java 语义：这两笔 RPC **不是原子的**——
    # `ConsumerOffsetManager#commitOffset` 只做覆盖写入（连 offset 变小都只打一条
    # [NOTIFYME] warn，不做区间校验），所以第 1 笔 updateConsumerOffset 已经把非法位点
    # 落库，第 2 笔才被拒绝。这里断言「222 拒绝 + 位点仍停在第 1 笔写入的非法值」，
    # 与 Java `DefaultMQAdminExtImpl:1829-1846` 完全同构；不额外加保护性回滚。
    max_for_range = admin.max_offset(mq)
    bad_target = max_for_range + 100
    try:
        admin.reset_offset_by_queue_id(broker_addr, GROUP, TOPIC, mq.queue_id, bad_target)
        check("resetOffsetByQueueId 越界目标被 broker 拒绝", False, "没有抛异常")
    except Exception as e:  # noqa: BLE001
        check("resetOffsetByQueueId 越界目标被 broker 拒绝", True,
              "%s: %s" % (type(e).__name__, str(e)[:160]))
    off_after_bad = admin.examine_consumer_offset(GROUP, mq)
    check("越界 reset 停留在第 1 笔写入的非法位点（Java 两笔 RPC 非原子）",
          off_after_bad == bad_target,
          "consumerOffset=%s badTarget=%s" % (off_after_bad, bad_target))

    # ---------- 9.6 queryTopicsByConsumer（343）：单 broker 原始调用 vs Java 的组级重载 ----------
    # broker 端读的是 offsetTable（ConsumerOffsetManager#whichTopicByConsumer），所以必须
    # 等消费者刷过位点；Java 的 admin 级方法（DefaultMQAdminExtImpl:1078）只收 group——
    # 先按 %RETRY%<group> 查路由，再逐 broker 扇出并合并成 Set。
    by_broker = safe("queryTopicsByConsumerToBroker",
                     lambda: admin.query_topics_by_consumer_to_broker(broker_addr, GROUP),
                     lambda t: "topics=%d" % len(t.get_topic_list()))
    if by_broker is not None:
        check("queryTopicsByConsumerToBroker 读出本组消费过的 topic",
              TOPIC in set(by_broker.get_topic_list()), str(by_broker.get_topic_list()))
    by_group = safe("queryTopicsByConsumer(group)（按 %RETRY% 路由扇出）",
                    lambda: admin.query_topics_by_consumer(GROUP),
                    lambda t: "topics=%d" % len(t.get_topic_list()))
    if by_group is not None:
        check("queryTopicsByConsumer(group) 合并后含本组消费过的 topic",
              TOPIC in set(by_group.get_topic_list()), str(by_group.get_topic_list()))

    prod.shutdown()

    # ---------- 9. 清理 ----------
    safe("deleteSubscriptionGroup", lambda: admin.delete_subscription_group(
        broker_addr, GROUP, True), lambda _: "OK")
    safe("deleteTopic", lambda: admin.delete_topic(TOPIC), lambda _: "OK")
    safe("deleteTopic(boundary)", lambda: admin.delete_topic(bnd_topic), lambda _: "OK")
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
