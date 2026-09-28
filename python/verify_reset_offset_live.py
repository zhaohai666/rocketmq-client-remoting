# -*- coding: utf-8 -*-
"""220 RESET_CONSUMER_CLIENT_OFFSET（Java ``MQClientInstance.resetOffset:1403-1450``）真机验证。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_reset_offset_live.py 127.0.0.1:9876

220 是 broker 推给**消费端**的重置指令，管理端那笔 INVOKE_BROKER_TO_RESET_OFFSET(222)
的响应只是一张"每个队列重置到哪"的表；真正让消费端改位点的是 broker 随后 oneway 推的
220（``Broker2Client.resetOffset:181-238``）。而 broker 只在
``useServerSideResetOffset=false`` 时才走这条推送路径（默认 true 在
``AdminBrokerProcessor:2255-2263`` 直接服务端改位点、一台消费端都不通知），所以本验证
先把该开关热改成 false（回读确认），跑完还原。

离线单测（``tests/test_reset_offset_handler.py``）锁死了请求体两种形状与"撤队列 +
代号 +1 + 新位点经撤销尾巴落盘"的本地状态；下面这些事只有真集群能证明：

  S1 「回退重置立刻生效 + 在途批次作废」——位点从 10 往回重置到 3。三段判据：
      a) broker 上的已提交位点在 ~2s 内变成 3：**只有**重置路径那次 persist 会写它
         （周期落盘已拉长到 60s），只写内存表的实现在这里原地不动（broker 停在 10）。
      b) listener 里卡着的旧批次（重置前取回的 offset 10）放行后，它的 ack 必须整批作废
         —— 采样点：放行旧批次、新队列的**第一批**已进 listener 且还没 ack 时，本地已消费
         位点必须还是"没有记录/≤3"；没有代号闸门的实现这时会跳到 11。
      c) 队列被真正重建：3..14 每一批都**重投一次**（旧缓冲里没 ack 的 11..14 也随之
         作废，只能作为重投的一部分出现），放行一轮断言一轮。

  S2 「前跳 + 恢复」——timestamp=-1 重置到 maxOffset：位点直接跳到 10，中间 4..9 一条都不
      投；随后新消息照常消费、位点继续前进。

为什么不把时序判断塞进离线单测：这里每一步都依赖真 broker 的推送与 offset 表，而"重置
生效"的失败模式在真机上只表现为"位点没动、消息照旧不重投"，admin 侧完全看不出异常。
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from rocketmq.client.admin import DefaultMQAdminExt
from rocketmq.client.consumer import (ConsumeConcurrentlyStatus,
                                      DefaultMQPushConsumer,
                                      MessageListenerConcurrently)
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message
from rocketmq.remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC1 = "ResetOffsetBack_%d" % STAMP
GROUP1 = "GID_ResetOffsetBack_%d" % STAMP
TOPIC2 = "ResetOffsetSkip_%d" % STAMP
GROUP2 = "GID_ResetOffsetSkip_%d" % STAMP
MSGS = 10
BACK_TARGET = 3

PASS = 0
FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print("  [PASS] %s%s" % (name, ("  " + detail) if detail else ""))
    else:
        FAIL += 1
        print("  [FAIL] %s%s" % (name, ("  " + detail) if detail else ""))


def new_client(tag: str) -> MQClientInstance:
    c = MQClientInstance("%s-%d" % (tag, STAMP), [NAMESRV])
    c.start()
    return c


def wait_until(pred, timeout: float, interval: float = 1.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return pred()


def committed(client: MQClientInstance, group: str, topic: str):
    """[(MessageQueue, maxOffset, committed or None)]，committed=None 表示 broker 上查无记录。"""
    publish = client.get_topic_publish_info(topic)
    if publish is None or not publish.msg_queue_list:
        return None
    out = []
    for mq in publish.msg_queue_list:
        try:
            off = client.query_consumer_offset(group, mq, set_zero_if_not_found=False)
        except Exception:  # noqa: BLE001
            off = None
        try:
            max_off = client.get_max_offset(mq)
        except Exception:  # noqa: BLE001
            max_off = -1
        out.append((mq, max_off, off))
    return out


def fmt_rows(rows) -> str:
    return " ".join("q%d:%s/max%d" % (mq.queue_id, o, m) for mq, m, o in rows)


class SteppingListener(MessageListenerConcurrently):
    """每一批都停在闸门上，由测试逐批放行 —— 批与批之间的本地状态因此可以被采样。

    S1 的关键采样点（旧批次已放行、新队列第一批还没 ack）只有在"逐批可控"时才存在；
    一次性放行的 listener 会把 11（旧批次 ack）与随后的重投混在一个瞬间里。
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._batches = []
        self._release = threading.Event()

    def consume_message(self, msgs, context):
        with self._lock:
            self._batches.append([m.queue_offset for m in msgs])
        # 上限只防死锁：正常路径由测试逐批 release()
        self._release.wait(90)
        self._release.clear()
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def wait_batch(self, n: int, timeout: float = 20.0) -> bool:
        return wait_until(lambda: len(self.batches()) >= n, timeout, 0.05)

    def release(self) -> None:
        self._release.set()

    def batches(self):
        with self._lock:
            return [list(b) for b in self._batches]

    def offsets(self):
        with self._lock:
            return [o for batch in self._batches for o in batch]


class RecordingListener(MessageListenerConcurrently):
    def __init__(self):
        self._offsets = []
        self._lock = threading.Lock()

    def consume_message(self, msgs, context):
        with self._lock:
            self._offsets.extend(m.queue_offset for m in msgs)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def offsets(self):
        with self._lock:
            return list(self._offsets)


def consumer_key(c: DefaultMQPushConsumer, topic: str):
    """消费者视角下该 topic 的队列 key（用 _mq_map 的键，避免自己拼 brokerName）。"""
    with c._lock:
        for key, mq in c._mq_map.items():
            if mq.topic == topic:
                return key
    return None


def local_offset(c: DefaultMQPushConsumer, topic: str):
    """本端口内存里的「已消费位点」；返回 None = 该队列在表里没有记录。

    220 的重置语义（Java removeOffset）就是"重置后表里没有旧位点"，所以 None 与 0 必须
    能分开：只有 None 才能证明旧批次的 ack 没有把位点推回去。
    """
    status = c.get_consumer_status(topic)
    for mq, off in status.items():
        if mq.queue_id == 0:
            return off
    return None


def wait_broker_addr(admin: DefaultMQAdminExt, timeout: float = 40.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            ci = admin.fetch_broker_cluster_info()
            if ci and ci.broker_addr_table:
                return ci.get_broker_addrs()[0]
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1)
    return None


def set_server_side_reset(admin: DefaultMQAdminExt, broker_addr: str, value: str) -> bool:
    admin.update_broker_config(broker_addr, {"useServerSideResetOffset": value})
    time.sleep(1)
    cfg = admin.get_broker_config(broker_addr)
    return cfg.get("useServerSideResetOffset") == value


def start_consumer(group: str, topic: str, listener, batch_size: int, persist_ms: int):
    c = DefaultMQPushConsumer(group)
    c.set_namesrv_addr(NAMESRV)
    c.set_message_listener(listener)
    c.consume_message_batch_max_size = batch_size
    if persist_ms > 0:
        c.persist_consumer_offset_interval = persist_ms
    c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c.subscribe(topic, "*")
    c.start()
    return c


# ------------------------------------------------ S1：回退重置 + 在途批次作废


def s1_backward_reset(setup: MQClientInstance, admin: DefaultMQAdminExt,
                      broker_addr: str, producer: DefaultMQProducer) -> None:
    print("\n--- S1：回退重置（10 → 3）立刻落盘 + 在途批次整批作废 ---")
    # 前置：先用普通 listener 把 broker 上的位点做成 10（reset 要求该组在 broker 上有记录）
    l0 = RecordingListener()
    c0 = start_consumer(GROUP1, TOPIC1, l0, 1, 0)
    time.sleep(1)
    t_mid = 0
    for i in range(MSGS):
        producer.send(Message(TOPIC1, b"rb-%d" % i))
        if i == 2:
            t_mid = int(time.time() * 1000)
            # 让第 3 条（offset 3）与前面三条拉开存储时间，后面按时间戳重置才能稳定命中 3
            time.sleep(2)
    ok = wait_until(lambda: len(l0.offsets()) == MSGS, 30, 0.2)
    check("S1-前置：消费者消费掉 %d 条" % MSGS, ok, "arrivals=%s" % l0.offsets())

    def broker_is(n):
        rows = committed(setup, GROUP1, TOPIC1)
        return bool(rows) and all(o == n for _, _, o in rows)

    ok = wait_until(lambda: broker_is(MSGS), 20, 0.5)
    check("S1-前置：broker 位点周期落盘到 %d（reset 要求组在 broker 上有记录）" % MSGS, ok,
          fmt_rows(committed(setup, GROUP1, TOPIC1) or []))
    c0.shutdown()
    time.sleep(1)

    # 主力消费者：逐批闸住 + 周期落盘 60s ⇒ 窗口内唯一能改 broker 位点的路径是重置自带的那次
    listener = SteppingListener()
    c = start_consumer(GROUP1, TOPIC1, listener, 1, 60000)
    t_start = time.time()
    time.sleep(2)
    for i in range(MSGS, MSGS + 5):
        producer.send(Message(TOPIC1, b"rb-%d" % i))
    key = None

    def arranged():
        nonlocal key
        k = consumer_key(c, TOPIC1)
        if k is None:
            return False
        key = k
        return len(listener.batches()) >= 1 and len(c._pending.get(k, ())) == 4

    ok = wait_until(arranged, 30, 0.05)
    check("S1-窗口就绪：1 条在途（offset 10 卡在 listener）+ 4 条留在缓冲",
          ok and listener.offsets()[:1] == [10],
          "arrivals=%s pending=%s" % (listener.offsets(),
                                      len(c._pending.get(key, ())) if key else "?"))
    if not ok or key is None:
        listener.release()
        c.shutdown()
        return

    # 等周期落盘的**首跳**过去（Java initialDelay＝start 后 10s，之后才是 60s 周期）。
    # 不等它，重置后那次 persist 会与首跳混在一起，"broker 位点之所以是 3" 就说不清。
    wait_until(lambda: time.time() - t_start > 12, 15, 0.5)
    check("S1-首跳周期落盘已过（此后 60s 内不再有周期写）",
          time.time() - t_start > 12 and broker_is(MSGS),
          fmt_rows(committed(setup, GROUP1, TOPIC1) or []))

    # 按时间戳重置到 3：t_mid 落在第 3 条与第 4 条之间 ⇒ getOffsetInQueueByTime 命中 3，
    # 3 < consumerOffset(10) 且 isForce=true ⇒ broker 推 {mq: 3}
    reset_ts = t_mid + 500
    t0 = time.time()
    table = admin.reset_offset_by_timestamp(TOPIC1, GROUP1, reset_ts, True)
    check("S1-222 响应里的目标位点就是 3", [v for v in table.values()] == [BACK_TARGET],
          "table=%s" % {str(k): v for k, v in table.items()})

    ok = wait_before(lambda: broker_is(BACK_TARGET), 2.0)
    elapsed = time.time() - t0
    check("S1-broker 位点 %.1fs 内变成 %d（重置路径自带的那次 persist，周期落盘=60s）"
          % (elapsed, BACK_TARGET), ok, fmt_rows(committed(setup, GROUP1, TOPIC1) or []))

    off = local_offset(c, TOPIC1)
    check("S1-重置后本地表里没有旧位点（Java removeOffset；新位点经撤销尾巴出去）",
          off is None, "local_offset=%s epoch=%d" % (off, c._queue_epoch.get(key, 0)))

    # 放行旧批次：它的 ack 属于已被撤销的 ProcessQueue，必须作废
    listener.release()
    ok = listener.wait_batch(2, 20.0)
    off = local_offset(c, TOPIC1)
    check("S1-旧批次 ack 作废（放行后新队列第一批 offset %d 已在途时，本地位点仍未越过 3）"
          % (listener.batches()[1][0] if ok and len(listener.batches()) > 1 else -1),
          ok and (off is None or off <= BACK_TARGET),
          "local_offset=%s arrivals=%s" % (off, listener.offsets()))

    # 逐批放行走完重投：3..14 每条重投一次（旧缓冲里 11..14 也已作废，只能作为重投出现）
    expected = [10] + list(range(BACK_TARGET, MSGS + 5))
    for n in range(2, len(expected) + 1):
        listener.release()
        if n + 1 <= len(expected):
            listener.wait_batch(n + 1, 15.0)
    arr = listener.offsets()
    check("S1-队列被真正重建：重投序列是 %s" % expected, arr == expected, "arrivals=%s" % arr)
    wait_until(lambda: local_offset(c, TOPIC1) == MSGS + 5, 10, 0.05)

    rows = committed(setup, GROUP1, TOPIC1) or []
    check("S1-窗口内只有重置那次写 broker（位点仍停在 %d，周期落盘=60s）" % BACK_TARGET,
          bool(rows) and all(o == BACK_TARGET for _, _, o in rows), fmt_rows(rows))

    # 关停会同步落盘一次：重投的 ack 才是最终值（15 = 最后一条 14 的 +1）
    c.shutdown()
    ok = wait_until(lambda: broker_is(MSGS + 5), 15, 0.5)
    check("S1-关停落盘把重投的 ack 写回 broker（位点前进到 %d）" % (MSGS + 5), ok,
          fmt_rows(committed(setup, GROUP1, TOPIC1) or []))


def wait_before(pred, seconds: float, interval: float = 0.05) -> bool:
    """在 seconds 秒内轮询（用于"很快就要发生"的断言，比 wait_until 采样密）。"""
    deadline = time.time() + seconds
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return pred()


# ------------------------------------------------ S2：前跳重置 + 恢复


def s2_forward_skip(setup: MQClientInstance, admin: DefaultMQAdminExt,
                    broker_addr: str, producer: DefaultMQProducer) -> None:
    print("\n--- S2：前跳重置（timestamp=-1 → maxOffset）不重投 + 新消息照常消费 ---")
    listener = SteppingListener()
    # 周期落盘用默认 5s：本场景要先靠首跳（start 后 10s）在 broker 上给这个组建记录
    #（Broker2Client.resetOffset 对 queryOffset==-1 的组直接回 SYSTEM_ERROR），
    # 再等一个 >5s 的窗口做重置 —— 重置那笔 persist 与周期写不同刻，判据仍然干净。
    c = start_consumer(GROUP2, TOPIC2, listener, 1, 0)
    t_start = time.time()
    time.sleep(2)
    for i in range(MSGS):
        producer.send(Message(TOPIC2, b"rs-%d" % i))

    key = None

    def arranged():
        nonlocal key
        k = consumer_key(c, TOPIC2)
        if k is None:
            return False
        key = k
        return len(listener.batches()) >= 4

    # 逐批放行前 3 条：第 4 条（offset 3）留在 listener 里当"在途批次"
    for n in range(1, 5):
        if not listener.wait_batch(n, 30.0):
            break
        if n < 4:
            listener.release()
    ok = arranged()
    check("S2-窗口就绪：前 3 条已 ack、第 4 条卡在 listener",
          ok and listener.offsets()[:4] == [0, 1, 2, 3], "arrivals=%s" % listener.offsets())
    if not ok or key is None:
        listener.release()
        c.shutdown()
        return

    def broker_is(n):
        rows = committed(setup, GROUP2, TOPIC2)
        return bool(rows) and all(o == n for _, _, o in rows)

    # 等首跳落盘把组建出来（本地已消费位点 3 = 前三条的 ack），并留出 >5s 的静默窗口
    ok = wait_until(lambda: time.time() - t_start > 12 and broker_is(3), 20, 0.5)
    check("S2-前置：broker 上该组有记录（q0:3），且下一笔周期写还在 5s 之外", ok,
          fmt_rows(committed(setup, GROUP2, TOPIC2) or []))

    t0 = time.time()
    table = admin.reset_offset_by_timestamp(TOPIC2, GROUP2, -1, True)
    check("S2-222 响应里的目标位点就是 maxOffset(%d)" % MSGS,
          [v for v in table.values()] == [MSGS],
          "table=%s" % {str(k): v for k, v in table.items()})

    ok = wait_before(lambda: broker_is(MSGS), 2.0)
    elapsed = time.time() - t0
    check("S2-broker 位点 %.1fs 内前跳到 %d（重置路径自带的那次 persist）" % (elapsed, MSGS), ok,
          fmt_rows(committed(setup, GROUP2, TOPIC2) or []))

    listener.release()
    time.sleep(3)
    check("S2-被跳过的 4..%d 一条都不投（在途那条的 ack 也作废）" % (MSGS - 1),
          listener.offsets() == [0, 1, 2, 3], "arrivals=%s" % listener.offsets())

    producer.send(Message(TOPIC2, b"rs-new"))
    ok = listener.wait_batch(5, 20.0)
    listener.release()
    check("S2-重建后的队列从队尾续跑（新消息 offset %d 正常投递）" % MSGS,
          ok and listener.offsets() == [0, 1, 2, 3, MSGS], "arrivals=%s" % listener.offsets())

    # release() 只让 listener 返回；ack 是消费线程随后落的。不等本地位点真的推到 11 就
    # 关停，关停那次 persist 可能跑在 ack 之前（写回的还是 10）——这是断言竞态，不是语义问题。
    wait_until(lambda: local_offset(c, TOPIC2) == MSGS + 1, 10, 0.05)
    c.shutdown()
    ok = wait_until(lambda: broker_is(MSGS + 1), 15, 0.5)
    check("S2-关停落盘把新消息的 ack 写回 broker（位点前进到 %d）" % (MSGS + 1), ok,
          fmt_rows(committed(setup, GROUP2, TOPIC2) or []))


def cleanup(admin: DefaultMQAdminExt, broker_addr: str) -> None:
    for topic in (TOPIC1, TOPIC2):
        try:
            admin.delete_topic(topic)
        except Exception as e:  # noqa: BLE001
            print("  (cleanup) delete topic %s failed: %s" % (topic, e))
    for group in (GROUP1, GROUP2):
        try:
            admin.delete_subscription_group(broker_addr, group, True)
        except Exception as e:  # noqa: BLE001
            print("  (cleanup) delete group %s failed: %s" % (group, e))


def main() -> int:
    setup = new_client("setup")
    setup.create_topic_in_route(TOPIC1, 1, 1)
    setup.create_topic_in_route(TOPIC2, 1, 1)
    print("TOPIC1=%s TOPIC2=%s（各 1 队列，各 %d 条消息）" % (TOPIC1, TOPIC2, MSGS))

    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.set_timeout_millis(10000)
    admin.start()
    broker_addr = wait_broker_addr(admin)
    if broker_addr is None:
        check("集群探活", False, "nameServer 上无 broker 注册")
        admin.shutdown()
        setup.shutdown()
        return 1
    print("broker_addr=%s" % broker_addr)

    producer = DefaultMQProducer("ResetOffsetLive_pg_%d" % STAMP)
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    try:
        check("开关热改：useServerSideResetOffset=false（220 推送路径的前提）",
              set_server_side_reset(admin, broker_addr, "false"),
              "回读 useServerSideResetOffset=%s"
              % admin.get_broker_config(broker_addr).get("useServerSideResetOffset"))
        s1_backward_reset(setup, admin, broker_addr, producer)
        s2_forward_skip(setup, admin, broker_addr, producer)
        cleanup(admin, broker_addr)
    finally:
        try:
            ok = set_server_side_reset(admin, broker_addr, "true")
            check("还原 useServerSideResetOffset=true", ok)
        except Exception as e:  # noqa: BLE001
            check("还原 useServerSideResetOffset=true", False, str(e))
        producer.shutdown()
        admin.shutdown()
        setup.shutdown()

    print("\nreset-offset live: %d passed, %d failed" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
