# -*- coding: utf-8 -*-
"""OFFSET_ILLEGAL 纠错分支（Java ``DefaultMQPushConsumerImpl:402-427``）真机验证。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_offset_illegal_live.py 127.0.0.1:9876

这条分支做四件事：位点改用 broker 给的修正值（``setNextOffset``）→ 丢掉这条队列上
已取回未消费的消息（``ProcessQueue.setDropped(true)``）→ 把修正位点**立刻**落盘
（``updateAndFreezeOffset`` + ``persist``）→ 撤掉队列让 rebalance 按修正位点重建
（``removeProcessQueue`` + ``rebalanceImmediately``）。离线单测只能锁住本地状态怎么清、
哪个 ack 被作废，下面两件事只有真集群能证明：

  S1 「丢队列」——broker 判定位点非法时，队列上**已取回但还没消费/没 ack** 的消息必须
      整批作废。做法：让 listener 卡住第一条（在途 1 条、缓冲里 2 条），再用
      ``resetOffsetByQueueId`` 把位点重置到 3（服务端重置 ⇒ 下一笔 pull 被
      ``PullMessageProcessor:539-545`` 短路成 OFFSET_RESET ⇒ 客户端 OFFSET_ILLEGAL）。
      修复前：缓冲里的第 1、2 条照常投递（listener 实收 3 条）；修复后：只剩在途的
      第 0 条，且它的 ack 因队列已被丢（Java ``ConsumeMessageConcurrentlyService:267``）
      而作废。最后再发第 4 条，验证重建后的队列从修正位点续跑、冻结已随重建解除
      （新消息的 ack 让 broker 上的位点继续前进到 4）。

  S2 「立刻落盘」——纠错后的位点必须马上推给 broker，不能等周期落盘。做法：利用
      ``resetOffsetByQueueId`` 两笔 RPC 非原子（第 1 笔 commitOffset 无区间校验先落库、
      第 2 笔 222 被 ``resetOffsetInner`` 拒绝，见 ``verify_admin_live.py`` 9.5 的实测）
      的既有行为，把 broker 上的已提交位点做成非法值 103，再让一个
      ``persist_consumer_offset_interval=60000`` 的新消费者从 103 起拉。窗口内唯一能把
      103 写回 3（maxOffset）的路径就是纠错分支自带的那次 persist，且全程零投递。
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from client.admin import DefaultMQAdminExt
from client.consumer import (ConsumeConcurrentlyStatus,
                                      DefaultMQPushConsumer,
                                      MessageListenerConcurrently)
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from common.message import Message
from remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC1 = "OffsetIllegalDrop_%d" % STAMP
GROUP1 = "GID_OffsetIllegalDrop_%d" % STAMP
TOPIC2 = "OffsetIllegalPersist_%d" % STAMP
GROUP2 = "GID_OffsetIllegalPersist_%d" % STAMP
MSGS = 3
ILLEGAL_TARGET = 103

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


class GatedListener(MessageListenerConcurrently):
    """第 1 批卡在闸门上（维持"1 条在途 + 其余在缓冲"的窗口），放行后照常返回。"""

    def __init__(self):
        self.batches = []
        self._lock = threading.Lock()
        self.release = threading.Event()

    def consume_message(self, msgs, context):
        with self._lock:
            self.batches.append([m.queue_offset for m in msgs])
        # 上限只防死锁：正常路径下由 release 显式放行，且必须晚于纠错（见 s1 的时序说明）
        self.release.wait(90)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def offsets(self):
        with self._lock:
            return [o for batch in self.batches for o in batch]


class RecordingListener(MessageListenerConcurrently):
    def __init__(self):
        self.offsets_ = []
        self._lock = threading.Lock()

    def consume_message(self, msgs, context):
        with self._lock:
            self.offsets_.extend(m.queue_offset for m in msgs)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def offsets(self):
        with self._lock:
            return list(self.offsets_)


def consumer_key(c: DefaultMQPushConsumer, topic: str):
    """消费者视角下该 topic 的队列 key（用 _mq_map 的键，避免自己拼 brokerName）。"""
    with c._lock:
        for key, mq in c._mq_map.items():
            if mq.topic == topic:
                return key
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


def s1_drop_and_rebuild(setup: MQClientInstance, admin: DefaultMQAdminExt,
                        broker_addr: str, producer: DefaultMQProducer) -> None:
    print("\n--- S1：OFFSET_ILLEGAL 整批作废在途/缓冲消息并按修正位点重建 ---")
    c = DefaultMQPushConsumer(GROUP1)
    c.set_namesrv_addr(NAMESRV)
    listener = GatedListener()
    c.set_message_listener(listener)
    c.consume_message_batch_max_size = 1
    c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c.subscribe(TOPIC1, "*")
    c.start()
    time.sleep(1)
    for i in range(MSGS):
        msg = Message(TOPIC1, b"ilo-%d" % i)
        producer.send(msg)
    print("S1: 已发送 %d 条（单队列），等 listener 卡住第 1 条..." % MSGS)

    state = {}

    def arranged():
        key = consumer_key(c, TOPIC1)
        if key is None:
            return False
        state["key"] = key
        state["pending"] = len(c._pending.get(key, ()))
        state["inflight"] = c._msg_queue_inflight.get(key, 0)
        return state["pending"] == MSGS - 1 and state["inflight"] == 1

    ok = wait_until(arranged, 30, 0.2)
    key = state.get("key")
    check("S1-窗口就绪：1 条在途（listener 被闸住）+ %d 条留在缓冲" % (MSGS - 1), ok,
          "pending=%s inflight=%s arrivals=%s" % (state.get("pending"), state.get("inflight"),
                                                  listener.offsets()))
    if key is None:
        listener.release.set()
        c.shutdown()
        return

    # 服务端重置到 maxOffset：RPC2 写 resetOffsetTable，下一笔 pull 命中
    # PullMessageProcessor:539-545 被短路成 OFFSET_RESET（nextBeginOffset=修正值）
    admin.reset_offset_by_queue_id(broker_addr, GROUP1, TOPIC1, 0, MSGS)
    print("S1: 已发出 resetOffsetByQueueId(->%d)，等下一笔 pull 取走服务端重置..." % MSGS)

    # 发现延迟 = 客户端在途长轮询的返回时间 + broker 侧的巡检周期：本端口按下发
    # suspendTimeoutMillis=20000（Java PullAPIWrapper.brokerSuspendMaxTimeMillis 默认值
    # 20s）请求挂起，broker 的 PullRequestHoldService 每 5s 巡检一次到期请求，
    # 命中前那笔 pull 不会重读 resetOffsetTable。实测 24.3s，与 Java 同构（长轮询语义
    # 如此，不是缺陷）——这里给 45s 余量。
    ok = wait_until(lambda: c._queue_epoch.get(key, 0) >= 1, 45, 0.2)
    check("S1-broker 判定位点非法后本端丢弃该队列（ProcessQueue.setDropped：代号 +1）", ok,
          "epoch=%s arrivals=%s" % (c._queue_epoch.get(key, 0), listener.offsets()))

    def committed_is(n):
        rows = committed(setup, GROUP1, TOPIC1)
        return bool(rows) and all(o == n for _, _, o in rows)

    ok = wait_until(lambda: committed_is(MSGS), 20, 0.5)
    rows = committed(setup, GROUP1, TOPIC1) or []
    check("S1-broker 上的位点停在修正值 %d（本场景两笔重置 RPC 已先写过一次，弱断言）" % MSGS,
          ok, " ".join("q%d:%s" % (mq.queue_id, o) for mq, _, o in rows))

    listener.release.set()
    time.sleep(6)
    arr = listener.offsets()
    check("S1-缓冲里已取回的 %d 条被整批作废（第 1、2 条永不投递）" % (MSGS - 1),
          arr and 1 not in arr and 2 not in arr, "arrivals=%s" % arr)

    msg = Message(TOPIC1, b"ilo-after")
    producer.send(msg)
    ok = wait_until(lambda: MSGS in listener.offsets(), 20, 0.5)
    arr = listener.offsets()
    check("S1-重建后的队列从修正位点续拉（第 %d 条新消息正常投递，历史拿过的不重投）"
          % MSGS, ok and 1 not in arr and 2 not in arr, "arrivals=%s" % arr)

    ok = wait_until(lambda: committed_is(MSGS + 1), 25, 1.0)
    rows = committed(setup, GROUP1, TOPIC1) or []
    check("S1-冻结随重建解除（新消息的 ack 让 broker 位点继续前进到 %d）" % (MSGS + 1),
          ok, "committed=%s" % [o for _, _, o in rows])
    c.shutdown()


def s2_immediate_persist(setup: MQClientInstance, admin: DefaultMQAdminExt,
                         broker_addr: str, producer: DefaultMQProducer) -> None:
    print("\n--- S2：纠错把修正位点立刻落盘（不等周期落盘） ---")
    listener = RecordingListener()
    c = DefaultMQPushConsumer(GROUP2)
    c.set_namesrv_addr(NAMESRV)
    c.set_message_listener(listener)
    c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c.subscribe(TOPIC2, "*")
    c.start()
    time.sleep(1)
    for i in range(MSGS):
        msg = Message(TOPIC2, b"ilo2-%d" % i)
        producer.send(msg)
    ok = wait_until(lambda: len(listener.offsets()) == MSGS, 30, 0.5)
    check("S2-前置：消费者先正常消费掉 %d 条" % MSGS, ok, "arrivals=%s" % listener.offsets())
    # shutdown 会同步 persist 一次（consumer.py:1522），此后 broker 上的位点是 3；
    # 必须先停掉它，否则它的周期/关停落盘会把下面种进去的非法值覆盖回去。
    c.shutdown()
    time.sleep(1)

    rejected = False
    remark = ""
    try:
        admin.reset_offset_by_queue_id(broker_addr, GROUP2, TOPIC2, 0, ILLEGAL_TARGET)
    except Exception as e:  # noqa: BLE001
        rejected = True
        remark = "%s: %s" % (type(e).__name__, str(e)[:140])
    check("S2-前置：越界目标被 resetOffsetInner 拒绝（第 2 笔 RPC）", rejected, remark)

    rows = committed(setup, GROUP2, TOPIC2) or []
    check("S2-前置：第 1 笔 commitOffset 已把非法位点 %d 落库（两笔 RPC 非原子）"
          % ILLEGAL_TARGET,
          bool(rows) and all(o == ILLEGAL_TARGET for _, _, o in rows),
          " ".join("q%d:%s/max%d" % (mq.queue_id, o, m) for mq, m, o in rows))

    c2 = DefaultMQPushConsumer(GROUP2)
    c2.set_namesrv_addr(NAMESRV)
    l2 = RecordingListener()
    c2.set_message_listener(l2)
    # 周期落盘拉长到 60s：窗口内唯一能改写 broker 位点的路径是纠错分支自带的立即 persist
    c2.persist_consumer_offset_interval = 60000
    c2.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c2.subscribe(TOPIC2, "*")
    t0 = time.time()
    c2.start()
    print("S2: 新消费者从非法位点 %d 起拉，等纠错把 broker 位点写回 %d（周期落盘=60s）..."
          % (ILLEGAL_TARGET, MSGS))

    def back_to_max():
        rows_ = committed(setup, GROUP2, TOPIC2)
        return bool(rows_) and all(o == MSGS for _, _, o in rows_)

    ok = wait_until(back_to_max, 20, 0.5)
    elapsed = time.time() - t0
    rows = committed(setup, GROUP2, TOPIC2) or []
    check("S2-broker 位点由 %d 纠回 %d（纠错分支自带的那次 persist）" % (ILLEGAL_TARGET, MSGS),
          ok, "elapsed=%.1fs committed=%s" % (elapsed, [o for _, _, o in rows]))

    # 再等一个静默窗口：位点被纠回后不会回头重投 0..2，也不会再被改写
    time.sleep(6)
    check("S2-全程零投递（修正位点落在历史消息之后，一条都不下发）",
          len(l2.offsets()) == 0, "arrivals=%s" % l2.offsets())
    check("S2-静默窗口后位点仍停在 %d" % MSGS, back_to_max(),
          "committed=%s" % [o for _, _, o in committed(setup, GROUP2, TOPIC2) or []])
    c2.shutdown()


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
    print("TOPIC1=%s TOPIC2=%s（各 1 队列，%d 条消息）" % (TOPIC1, TOPIC2, MSGS))

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

    producer = DefaultMQProducer("OffsetIllegalLive_pg_%d" % STAMP)
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    try:
        s1_drop_and_rebuild(setup, admin, broker_addr, producer)
        s2_immediate_persist(setup, admin, broker_addr, producer)
        cleanup(admin, broker_addr)
    finally:
        producer.shutdown()
        admin.shutdown()
        setup.shutdown()

    print("\noffset-illegal live: %d passed, %d failed" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
