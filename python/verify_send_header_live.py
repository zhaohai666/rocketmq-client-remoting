# -*- coding: utf-8 -*-
"""发送头三个字段（``defaultTopic`` / ``defaultTopicQueueNums`` / ``brokerName``）真机验证。

离线单测（``tests/test_send_header_fields.py``）锁的是**上线形状**：V2 头的单字母键
``c`` / ``d`` / ``n`` 有没有真的写进 extFields、五种发送入口是不是带同一份值。但「字段
上了线」和「broker 真拿它做了决定」是两件事，后者只有真集群能证：

  H1  默认值：不带任何配置发到新 topic，broker 按 ``min(d=4, TBW102.writeQueueNums)``
      建出队列数（``TopicConfigManager:289``），与 Java 客户端默认行为一致。
  H2  ``set_default_topic_queue_nums(2)`` 真的生效：建出来的 topic 只有 2 条队列。
      修之前写死 4，这条必然变成 4 —— 这是那个假 setter 唯一可观测的后果。
  H3  ``set_create_topic_key(src)`` 真的生效：先建一个带 PERM_INHERIT、3 条队列的模板
      topic，再以它为 ``c`` 发送 → 新 topic 继承模板的 3 条队列（而不是 TBW102 的）。
      ``c`` 被写死时这里是 TBW102 的队列数，与 3 必然不同（H0 会先把这个前提断言掉）。
  H4  补上这三个字段之后，五种发送入口（同步 / 定点 / 批量 320 / 单向 / 异步）在真 broker
      上仍然逐条落地，条数一条不差。
  H5  ``n``（brokerName）：落点就是路由选中的那台 broker 名，且带 ``n`` 的请求 broker
      照单全收。⚠ 经典 broker 的发送链路里**没有** ``requestHeader.getBrokerName()``
      的读者（5.5.1 源码 grep 过），所以 ``n`` 的线上存在只能由离线抓帧证明，
      这里不假装能观测到它。

前置：NameServer + Broker 已起，``autoCreateTopicEnable=true``。

用法：.venv/bin/python verify_send_header_live.py [127.0.0.1:9876]
"""
from __future__ import annotations

import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rocketmq.client.admin import DefaultMQAdminExt
from rocketmq.client.producer import DefaultMQProducer, SendCallback
from rocketmq.client.send_result import SendStatus
from rocketmq.common.message import Message
from rocketmq.common.mix_all import MixAll
from rocketmq.common.sysflag import PermName

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time())

results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print("[%s] %s %s" % ("PASS" if ok else "FAIL", name, detail))


def _wait_until(pred, timeout_sec=20.0, interval=0.3):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


def _producer(instance_name, create_topic_key=None, default_topic_queue_nums=None):
    p = DefaultMQProducer("PID_send_header_%d" % STAMP)
    p.set_namesrv_addr(NAMESRV)
    p.set_instance_name(instance_name)
    if create_topic_key is not None:
        p.set_create_topic_key(create_topic_key)
    if default_topic_queue_nums is not None:
        p.set_default_topic_queue_nums(default_topic_queue_nums)
    return p


def _queue_nums(admin, broker_addr, topic):
    """读回 broker 上该 topic 的 (read, write) 队列数；还不存在返回 (None, None)。"""
    try:
        cfg = admin.examine_topic_config(broker_addr, topic)
    except Exception:  # noqa: BLE001
        return None, None
    return cfg.read_queue_nums, cfg.write_queue_nums


def _total_messages(admin, topic):
    """该 topic 全 broker 的落库条数（新 topic 的 minOffset 恒为 0）。"""
    try:
        stats = admin.examine_topic_stats(topic)
    except Exception:  # noqa: BLE001
        return -1
    return sum(o.max_offset - o.min_offset for o in stats.offset_table.values())


class _Latch(SendCallback):
    def __init__(self):
        self.done = threading.Event()
        self.result = None
        self.error = None

    def on_success(self, send_result):
        self.result = send_result
        self.done.set()

    def on_exception(self, e):
        self.error = e
        self.done.set()


def main():
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.set_timeout_millis(10000)
    admin.start()

    cluster = None
    for _ in range(40):
        try:
            ci = admin.fetch_broker_cluster_info()
            if ci and ci.broker_addr_table:
                cluster = ci
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1)
    if cluster is None:
        check("集群探活", False, "nameServer 无 broker 注册")
        admin.shutdown()
        return 1
    broker_addr = cluster.get_broker_addrs()[0]
    broker_name = next((name for name, ids in cluster.broker_addr_table.items()
                        if broker_addr in ids.values()), "")
    check("集群探活", True, "broker=%s" % broker_addr)

    topics = []
    # H0：TBW102 是这个 broker 上真正的「模板 topic」，H2/H3 的判据都依赖它的队列数
    tbw_read, tbw_write = _queue_nums(admin, broker_addr, MixAll.DEFAULT_TOPIC)
    if tbw_write is None:
        check("H0 TBW102 模板可读", False, "examine_topic_config(TBW102) 读不到")
        admin.shutdown()
        return 1
    check("H0 TBW102 模板可读", tbw_write > 0, "read=%s write=%s" % (tbw_read, tbw_write))

    # ---------------- H1 默认值：d=4，建出来的 topic 取 min(4, TBW102) ----------------
    t1 = "HdrDef_%d" % STAMP
    topics.append(t1)
    p1 = _producer("hdr-h1-%d" % STAMP)
    p1.start()
    try:
        r1 = p1.send(Message(t1, b"h1"))
        check("H1 默认配置发送成功", r1.send_status == SendStatus.SEND_OK,
              "msgId=%s mq=%s" % (r1.msg_id, r1.message_queue))
    finally:
        p1.shutdown()
    _wait_until(lambda: _queue_nums(admin, broker_addr, t1)[0] is not None, 10)
    h1_read, h1_write = _queue_nums(admin, broker_addr, t1)
    check("H1 默认 d=4 建的 topic 队列数=min(4, TBW102)",
          h1_write == min(4, tbw_write) and h1_read == h1_write,
          "read=%s write=%s (TBW102=%s)" % (h1_read, h1_write, tbw_write))

    # ---------------- H2 set_default_topic_queue_nums(2) 真的生效 ----------------
    t2 = "HdrNums_%d" % STAMP
    topics.append(t2)
    p2 = _producer("hdr-h2-%d" % STAMP, default_topic_queue_nums=2)
    p2.start()
    try:
        p2.send(Message(t2, b"h2"))
    finally:
        p2.shutdown()
    _wait_until(lambda: _queue_nums(admin, broker_addr, t2)[0] is not None, 10)
    h2_read, h2_write = _queue_nums(admin, broker_addr, t2)
    check("H2 defaultTopicQueueNums=2 建的 topic 只有 2 条队列",
          h2_write == min(2, tbw_write) and h2_read == h2_write,
          "read=%s write=%s（写死 4 的旧行为会是 %s）" % (h2_read, h2_write, h1_write))

    # ---------------- H3 set_create_topic_key(src) 真的生效 ----------------
    # 模板 topic 必须带 PERM_INHERIT，否则 TopicConfigManager:286 的 isInherited
    # 不通过，broker 直接拒绝自动建 topic。
    src = "HdrSrc_%d" % STAMP
    topics.append(src)
    admin.create_topic_in_broker(broker_addr, src, 3, 3,
                                 PermName.PERM_READ | PermName.PERM_WRITE | PermName.PERM_INHERIT)
    src_read, src_write = _queue_nums(admin, broker_addr, src)
    check("H3 模板 topic 建好（3 条队列、带 INHERIT）",
          src_write == 3 and (src_read, src_write) != (tbw_read, tbw_write),
          "src=(%s,%s) tbw102=(%s,%s)" % (src_read, src_write, tbw_read, tbw_write))

    t3 = "HdrSrc_%d" % STAMP
    topics.append(t3)
    p3 = _producer("hdr-h3-%d" % STAMP, create_topic_key=src, default_topic_queue_nums=8)
    p3.start()
    try:
        p3.send(Message(t3, b"h3"))
    finally:
        p3.shutdown()
    _wait_until(lambda: _queue_nums(admin, broker_addr, t3)[0] is not None, 10)
    h3_read, h3_write = _queue_nums(admin, broker_addr, t3)
    check("H3 createTopicKey=模板 topic 时被继承（min(8,3)=3 而不是 TBW102 的 %s）" % tbw_write,
          h3_write == 3 and h3_read == 3,
          "read=%s write=%s" % (h3_read, h3_write))

    # ---------------- H4 五种入口都照常落地 ----------------
    t4 = "HdrEntries_%d" % STAMP
    topics.append(t4)
    p4 = _producer("hdr-h4-%d" % STAMP)
    p4.start()
    latch = _Latch()
    landed = {}
    try:
        rs = p4.send(Message(t4, b"h4-sync"))
        landed["sync"] = rs.send_status == SendStatus.SEND_OK

        # 定点发送：显式给 mq，落在 H1 之外另一条路径（broker 名由调用方给）
        mq = rs.message_queue
        rb = p4.send(Message(t4, b"h4-pinned"), mq=mq)
        landed["pinned"] = (rb.send_status == SendStatus.SEND_OK
                            and rb.message_queue.broker_name == mq.broker_name)

        p4.send_oneway(Message(t4, b"h4-oneway"))
        landed["oneway"] = True      # 单向没有应答，只能靠 H4 的总数兜底

        batch = [Message(t4, b"h4-batch-%d" % i) for i in range(3)]
        rr = p4.send(batch)
        landed["batch"] = rr.send_status == SendStatus.SEND_OK

        p4.send_async(Message(t4, b"h4-async"), latch, 5000)
        landed["async"] = (latch.done.wait(10) and latch.error is None
                           and latch.result is not None
                           and latch.result.send_status == SendStatus.SEND_OK)
    finally:
        p4.shutdown()

    for entry in ("sync", "pinned", "oneway", "batch", "async"):
        check("H4 %s 入口发送成功" % entry, landed.get(entry, False),
              "" if landed.get(entry) else "该入口没有拿到 SEND_OK")
    # 1 同步 + 1 定点 + 1 单向 + 3 批量 + 1 异步 = 7 条
    got = _wait_until(lambda: _total_messages(admin, t4) >= 7, 20)
    total = _total_messages(admin, t4)
    check("H4 七条消息逐条落库（批量按子消息计）", got and total == 7,
          "total=%s ok=%s" % (total, landed))

    # ---------------- H5 brokerName 落在选中的那台 ----------------
    r5 = None
    p5 = _producer("hdr-h5-%d" % STAMP)
    p5.start()
    try:
        t5 = "HdrBrokerName_%d" % STAMP
        topics.append(t5)
        r5 = p5.send(Message(t5, b"h5"))
    finally:
        p5.shutdown()
    check("H5 落点 broker 名与路由一致（n 就是它）",
          r5 is not None and r5.send_status == SendStatus.SEND_OK
          and r5.message_queue.broker_name == broker_name,
          "落点=%s 路由=%s" % (getattr(r5.message_queue, "broker_name", None) if r5 else None,
                               broker_name))

    # ---------------- 清理 ----------------
    for t in topics:
        try:
            admin.delete_topic(t)
        except Exception as e:  # noqa: BLE001
            print("    (delete_topic(%s) 失败: %s)" % (t, e))
    admin.shutdown()

    passed = sum(1 for _, ok, _ in results if ok)
    print("\n== 结果: %d/%d 通过 ==" % (passed, len(results)))
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
