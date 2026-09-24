#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""轻量拉取消费者（DefaultLitePullConsumer）真机验证。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_lite_pull_live.py 127.0.0.1:9876

为什么单独测：DefaultLitePullConsumer 是三个语言都缺失的能力（Java 有，本仓库此前三语言都只有
手动的 DefaultMQPullConsumer）。本脚本把它的核心——「subscribe/assign + 后台灌本地缓冲 + poll 取批量」——
在真实集群上跑通。

场景（围绕 Lite 相对 Pull 的本质区别：调用方不用管位点，poll 从本地缓冲拿消息）：
  S1 建 topic + subscribe 模式等待 rebalance 分到位点
  S2 先起消费者、再发 12 条（交替 TagA/TagB）→ subscribe + poll 收全 12 条且内容一致
  S3 auto-commit：只读 committed()，继续 poll 过一整个自动提交周期后位点自己 > 0
  S4 assign 模式：显式 assign 全部队列 + seek 到队首 → poll 重新收全 12 条（验证 assign/seek/poll）
  S5 订阅 TagA：subscribe(T, "TagA") 只收 TagA 的 6 条（验证订阅级 tag 过滤）
  S6 CONSUME_FROM_TIMESTAMP：consumeTimestamp 按 Java 的 14 位本地墙钟解释
     S6a 新组 + 起点=30 分钟前 → 收全 12 条
     S6b 墙钟→队列位置映射：30 分钟前 → 各队列队首（Σ=0）；10 分钟后 → 越过全部消息（Σ=12）
     旧实现把墙钟串当 epoch 解析会把两个方向同时翻转。
  S7 可插拔分配策略（对应 Java setAllocateMessageQueueStrategy）
     S7a 默认策略名 AVG；置 None 由 start() 按 Java checkConfig 拒绝
     S7b AVG_BY_CIRCLE：同组两实例把 4 个队列按下标取模交叉切开，不重不漏
     S7c CONFIG：只分配配置进去的队列 → assignment 恰为其一，且 poll 到的消息
         queueId 全部落在配置队列内、两半合起来覆盖全部 12 条且互不重叠
     S7d CONSISTENT_HASH：拿**真实 clientId** 建环，线上 assignment 必须收敛到
         「真实 mqAll/cidAll 离线跑同一策略」的预测。环可能一边 4 条一边 0 条
         （Java 同款落点偏斜），所以判定只看「不重不漏 + 等于预测」
     S7e MACHINE_ROOM_NEARBY：真实集群只有一个机房 ⇒ 装饰器必须原样透传内层策略；
         resolver 的调用记录同时证明 rebalance 真的逐个问过队列/客户端的机房
     S7f MACHINE_ROOM：真实 brokerName 不含 '@'，白名单再怎么写都筛不出队列 ——
         验的是「配错机房安静饿死」（分不到队列、poll 不到消息、不打崩重平衡）
  S8 三张位点表（对位 Java AssignedMessageQueue + OffsetStore）：1 队列 topic 灌 1200 条，
      把「拉取游标 / 已消费游标 / 提交落点」三个数字在真机各自数出来
"""
from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")

from rocketmq.client.consumer import (AllocateMachineRoomNearby,
                                      AllocateMessageQueueAveragely,
                                      AllocateMessageQueueAveragelyByCircle,
                                      AllocateMessageQueueByConfig,
                                      AllocateMessageQueueByMachineRoom,
                                      AllocateMessageQueueConsistentHash,
                                      DefaultLitePullConsumer, DefaultMQPullConsumer,
                                      MachineRoomResolver)
from rocketmq.client.exception import MQClientException
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.client.send_result import SendStatus
from rocketmq.common.message import Message, MessageQueue
from rocketmq.remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "LiteLive_%d" % STAMP
GROUP1 = "LitePG1_%d" % STAMP
GROUP2 = "LitePG2_%d" % STAMP
GROUP3 = "LitePG3_%d" % STAMP
GROUP4 = "LitePG4_%d" % STAMP
GROUP5 = "LitePG5_%d" % STAMP
GROUP6 = "LitePG6_%d" % STAMP
GROUP7 = "LitePG7_%d" % STAMP
GROUP8 = "LitePG8_%d" % STAMP
GROUP9 = "LitePG9_%d" % STAMP
GROUP10 = "LitePG10_%d" % STAMP   # S7d 一致性哈希环
GROUP11 = "LitePG11_%d" % STAMP   # S7e 同机房就近分配
GROUP12 = "LitePG12_%d" % STAMP   # S7f 机房白名单配错
N_MSG = 12
QUEUE_NUM = 4
TAG_A = {("lite-%02d" % i).encode() for i in range(0, N_MSG, 2)}   # 偶数下标 → TagA，共 6 条
TAG_B = {("lite-%02d" % i).encode() for i in range(1, N_MSG, 2)}   # 奇数下标 → TagB，共 6 条

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


def prepare_topic() -> None:
    prod = DefaultMQProducer("PG_Prepare_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    try:
        prod.create_topic("TBW102", TOPIC, QUEUE_NUM)
    finally:
        prod.shutdown()
    # 等路由注册（用 DefaultMQPullConsumer 探测即可，它不要求先订阅）
    deadline = time.time() + 20
    while time.time() < deadline:
        c = DefaultMQPullConsumer("PG_Probe_%d" % STAMP)
        c.set_namesrv_addr(NAMESRV)
        c.start()
        try:
            qs = c.fetch_subscribe_message_queues(TOPIC)
            if len(qs) >= QUEUE_NUM:
                return
        except BaseException:
            pass
        finally:
            c.shutdown()
        time.sleep(1)
    raise RuntimeError("topic not routable after 20s")


def produce() -> set:
    print("\nS2 生产 %d 条（交替 TagA/TagB）" % N_MSG)
    prod = DefaultMQProducer("PG_LiteLive_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    sent = set()
    try:
        for i in range(N_MSG):
            body = ("lite-%02d" % i).encode()
            msg = Message(TOPIC, body)
            msg.set_keys("lite-key-%02d" % i)
            msg.set_tags("TagA" if i % 2 == 0 else "TagB")
            prod.send(msg)
            sent.add(bytes(body))
    finally:
        prod.shutdown()
    check("S2 生产 %d 条成功" % N_MSG, len(sent) == N_MSG, "sent=%d" % len(sent))
    return sent


def wait_assignment(consumer: DefaultLitePullConsumer, timeout: int = 20) -> list:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if consumer.assignment():
            return consumer.assignment()
        time.sleep(0.5)
    return []


def drain(consumer: DefaultLitePullConsumer, expect: int, timeout: int = 30) -> list:
    collected = []
    deadline = time.time() + timeout
    while len(collected) < expect and time.time() < deadline:
        batch = consumer.poll(timeout=1000)
        collected.extend(batch)
    return collected


def drain_for(consumer: DefaultLitePullConsumer, seconds: float) -> list:
    """按**时间**排空缓冲：策略只分到部分队列时收条数未知，不能按条数等。"""
    collected = []
    deadline = time.time() + seconds
    while time.time() < deadline:
        collected.extend(consumer.poll(timeout=1000))
    return collected


def queue_keys(queues: list) -> set:
    """队列列表 → key 集合（"brokerName#queueId"），用于跨语言比分配结果。"""
    return {"%s#%d" % (mq.broker_name, mq.queue_id) for mq in queues}


def wait_split_assignment(a: DefaultLitePullConsumer, b: DefaultLitePullConsumer,
                          total: int, timeout: int = 25) -> tuple:
    """等两个实例把队列**分完**（分配收敛要两边各跑一轮心跳 + rebalance）。"""
    deadline = time.time() + timeout
    va, vb = [], []
    while time.time() < deadline:
        va, vb = a.assignment(), b.assignment()
        ka, kb = queue_keys(va), queue_keys(vb)
        if ka and kb and not (ka & kb) and len(ka | kb) == total:
            return va, vb
        time.sleep(0.5)
    return va, vb


def make_pair(group: str, names: tuple, strategies: tuple, *subscribe: str) -> tuple:
    """造同组两实例并各装一个策略（未 start）。

    Python 的 client_id 默认按秒生成，同秒起两个实例会撞成同一个 clientId（broker
    只看到一个消费者），所以这里照 S7b 的写法显式给一个带名字后缀的 id。
    """
    out = []
    for name, strategy in zip(names, strategies):
        c = DefaultLitePullConsumer(group)
        c.set_namesrv_addr(NAMESRV)
        c.set_poll_timeout_millis(1000)
        c.instance_name = name
        c.client_id = "%s@%d#%s" % (group, STAMP, name)
        c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
        c.allocate_message_queue_strategy = strategy
        if subscribe:
            c.subscribe(*subscribe)
        out.append(c)
    return tuple(out)


def wait_until_prediction_converged(group: str, mq_all: list, cid_members: tuple,
                                   asserted: tuple, strategies: tuple,
                                   timeout: int = 45) -> tuple:
    """用**真实**输入（路由给的 mqAll + cid_members 的真实 clientId 当 cidAll）离线跑策略，
    并**等线上 assignment() 收敛到这份预测**，返回 (是否收敛, 两边快照)。

    为什么以预测为收敛条件，而不是「先等不重不漏、再比预测」：哈希环完全可能把 4 个
    队列全分给一个实例，另一边在首轮重平衡之前 assignment() 本来就是空 —— 那种初始态
    同样满足「不重不漏」，比出来的其实是「一边还没算」的快照。（Rust 版第一版就在这里翻车。）

    Java RebalanceImpl#rebalanceByTopic 调策略前会把 mqAll、cidAll 都 Collections.sort，
    所以这里也得自己排：mq_all 由调用方排好，clientId 按字典序（同 String#compareTo）。
    strategies 与 asserted 一一对应 —— S7f 要故意让两边配不同策略。
    """
    cid_all = sorted(c.client_id for c in cid_members)
    expected = []
    for c, strategy in zip(asserted, strategies):
        predicted = strategy.allocate(group, c.client_id, mq_all, cid_all)
        expected.append(queue_keys(predicted))
    deadline = time.time() + timeout
    while True:
        live = [queue_keys(c.assignment()) for c in asserted]
        if live == expected:
            return True, _snapshot(asserted, live, expected, cid_all)
        if time.time() >= deadline:
            return False, _snapshot(asserted, live, expected, cid_all)
        time.sleep(0.5)


def _snapshot(asserted: tuple, live: list, expected: list, cid_all: list) -> str:
    return " | ".join(
        ["cid=%s live=[%s] predict=[%s]" % (c.client_id, " ".join(sorted(l)), " ".join(sorted(e)))
         for c, l, e in zip(asserted, live, expected)]
        + ["cidAll=%s" % cid_all])


# NEARBY 的落点：真实集群只有一个 broker，把队列和客户端都记成同一个机房，
# 于是 NEARBY 必定走「自己机房」那条分支、等价于内层策略。
# 同时留调用记录 —— 用来证明 rebalance 真的逐个问过队列和客户端的机房，
# 而不是策略对象换了个名字却没参与分配。
ROOM = "room1"


class OneRoomResolver(MachineRoomResolver):
    """把所有队列 / 客户端都归到 ROOM 的 resolver，顺带记下每次调用。"""

    def __init__(self) -> None:
        self.broker_calls: list = []
        self.consumer_calls: list = []

    def broker_deploy_in(self, message_queue: MessageQueue) -> str:
        self.broker_calls.append(message_queue.broker_name)
        return ROOM

    def consumer_deploy_in(self, client_id: str) -> str:
        self.consumer_calls.append(client_id)
        return ROOM


def broker_offset(probe: DefaultMQPullConsumer, mq: MessageQueue) -> int:
    """独立探针读 broker 上这一格的位点；没提交过（QUERY_NOT_FOUND）记成 -1。"""
    value = probe.fetch_consume_offset(mq)
    return -1 if value is None else value


def s8_three_offset_tables(foreign_pool: list) -> None:
    """S8 三张位点表（对位 Java AssignedMessageQueue.MessageQueueState + RemoteBrokerOffsetStore）：
    拉取游标 / 已消费游标 / 提交落点，在一队列 topic 上灌 1200 条，把三个数字在真机各自数出来。

    单测锁得住表形状，锁不住「提交错一格在真机是静默丢消息」这条后果：把拉取游标当提交源，
    位点会跑到消费前面，重启后那段消息永远不再投；反过来提交得太保守只会重复投，肉眼看得见。
    与 C++ live_lite_pull.cpp 的 S8、Rust live_lite_pull_consumer.rs 的 L11 同场景同断言。
    """
    off_topic = "LiteOff_%d" % STAMP
    off_group = "LiteOffPG_%d" % STAMP
    n_big = 1200          # > poll 单次上限 1024 ⇒ 一次交付必然留下没交出去的尾巴

    queues: list = []
    landed = 0
    prod = DefaultMQProducer("PG_PrepareOff_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    try:
        try:
            prod.create_topic("TBW102", off_topic, 1)
        except Exception as e:  # noqa: BLE001
            print("!! create_topic(%s) failed: %s" % (off_topic, e))
        route_deadline = time.time() + 20
        while not queues and time.time() < route_deadline:
            try:
                queues = prod.fetch_publish_message_queues(off_topic)
            except Exception:  # noqa: BLE001
                queues = []
            if not queues:
                time.sleep(0.5)
        check("S8 准备 topic（1 条队列）", len(queues) == 1, "queues=%d" % len(queues))
        if len(queues) == 1:
            for frm in range(0, n_big, 300):
                chunk = [Message(off_topic, ("off-%04d" % i).encode())
                         for i in range(frm, min(n_big, frm + 300))]
                try:
                    r = prod.send(chunk, timeout_millis=5000, mq=queues[0])
                    if r.send_status == SendStatus.SEND_OK:
                        landed += len(chunk)
                except Exception as e:  # noqa: BLE001
                    print("   send batch failed: %s" % e)
    finally:
        prod.shutdown()
    check("S8 生产 %d 条成功" % n_big, landed == n_big, "landed=%d" % landed)

    # 独立读 broker 位点的探针（自己的组、自己的连接，不碰被测实例的任何内存）
    probe = DefaultMQPullConsumer(off_group)
    probe.set_namesrv_addr(NAMESRV)
    probe.start()

    q0 = queues[0] if queues else MessageQueue(off_topic, "broker-a", 0)
    o = DefaultLitePullConsumer(off_group)
    o.set_namesrv_addr(NAMESRV)
    o.instance_name = "liteoff"
    o.client_id = "%s@%d#liteoff" % (off_group, STAMP)
    o.set_poll_timeout_millis(1000)
    o.set_pull_batch_size(32)
    o.auto_commit = False            # 提交时机全部由场景控制
    o.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    o.assign([q0])
    o.start()
    try:
        # S8a 只拉不交付：拉取游标跑到 1200，已消费游标一格都不许动
        pull_deadline = time.time() + 30
        while o.pull_cursor_of(q0) < n_big and time.time() < pull_deadline:
            time.sleep(0.2)
        check("S8a 后台把 %d 条全拉进本地缓冲（拉取游标=%d）" % (n_big, o.pull_cursor_of(q0)),
              o.pull_cursor_of(q0) == n_big, "pullCursor=%d" % o.pull_cursor_of(q0))
        check("S8a 一条都没交付 ⇒ 已消费游标停在 -1", o.consume_cursor_of(q0) == -1,
              "consumeCursor=%d" % o.consume_cursor_of(q0))
        # 这条是 #68 的核心：旧实现 commit() 遍历的就是拉取游标，这里会把 1200 发出去，
        # 调用方在此之前崩掉 ⇒ 1200 条一条都没消费过却再也不会投。
        o.commit()
        after_empty = broker_offset(probe, q0)
        check("S8a 没交付过 ⇒ broker 位点没被推走（旧实现在这里提交 %d）" % n_big,
              after_empty != n_big, "brokerOffset=%d" % after_empty)

        # S8b poll 单次上限 1024 < 缓冲里的 1200 ⇒ 尾巴那 176 条不算已消费
        first = o.poll(timeout=3000)
        check("S8b 一次 poll 交出 1024 条（单次交付上限）", len(first) == 1024,
              "got=%d" % len(first))
        check("S8b 已消费游标 = 交出去的那一格", o.consume_cursor_of(q0) == 1024,
              "consumeCursor=%d" % o.consume_cursor_of(q0))
        check("S8b 缓冲里还压着 %d 条没交付" % (n_big - 1024),
              o.pull_cursor_of(q0) == n_big and o.consume_cursor_of(q0) < o.pull_cursor_of(q0),
              "pull=%d" % o.pull_cursor_of(q0))
        o.commit()
        broker_at = broker_offset(probe, q0)
        check("S8b 提交给 broker 的正是 1024（不是 %d）" % n_big, broker_at == 1024,
              "brokerOffset=%d" % broker_at)

        # S8c 指定一个更靠前的位点：只改提交落点，两条游标都不许动
        pull_before = o.pull_cursor_of(q0)
        consume_before = o.consume_cursor_of(q0)
        o.commit({q0: 5}, persist=True)
        rewound = broker_offset(probe, q0)
        check("S8c commit(map) 把 broker 位点改到调用方指定的 5", rewound == 5,
              "brokerOffset=%d" % rewound)
        check("S8c 提交位点不改拉取游标", o.pull_cursor_of(q0) == pull_before,
              "pullCursor=%d" % o.pull_cursor_of(q0))
        check("S8c 提交位点不改已消费游标", o.consume_cursor_of(q0) == consume_before,
              "consumeCursor=%d" % o.consume_cursor_of(q0))
        # 位点退回 5 之后，尾巴那 176 条照旧交付（缓冲与 broker 位点无关）
        tail = o.poll(timeout=3000)
        check("S8c 退回 5 之后缓冲里剩下的 %d 条照旧交付" % (n_big - 1024),
              len(tail) == n_big - 1024, "got=%d" % len(tail))
        bodies = {bytes(m.body) for m in first} | {bytes(m.body) for m in tail}
        check("S8 全程收全 %d 条且一条不重不漏" % n_big, len(bodies) == n_big,
              "distinct=%d" % len(bodies))

        # S8d 一个字节都不许上线：committed() 看得见、broker 看不见
        o.commit({q0: 777}, persist=False)
        check("S8d persist=False：committed() 读到内存表的 777", o.committed(q0) == 777,
              "committed=%s" % o.committed(q0))
        still = broker_offset(probe, q0)
        check("S8d persist=False：broker 侧还是上一轮的 5", still == 5,
              "brokerOffset=%d" % still)

        # S8e 新实例（同组）从 broker 上那一格续消费：内存表不跨实例
        o2 = DefaultLitePullConsumer(off_group)
        o2.set_namesrv_addr(NAMESRV)
        o2.instance_name = "liteoff2"
        o2.client_id = "%s@%d#liteoff2" % (off_group, STAMP)
        o2.set_poll_timeout_millis(1000)
        o2.set_pull_batch_size(32)
        o2.auto_commit = False
        o2.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
        o2.assign([q0])
        o2.start()
        start2 = o2.pull_cursor_of(q0)
        # 上界只要求「没吃到另一个实例内存里那格 777」：起点的拉取游标会被后台续拉往前推，
        # 卡死等于 5 就成了赌时序（Rust L11e 同口径）。
        check("S8e 新实例的起点是 broker 上的 5（不是另一个实例内存里的 777）",
              5 <= start2 < 777, "pullCursor=%d" % start2)
        o2.shutdown()

        # S8f seek 同时改写两条游标：重放的段不能被旧位点跳过
        # 先暂停这条队列：后台续拉会把拉取游标推过 60，不停下来这条断言就成了赌时序。
        o.pause([q0])
        time.sleep(0.3)
        o.seek(q0, 60)
        check("S8f seek 改拉取游标", o.pull_cursor_of(q0) == 60,
              "pullCursor=%d" % o.pull_cursor_of(q0))
        check("S8f seek 也改已消费游标", o.consume_cursor_of(q0) == 60,
              "consumeCursor=%d" % o.consume_cursor_of(q0))
        o.commit()
        seeked = broker_offset(probe, q0)
        check("S8f seek 之后 commit 落到 60", seeked == 60, "brokerOffset=%d" % seeked)
        o.resume([q0])

        # S8g Java RemoteBrokerOffsetStore#persistAll 的 "remove unused mq"：点名提交
        # 只发被点名的队列，内存表里**其余**条目顺手删掉 —— 上一轮 persist=False 攒下、
        # 还没落盘的内存值就此丢掉。这条在 broker 上可观测：清掉之后 committed() 只能
        # 回读到 broker 上那一格，再也读不到 300。
        if len(foreign_pool) < 2:
            check("S8g 需要主 topic 至少 2 条队列", False, "queues=%d" % len(foreign_pool))
            return
        foreign = foreign_pool[1]      # 另一个 topic 的队列，off_group 没碰过
        o.assign([q0, foreign])        # 只多一条游标，不影响上面的对列
        o.commit({q0: 300}, persist=False)
        check("S8g 未落盘的内存值先看得见", o.pending_commit_of(q0) == 300,
              "pending=%d" % o.pending_commit_of(q0))
        # 空集合：Java 的 commit(Set) 直接 return，表不动、消息也不发
        o.commit([], persist=True)
        check("S8g 空集合不清表也不发消息", o.pending_commit_of(q0) == 300,
              "pending=%d" % o.pending_commit_of(q0))
        still2 = broker_offset(probe, q0)
        check("S8g 空集合没动 broker", still2 == 60, "brokerOffset=%d" % still2)
        # 点名一条 foreign 队列：它没有消费记录（-1 守卫拦下写表），
        # 但 persistAll 扫表时把 q0 那份未落盘的值清了 —— 这才是「提交部分队列」的代价。
        o.commit([foreign], persist=True)
        check("S8g 点名提交会把没点名的内存值清掉（Java 的 remove unused mq）",
              o.pending_commit_of(q0) == -1, "pending=%d" % o.pending_commit_of(q0))
        check("S8g 清掉之后回读到的是 broker 上那一格（并回填进表）",
              o.committed(q0) == 60 and o.pending_commit_of(q0) == 60,
              "committed=%s pending=%d" % (o.committed(q0), o.pending_commit_of(q0)))
        after_prune = broker_offset(probe, q0)
        check("S8g 清理只是丢内存值，没往 broker 写 300", after_prune == 60,
              "brokerOffset=%d" % after_prune)
        # commit(Set) 取的是当下已消费游标，不是内存里那格
        o.commit({q0: 300}, persist=False)
        o.commit([q0], persist=True)
        set_committed = broker_offset(probe, q0)
        check("S8g commit(Set) 提交的是已消费游标（60），不是内存里那格 300",
              set_committed == 60, "brokerOffset=%d" % set_committed)
        check("S8g persistAll 之后内存表回到已消费游标（Java 的 updateConsumeOffset）",
              o.pending_commit_of(q0) == 60, "pending=%d" % o.pending_commit_of(q0))
    finally:
        o.shutdown()
        probe.shutdown()


def main() -> int:
    print("LitePullConsumer 真机验证 topic=%s group=%s" % (TOPIC, GROUP1))
    prepare_topic()

    # ===== S2 subscribe 模式：先起消费者再发消息 =====
    print("\nS1 subscribe 模式启动 + 等待 rebalance 分到位点")
    c1 = DefaultLitePullConsumer(GROUP1)
    c1.set_namesrv_addr(NAMESRV)
    c1.set_poll_timeout_millis(1000)
    c1.subscribe(TOPIC, "*")
    c1.start()
    assigned = wait_assignment(c1)
    check("S1 rebalance 分配到队列", len(assigned) == QUEUE_NUM, "assigned=%d" % len(assigned))
    if not assigned:
        print("!! 未分配到队列，后续跳过")
        c1.shutdown()
        return 1

    sent = produce()
    got = drain(c1, N_MSG)
    got_bytes = {bytes(m.body) for m in got}
    check("S2 subscribe+poll 收全 %d 条且内容一致" % N_MSG, got_bytes == sent,
          "got=%d missing=%s extra=%s" % (len(got), sorted(sent - got_bytes),
                                          sorted(got_bytes - sent)))

    # ===== S3 auto-commit =====
    print("\nS3 auto-commit 位点")
    # 场景故意不手动 commit：验的是「继续 poll 的常态下位点会自己落到 broker」。
    # Java 的自动提交只在 poll() 开头按 nextAutoCommitDeadline 到点才跑（默认 5s），
    # 而第一次检查发生在交付之前（已消费游标还是 -1，提交不出东西），所以要 poll 过一整个周期。
    before = {mq: c1.committed(mq) for mq in assigned}
    deadline = time.time() + 20
    after = before
    while time.time() < deadline:
        c1.poll(timeout=500)          # 空转也算一次 poll：闸门是在 poll 里查的
        after = {mq: c1.committed(mq) for mq in assigned}
        if all(v is not None and v > 0 for v in after.values()):
            break
    check("S3 继续 poll 过自动提交周期后位点自己落盘（没人调 commit）",
          all(v is not None and v > 0 for v in after.values()),
          "before=%s after=%s" % (before, after))
    # 手动 commit 那条路也要能落盘：位点已经对上了，再提交一次不许把它挪回去
    c1.commit()
    after2 = {mq: c1.committed(mq) for mq in assigned}
    check("S3 commit() 之后位点不回退",
          all(after2[mq] is not None and after2[mq] >= (after[mq] or 0) for mq in assigned),
          "after2=%s" % after2)

    # ===== S4 assign 模式：显式分配 + seek 到队首重新收全 =====
    print("\nS4 assign 模式：assign 全部队列 + seek 到队首重新收全")
    c2 = DefaultLitePullConsumer(GROUP2)
    c2.set_namesrv_addr(NAMESRV)
    c2.set_poll_timeout_millis(1000)
    c2.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    all_queues = c1.fetch_message_queues(TOPIC)
    c2.assign(all_queues)
    c2.start()
    for mq in all_queues:
        c2.seek_to_begin(mq)
    got2 = drain(c2, N_MSG)
    got2_bytes = {bytes(m.body) for m in got2}
    check("S4 assign+seek+poll 重新收全 %d 条" % N_MSG, got2_bytes == sent,
          "got=%d" % len(got2))
    c2.shutdown()

    # ===== S5 订阅级 tag 过滤 =====
    print("\nS5 订阅 TagA：只收 TagA 的 6 条")
    c3 = DefaultLitePullConsumer(GROUP3)
    c3.set_namesrv_addr(NAMESRV)
    c3.set_poll_timeout_millis(1000)
    c3.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c3.subscribe(TOPIC, "TagA")
    c3.start()
    wait_assignment(c3)
    got3 = drain(c3, 6)
    got3_bytes = {bytes(m.body) for m in got3}
    print("    [diag] S5 got=%d bodies=%s tags=%s" % (
        len(got3), sorted(bytes(m.body).decode() for m in got3),
        sorted((m.get_tags() or "?") for m in got3)))
    only_a = got3_bytes.issubset(TAG_A) and len(got3_bytes) == 6
    check("S5 仅收 TagA 且恰好 6 条", only_a,
          "got=%d set_ok=%s" % (len(got3), got3_bytes <= TAG_A))
    c3.shutdown()

    # ===== S6 CONSUME_FROM_TIMESTAMP：14 位本地墙钟 =====
    print("\nS6 CONSUME_FROM_TIMESTAMP：墙钟起点收全 + 时间戳→位点映射")
    wall = int(time.time() * 1000)

    def wall_clock(delta_ms: int) -> str:
        return time.strftime("%Y%m%d%H%M%S", time.localtime((wall + delta_ms) / 1000))

    c4 = DefaultLitePullConsumer(GROUP4)
    c4.set_namesrv_addr(NAMESRV)
    c4.set_poll_timeout_millis(1000)
    c4.consume_from_where = ConsumeFromWhere.CONSUME_FROM_TIMESTAMP
    c4.consume_timestamp = wall_clock(-30 * 60 * 1000)
    c4.subscribe(TOPIC, "*")
    c4.start()
    wait_assignment(c4)
    got4 = drain(c4, N_MSG)
    got4_bytes = {bytes(m.body) for m in got4}
    check("S6a 起点=%s 早于全部消息 → 收全 %d 条" % (c4.consume_timestamp, N_MSG),
          got4_bytes == sent, "got=%d" % len(got4_bytes))
    c4.shutdown()

    c5 = DefaultLitePullConsumer(GROUP5)
    c5.set_namesrv_addr(NAMESRV)
    c5.set_poll_timeout_millis(1000)
    c5.consume_from_where = ConsumeFromWhere.CONSUME_FROM_TIMESTAMP
    c5.consume_timestamp = wall_clock(10 * 60 * 1000)
    c5.subscribe(TOPIC, "*")
    c5.start()
    q5 = wait_assignment(c5)
    # 不断言「未来时间戳收不到消息」：Java 的 RebalanceLitePullImpl 先读已提交位点，
    # 新组只要队首仍在 commitlog 内，broker 就直接回 0，consume_from_where 不参与。
    # 墙钟真正影响的是「时间戳 → 队列位置」的映射，所以断言这个量。
    sum_past = sum(c5.offset_for_timestamp(mq, wall - 30 * 60 * 1000) for mq in q5)
    sum_future = sum(c5.offset_for_timestamp(mq, wall + 10 * 60 * 1000) for mq in q5)
    check("S6b 30 分钟前 → 各队列队首", sum_past == 0, "sumOffset=%d" % sum_past)
    check("S6b 10 分钟后 → 越过全部 %d 条" % N_MSG, sum_future == N_MSG,
          "sumOffset=%d" % sum_future)
    c5.shutdown()

    # ===== S7 可插拔队列分配策略 =====
    print("\nS7 可插拔队列分配策略（对应 Java setAllocateMessageQueueStrategy）")
    all_queues = sorted(c1.fetch_message_queues(TOPIC),
                        key=lambda mq: (mq.topic, mq.broker_name, mq.queue_id))
    total_queues = len(all_queues)

    # S7a 默认策略 / None 守卫（Java 在 checkConfig 里拒绝 null）
    probe = DefaultLitePullConsumer(GROUP6)
    probe.set_namesrv_addr(NAMESRV)
    check("S7a 默认策略名 = AVG",
          probe.allocate_message_queue_strategy.get_name() == "AVG",
          "name=%s" % probe.allocate_message_queue_strategy.get_name())
    probe.allocate_message_queue_strategy = None
    probe.subscribe(TOPIC, "*")
    try:
        probe.start()
        check("S7a 策略为 None 时 start() 报 Java 同款文案", False, "没有抛异常")
        probe.shutdown()
    except MQClientException as e:
        check("S7a 策略为 None 时 start() 报 Java 同款文案",
              "allocateMessageQueueStrategy is null" in str(e), str(e))

    # S7b AVG_BY_CIRCLE：同组两实例交叉切分，不重不漏
    circle = AllocateMessageQueueAveragelyByCircle()
    ca, cb = DefaultLitePullConsumer(GROUP7), DefaultLitePullConsumer(GROUP7)
    for c, name in ((ca, "s7ca"), (cb, "s7cb")):
        c.set_namesrv_addr(NAMESRV)
        c.set_poll_timeout_millis(1000)
        c.instance_name = name
        # 同进程两个实例必须有不同的 clientId，否则 broker 侧只看到一个消费者
        c.client_id = "%s@%s#%s" % (GROUP7, STAMP, name)
        c.allocate_message_queue_strategy = circle
        c.subscribe(TOPIC, "*")
    check("S7b 替换后策略名 = AVG_BY_CIRCLE",
          ca.allocate_message_queue_strategy.get_name() == "AVG_BY_CIRCLE")
    ca.start()
    cb.start()
    qa, qb = wait_split_assignment(ca, cb, total_queues)
    ka, kb = queue_keys(qa), queue_keys(qb)
    check("S7b 两实例分配无交集", not (ka & kb), "overlap=%s" % sorted(ka & kb))
    check("S7b 并集覆盖全部 %d 个队列" % total_queues,
          (ka | kb) == queue_keys(all_queues), "a=%d b=%d" % (len(ka), len(kb)))
    # 环形分配的签名：拿到的是「按下标取模」的交叉队列而非连续段
    # （4 队列 / 2 实例 → 各 2 条且下标步长为 2；AVG 会给连续两段）。
    pos_a = sorted(i for i, mq in enumerate(all_queues)
                   if "%s#%d" % (mq.broker_name, mq.queue_id) in ka)
    circle_shape = len(pos_a) == 2 and all((y - x) % 2 == 0 for x, y in zip(pos_a, pos_a[1:]))
    check("S7b 分配形状是交叉（步长 2），不是 AVG 的连续段",
          total_queues != 4 or circle_shape, "posA=%s" % pos_a)
    ca.shutdown()
    cb.shutdown()

    # S7c CONFIG：只分配配置进去的一半队列，poll 到的消息也只能来自这些队列
    half_a, half_b = all_queues[:total_queues // 2], all_queues[total_queues // 2:]
    cfg_a, cfg_b = AllocateMessageQueueByConfig(half_a), AllocateMessageQueueByConfig(half_b)
    c6, c7 = DefaultLitePullConsumer(GROUP8), DefaultLitePullConsumer(GROUP9)
    for c, strategy in ((c6, cfg_a), (c7, cfg_b)):
        c.set_namesrv_addr(NAMESRV)
        c.set_poll_timeout_millis(1000)
        c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
        c.allocate_message_queue_strategy = strategy
        c.subscribe(TOPIC, "*")
    c6.start()
    c7.start()
    a6, a7 = wait_assignment(c6), wait_assignment(c7)
    check("S7c CONFIG 只给配置进去的队列（无视 mqAll/cidAll）",
          queue_keys(a6) == queue_keys(half_a) and queue_keys(a7) == queue_keys(half_b),
          "a=%s b=%s" % (sorted(queue_keys(a6)), sorted(queue_keys(a7))))
    keys_a, keys_b = queue_keys(half_a), queue_keys(half_b)
    g6, g7 = drain_for(c6, 8), drain_for(c7, 8)
    b6 = {bytes(m.body) for m in g6}
    b7 = {bytes(m.body) for m in g7}
    on_cfg_a = {bytes(m.body) for m in g6 if "%s#%d" % (m.broker_name, m.queue_id) in keys_a}
    on_cfg_b = {bytes(m.body) for m in g7 if "%s#%d" % (m.broker_name, m.queue_id) in keys_b}
    check("S7c CONFIG 消费者只收到自己配置队列里的消息",
          b6 == on_cfg_a and b7 == on_cfg_b,
          "a=%d aOnCfg=%d b=%d bOnCfg=%d" % (len(b6), len(on_cfg_a), len(b7), len(on_cfg_b)))
    check("S7c 两半合起来恰好覆盖全部 %d 条且互不重叠" % N_MSG,
          (b6 | b7) == sent and not (b6 & b7),
          "union=%d inter=%d" % (len(b6 | b7), len(b6 & b7)))
    c6.shutdown()
    c7.shutdown()

    # S7d CONSISTENT_HASH：用**真实 clientId** 建哈希环，线上分配要收敛到离线预测。
    # 注意判定只看「不重不漏」：环的落点由 clientId 的 MD5 决定，真实集群上完全可能
    # 一边 4 条、另一边 0 条（Java 同款），所以不能要求两边都非空。
    ch = AllocateMessageQueueConsistentHash()
    d1, d2 = make_pair(GROUP10, ("s7da", "s7db"), (ch, ch), TOPIC, "*")
    check("S7d 替换后策略名 = CONSISTENT_HASH",
          d1.allocate_message_queue_strategy.get_name() == "CONSISTENT_HASH",
          d1.allocate_message_queue_strategy.get_name())
    d1.start()
    d2.start()
    ok, snap = wait_until_prediction_converged(GROUP10, all_queues, (d1, d2), (d1, d2), (ch, ch))
    check("S7d 线上分配收敛到一致性环的离线预测", ok, snap)
    kd1, kd2 = queue_keys(d1.assignment()), queue_keys(d2.assignment())
    check("S7d 两实例分配无交集", not (kd1 & kd2), "overlap=%s" % sorted(kd1 & kd2))
    check("S7d 并集覆盖全部 %d 个队列" % total_queues,
          (kd1 | kd2) == queue_keys(all_queues), "a=%d b=%d" % (len(kd1), len(kd2)))
    bd1 = {bytes(m.body) for m in drain_for(d1, 8)}
    bd2 = {bytes(m.body) for m in drain_for(d2, 8)}
    check("S7d 两实例合起来收到全部 %d 条（环真的在驱动收发）" % N_MSG,
          (bd1 | bd2) == sent, "union=%d" % len(bd1 | bd2))
    d1.shutdown()
    d2.shutdown()

    # S7e MACHINE_ROOM_NEARBY：真实集群只有一个机房 ⇒ 装饰器必须原样透传内层策略。
    inner = AllocateMessageQueueConsistentHash()
    resolver = OneRoomResolver()
    nearby = AllocateMachineRoomNearby(inner, resolver)
    e1, e2 = make_pair(GROUP11, ("s7ea", "s7eb"), (nearby, nearby), TOPIC, "*")
    check("S7e 装饰后的策略名 = MACHINE_ROOM_NEARBY-CONSISTENT_HASH",
          e1.allocate_message_queue_strategy.get_name() == "MACHINE_ROOM_NEARBY-CONSISTENT_HASH",
          e1.allocate_message_queue_strategy.get_name())
    e1.start()
    e2.start()
    ok, snap = wait_until_prediction_converged(GROUP11, all_queues, (e1, e2), (e1, e2),
                                              (inner, inner))
    check("S7e NEARBY 的线上分配 == 内层环的离线预测", ok, snap)
    real_brokers = {mq.broker_name for mq in all_queues}
    check("S7e resolver 被逐个队列问过机房（%d 次）" % len(resolver.broker_calls),
          bool(resolver.broker_calls) and set(resolver.broker_calls) == real_brokers,
          "brokers=%s" % sorted(set(resolver.broker_calls)))
    check("S7e resolver 被问过两个真实 clientId",
          {e1.client_id, e2.client_id} <= set(resolver.consumer_calls),
          "calls=%s" % sorted(set(resolver.consumer_calls)))
    e1.shutdown()
    e2.shutdown()

    # S7f MACHINE_ROOM：真实 brokerName 是 `broker-a`，Java 的 split("@") 只切出 1 段
    # ⇒ 白名单怎么写都筛不出队列。要验的是「配错机房安静饿死」，而不是把 rebalance 打崩。
    room = AllocateMessageQueueByMachineRoom({ROOM})
    check("S7f 策略名 = MACHINE_ROOM 且白名单能读回",
          room.get_name() == "MACHINE_ROOM" and room.consumeridcs == {ROOM},
          "idcs=%s" % sorted(room.consumeridcs))
    avg = AllocateMessageQueueAveragely()
    f1, f2 = make_pair(GROUP12, ("s7fa", "s7fb"), (room, avg), TOPIC, "*")
    f1.start()
    f2.start()
    ctrl = wait_assignment(f2)
    check("S7f 同组对照组（AVG）正常分到队列", bool(ctrl), "ctrl=%s" % sorted(queue_keys(ctrl)))
    check("S7f 机房不匹配真实 brokerName → 一条都不分（不报错也不误吃）",
          not f1.assignment(), "assignment=%s" % sorted(queue_keys(f1.assignment())))
    # 两边各自算策略（Java 就是各算各的）：配错机房的一方算出空，对照组按**两个** cid
    # 算 AVG 只拿到自己那半边 —— 它没有替配错的那位兜底，这才是 Java 的语义。
    ok, snap = wait_until_prediction_converged(GROUP12, all_queues, (f1, f2), (f1, f2),
                                              (room, avg))
    check("S7f 两边线上分配各自收敛到自己策略的离线预测", ok, snap)
    starved = drain_for(f1, 5)
    check("S7f 被饿死的一方 poll 不到消息也不抛错", not starved, "got=%d" % len(starved))
    f1.shutdown()
    f2.shutdown()

    c1.shutdown()

    # ===== S8 三张位点表 =====
    print("\nS8 三张位点表：拉取游标 / 已消费游标 / 提交落点")
    s8_three_offset_tables(assigned)

    print("\nLitePullConsumer: PASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
