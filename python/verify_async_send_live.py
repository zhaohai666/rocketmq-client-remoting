# -*- coding: utf-8 -*-
"""异步发送内核（``DefaultMQProducerImpl`` 的 ASYNC 分支 + ``sendMessageAsync`` /
``onExceptionImpl`` 那一整套）真机验证。

离线单测（``tests/test_producer_async.py``）锁的是链的形状：调用方立刻返回、钩子各跑
一次、预算共享、闸门/队满怎么拒。那些场景真集群造不出来（造不出 SYSTEM_BUSY，也造不
出慢 broker），但离线对拍也证不了真 broker 上的六件事：

  A1  「立刻返回」是真的：before 钩子睡 400ms 时调用方仍在 200ms 内返回；这一笔最后
      SEND_OK，且用 broker 回的 offset_msg_id 能 **view_message 读回原 body 和
      queue_offset**（回调里的 SendResult 不是自说自话）；before 钩子在
      ``AsyncSenderExecutor_N`` 上跑、用户回调在 ``NettyClientPublicExecutor_N`` 上跑
      （Java executeInvokeCallback 的线程口径）。
  A2  并发 30 笔异步发送：**每笔恰好一个终态**、全部 SEND_OK、broker 上正好落 30 条，
      并且各笔的 queue_offset 互不重叠（串台的话两个回调会指向同一个位置）。
  A3  定点发送（给了 mq）真的落在那条队列上，别的队列一条都不多。
  A4  拦截钩子（CheckForbidden）看到的是 CommunicationMode.ASYNC；它拒绝时异常原样到
      回调，而且 broker 上一条都没落（连请求都没发出去）。
  A5  批量异步没有异步内核，走同步批量内核（在 AsyncSender 线程里跑）：一次回调、
      三条都落地。
  A6  Shutdown 的**不等待**语义（本实现照抄 Java 的 ``shutdown()``，不像 .NET 那样 join
      池线程）：交进来的每一笔仍然跑完准备段、仍然拿到终态回调，但客户端已经先一步
      关掉，所以这一整轮**一笔都没上线**（实测 36/36 报错、broker 上连 topic 都没建
      出来）。这条把「调用方必须自己等回调再关」锁成可观察的事实。

前置：NameServer + Broker 已起，`autoCreateTopicEnable=true`。

用法：.venv/bin/python verify_async_send_live.py [127.0.0.1:9876]
"""
from __future__ import annotations

import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rocketmq.client.admin import DefaultMQAdminExt
from rocketmq.client.exception import MQClientException
from rocketmq.client.hook import CheckForbiddenHook, SendMessageHook
from rocketmq.client.producer import DefaultMQProducer, SendCallback
from rocketmq.client.send_result import SendStatus
from rocketmq.common.message import Message

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time())
GROUP = "PID_rmq_async_%d" % STAMP

results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print("[%s] %s %s" % ("PASS" if ok else "FAIL", name, detail))


def _wait_until(pred, timeout=20.0, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


def _first(items):
    """取第一条的文本（detail 里要打印，空列表得打印成空串而不是 IndexError）。"""
    return str(items[0]) if items else ""


class _Recorder(SendCallback):
    """线程安全的回调记账。

    除了终态条数还要记下**跑在哪根线程上**：Java 的 executeInvokeCallback 口径（回调
    在 publicExecutor 上）只能这么验。
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.results = []
        self.errors = []
        self.threads = []
        self.ats = []

    def on_success(self, send_result):
        with self.lock:
            self.results.append(send_result)
            self._note()

    def on_exception(self, e):
        with self.lock:
            self.errors.append(e)
            self._note()

    def _note(self):
        """记下发终态的时刻和线程名（调用方必须已持有 ``self.lock``）。"""
        self.ats.append(time.monotonic())
        self.threads.append(threading.current_thread().name)

    def after(self, moment):
        """``moment``（``time.monotonic()`` 刻度）之后发出的回调条数。"""
        with self.lock:
            return sum(1 for t in self.ats if t > moment)

    @property
    def done(self):
        with self.lock:
            return len(self.results) + len(self.errors)

    def summary(self):
        with self.lock:
            first = str(self.errors[0]) if self.errors else ""
            return "ok=%d err=%d %s" % (len(self.results), len(self.errors), first)


class _TracingHook(SendMessageHook):
    """before 钩子睡一段时间：把「准备段」拖慢，才能证明调用方没等它。

    顺便记下两件事：钩子跑在哪根线程上、before/after 各跑了几次。
    """

    def __init__(self, millis=0):
        self.millis = millis
        self.lock = threading.Lock()
        self.before = 0
        self.after = 0
        self.before_thread = ""

    def hook_name(self):
        return "async-tracing"

    def send_message_before(self, context):
        with self.lock:
            self.before += 1
            self.before_thread = threading.current_thread().name
        if self.millis:
            time.sleep(self.millis / 1000.0)

    def send_message_after(self, context):
        with self.lock:
            self.after += 1


class _ForbiddenTagHook(CheckForbiddenHook):
    """只拒绝带 forbidden 标签的消息，并记下它看到的 CommunicationMode。"""

    def __init__(self):
        self.lock = threading.Lock()
        self.calls = 0
        self.mode = None

    def hook_name(self):
        return "async-forbidden"

    def check_forbidden(self, context):
        with self.lock:
            self.calls += 1
            self.mode = context.communication_mode
        if context.message is not None and context.message.get_tags() == "forbidden":
            # 异常**不吞**：Java 的 checkForbidden 签名就是 throws MQClientException
            raise MQClientException("live test: tag forbidden is not allowed")


def _producer(instance_name, *hooks) -> DefaultMQProducer:
    p = DefaultMQProducer(GROUP)
    p.set_namesrv_addr(NAMESRV)
    p.set_instance_name(instance_name)
    # 钩子必须在 start() 之前注册：Java 的钩子表在 start 后就固定了
    for h in hooks:
        if isinstance(h, SendMessageHook):
            p.register_send_message_hook(h)
        else:
            p.register_check_forbidden_hook(h)
    return p


def _start_all(threads):
    for t in threads:
        t.start()


def _join_all(threads, timeout=30.0):
    deadline = time.monotonic() + timeout
    for t in threads:
        t.join(max(0.0, deadline - time.monotonic()))
    stuck = sum(1 for t in threads if t.is_alive())
    check("*  发送线程没有卡死", stuck == 0, "%d 条还活着" % stuck)


def _landed(admin, topic, quiet=False):
    """broker 上这个 topic 一共落了多少条（各队列 maxOffset-minOffset 之和）。

    这是「回调里的 SendResult 不是自说自话」的唯一硬证据。读不到路由返回 -1。
    """
    try:
        queues = admin.examine_topic_route(topic).get_all_message_queue(topic)
    except Exception as e:  # noqa: BLE001 — 路由还没注册上
        if not quiet:
            print("    (读 %s 路由失败: %s)" % (topic, e))
        return -1
    total = 0
    for mq in queues:
        try:
            total += admin.max_offset(mq) - admin.min_offset(mq)
        except Exception:  # noqa: BLE001 — 该队列刚建出来还没写过
            pass
    return total


def _wait_landed(admin, topic, expected, timeout=30.0):
    """等 broker 上至少出现 ``expected`` 条，返回最后一次的读数。

    新建 topic 要等 broker 把 topicConfig 增量注册到 namesrv（秒级到十秒级），所以
    头几次读不到路由不算失败，重试到超时才算。
    """
    deadline = time.time() + timeout
    landed = _landed(admin, topic, quiet=True)
    while landed < expected and time.time() < deadline:
        time.sleep(0.5)
        landed = _landed(admin, topic, quiet=True)
    return landed


def _queue_landed(admin, mq):
    try:
        return admin.max_offset(mq) - admin.min_offset(mq)
    except Exception:  # noqa: BLE001
        return -1


# ------------------------------------------------- A1 立刻返回 + 线程口径 + 读回
def a1_non_blocking_and_read_back(admin, topic):
    hook = _TracingHook(400)
    p = _producer("a1-%d" % STAMP, hook)
    p.start()
    rec = _Recorder()
    body = "async-a1-readback"
    try:
        msg = Message(topic, body.encode())
        began = time.monotonic()
        p.send_async(msg, rec, 5000)
        took_ms = int((time.monotonic() - began) * 1000)
        check("A1 调用方在准备段（钩子睡 400ms）之前就已返回",
              took_ms < 200 and hook.before <= 1,
              "调用方耗时=%dms before 已跑=%d" % (took_ms, hook.before))
        check("A1 回调恰好一次且 SEND_OK",
              _wait_until(lambda: rec.done >= 1, 20.0)
              and len(rec.results) == 1 and not rec.errors, rec.summary())
        if not rec.results:
            return
        r = rec.results[0]
        check("A1 before 钩子在 AsyncSenderExecutor_N 上跑",
              hook.before_thread.startswith("AsyncSenderExecutor_"),
              "线程名=%s" % hook.before_thread)
        check("A1 用户回调在 NettyClientPublicExecutor_N 上跑（Java executeInvokeCallback）",
              rec.threads[0].startswith("NettyClientPublicExecutor_"),
              "线程名=%s" % rec.threads[0])
        check("A1 before/after 各跑一次", hook.before == 1 and hook.after == 1,
              "before=%d after=%d" % (hook.before, hook.after))
        check("A1 broker 落了这一条", _wait_landed(admin, topic, 1) == 1)
        # offset_msg_id 是 broker 给的，只有它能解出 commitLog 偏移 ⇒ 读回来对 body
        back = [None]

        def _view():
            try:
                back[0] = admin.view_message(topic, r.offset_msg_id)
                return back[0] is not None
            except Exception:  # noqa: BLE001 — commitLog 还没刷出去
                return False

        read = _wait_until(_view, 15.0)
        check("A1 用回调里的 offset_msg_id 能读回这条消息",
              read and back[0].body.decode() == body,
              "body=%s" % ("<null>" if back[0] is None else back[0].body.decode()))
        check("A1 回调里的 queue_offset 就是它落在的位置",
              r.queue_offset == admin.max_offset(r.message_queue) - 1,
              "queue_offset=%d max_offset=%d"
              % (r.queue_offset, admin.max_offset(r.message_queue)))
        # msg_id 是客户端补的 UNIQ_KEY（32 位十六进制），不是 broker 的那份
        check("A1 msg_id 是客户端 UNIQ_KEY、与 broker 的 offset_msg_id 不同",
              len(r.msg_id) == 32 and r.msg_id != r.offset_msg_id,
              "msgId=%s offsetMsgId=%s" % (r.msg_id, r.offset_msg_id))
    finally:
        p.shutdown()


# ------------------------------------------------- A2 并发不串台
def a2_burst_exactly_once_each(admin, topic):
    burst = 30
    p = _producer("a2-%d" % STAMP)
    p.start()
    each = [_Recorder() for _ in range(burst)]
    try:
        threads = [threading.Thread(
            target=lambda i=i: p.send_async(
                Message(topic, ("async-burst-%d" % i).encode()), each[i], 8000))
            for i in range(burst)]
        _start_all(threads)
        _join_all(threads)
        check("A2 30 笔并发异步发送每笔都拿到终态",
              _wait_until(lambda: all(r.done >= 1 for r in each), 20.0),
              "done=%d" % sum(r.done for r in each))
        check("A2 每笔**恰好一个**终态（不多不少）",
              all(r.done == 1 for r in each),
              "多拿回调的笔数=%d" % sum(1 for r in each if r.done > 1))
        check("A2 全部 SEND_OK",
              all(len(r.results) == 1 and not r.errors for r in each), each[0].summary())
        check("A2 broker 上正好落 30 条", _wait_landed(admin, topic, burst) == burst)
        slots = set()
        uniq = set()
        for r in each:
            for s in r.results:
                slots.add((s.message_queue.broker_name, s.message_queue.queue_id,
                           s.queue_offset))
                uniq.add(s.msg_id)
        check("A2 各笔的 (broker, queueId, queue_offset) 互不重叠",
              len(slots) == burst, "去重后=%d" % len(slots))
        check("A2 每笔的 UNIQ_KEY 都不一样", len(uniq) == burst, "去重后=%d" % len(uniq))
    finally:
        p.shutdown()


# ------------------------------------------------- A3 定点发送
def a3_pinned_queue(admin, topic):
    p = _producer("a3-%d" % STAMP)
    p.start()
    rec = _Recorder()
    try:
        # 先把 topic 撑出来（同步发一笔，让 broker 把队列建全），再挑一条定点打。
        # ⚠ 取基线之前必须等预热那一笔在 broker 侧**已经可读**：刚 ack 的报文落到
        # consumeQueue 有延迟，基线读成 0、对账时它变成 1，会凭空多出 1 条。
        p.send(Message(topic, b"async-a3-warmup"), 5000)
        check("A3 预热那一笔已经在 broker 上可读", _wait_landed(admin, topic, 1) >= 1,
              "landed=%d" % _landed(admin, topic))
        queues = p.fetch_publish_message_queues(topic)
        check("A3 取到了发布队列", len(queues) > 0, "queues=%d" % len(queues))
        aimed = queues[0]
        before = _queue_landed(admin, aimed)
        others_before = sum(max(0, _queue_landed(admin, q)) for q in queues[1:])
        p.send_async(Message(topic, b"async-a3-pinned"), rec, 5000, aimed)
        check("A3 定点异步发送拿到终态且 SEND_OK",
              _wait_until(lambda: rec.done >= 1, 20.0)
              and len(rec.results) == 1, rec.summary())
        if not rec.results:
            return
        r = rec.results[0]
        check("A3 结果落在指定的那条队列上",
              r.message_queue.broker_name == aimed.broker_name
              and r.message_queue.queue_id == aimed.queue_id,
              "broker=%s queueId=%d" % (r.message_queue.broker_name,
                                        r.message_queue.queue_id))
        check("A3 那条队列正好多 1 条",
              _wait_until(lambda: _queue_landed(admin, aimed) == before + 1, 20.0),
              "landed=%d 之前=%d" % (_queue_landed(admin, aimed), before))
        others_after = sum(max(0, _queue_landed(admin, q)) for q in queues[1:])
        check("A3 别的队列一条都没多", others_after == others_before,
              "其它队列 %d -> %d" % (others_before, others_after))
    finally:
        p.shutdown()


# ------------------------------------------------- A4 拦截钩子
def a4_forbidden_hook(admin, topic):
    forbidden = _ForbiddenTagHook()
    p = _producer("a4-%d" % STAMP, forbidden)
    p.start()
    rejected, passed = _Recorder(), _Recorder()
    try:
        bad = Message(topic, b"async-a4-rejected", tags="forbidden")
        p.send_async(bad, rejected, 5000)
        check("A4 钩子拒绝的异常原样到了回调",
              _wait_until(lambda: rejected.done >= 1, 10.0)
              and rejected.errors
              and "tag forbidden is not allowed" in str(rejected.errors[0]),
              rejected.summary())
        check("A4 拦截钩子看到的是 ASYNC", forbidden.mode == "ASYNC",
              "mode=%s" % forbidden.mode)
        # 这个 topic 除了被拒的这一笔什么都没有 ⇒ 要么读到 0 条，要么连路由都还没
        # 注册上（-1）。路由是**第一条消息落到 broker** 才会被 autoCreate 建出来的，
        # 所以「读不到路由」本身就是「broker 没收到过请求」的证据。
        after_reject = _landed(admin, topic)
        check("A4 被拒的这笔在 broker 上没留痕", after_reject <= 0,
              "landed=%d（-1 = 路由还没建出来，即 broker 一条都没收到）" % after_reject)

        p.send_async(Message(topic, b"async-a4-ok"), passed, 5000)
        check("A4 同一个生产者换个标签照常落地（拒绝没把池子弄坏）",
              _wait_until(lambda: passed.done >= 1, 20.0) and len(passed.results) == 1,
              passed.summary())
        check("A4 broker 上正好落 1 条", _wait_landed(admin, topic, 1) == 1,
              "landed=%d" % _landed(admin, topic))
        check("A4 钩子一共被调 2 次（一笔被拒、一笔放行）", forbidden.calls == 2,
              "calls=%d" % forbidden.calls)
    finally:
        p.shutdown()


# ------------------------------------------------- A5 批量走同步批量内核
def a5_batch_async(admin, topic):
    p = _producer("a5-%d" % STAMP)
    p.start()
    rec = _Recorder()
    try:
        msgs = [Message(topic, ("async-a5-%d" % i).encode()) for i in range(3)]
        p.send_async(msgs, rec, 8000)
        check("A5 批量异步只有一次回调且 SEND_OK",
              _wait_until(lambda: rec.done >= 1, 20.0)
              and rec.done == 1 and len(rec.results) == 1, rec.summary())
        check("A5 三条一起落了地", _wait_landed(admin, topic, 3) == 3,
              "landed=%d" % _landed(admin, topic))
        check("*  A5 请求码 = SEND_BATCH_MESSAGE(320) 由离线单测取证",
              True, "真机看不到上线报文，见 tests/test_producer_async.py")

        # 逐条 ID 的**落地**证据：读回 broker 存下来的那条子消息，它必须带客户端生成的
        # 32 位 UNIQ_KEY。Java batch():1176 的顺序是「逐条 setUniqID → 才 encode()」；
        # 顺序错了（或像修复前的本端口那样压根不写），broker 拆开批量后存的就是没有 ID 的
        # 裸消息 —— 消费端去重、轨迹控制台串线全废，而发送侧回调照样 SEND_OK，看不出来。
        r = rec.results[0] if rec.results else None
        if r is None:
            check("A5 批量读回", False, "回调没有交付 SendResult")
            return
        check("A5 批量的 msg_id 是客户端 32 位 ID、不是 broker 的 offset_msg_id",
              len(r.msg_id) == 32 and "," not in r.msg_id and r.msg_id != r.offset_msg_id,
              "msgId=%s offsetMsgId=%s" % (r.msg_id, r.offset_msg_id))
        # 批量应答的 offset_msg_id 是 broker **逐条**回的一串（逗号分隔，一条子消息一个
        # commitLog 偏移），它本身就是「这一批被拆开存成 3 条」的证据
        sub_offsets = [o for o in r.offset_msg_id.split(",") if o]
        check("A5 broker 逐条回了 3 个 commitLog 偏移（批量确实被拆开落地）",
              len(sub_offsets) == 3, "offsetMsgId=%s" % r.offset_msg_id)
        stored = [None]

        def _read_sub():
            try:
                stored[0] = admin.view_message(topic, sub_offsets[0])
                return True
            except Exception:
                return False  # commitLog 还没刷出去

        uniq = ""
        if sub_offsets and _wait_until(_read_sub, 15.0) and stored[0] is not None:
            uniq = stored[0].get_property("UNIQ_KEY") or ""
        check("A5 broker 上存的子消息带客户端 UNIQ_KEY（逐条 ID 编在 body 里）",
              len(uniq) == 32, "stored UNIQ_KEY=%s" % uniq)
    finally:
        p.shutdown()


# ------------------------------------------------- A6 Shutdown 不等在途
def a6_shutdown_does_not_wait(admin, topic):
    sends = max(1, os.cpu_count() or 1) * 3
    hook = _TracingHook(100)
    p = _producer("a6-%d" % STAMP, hook)
    p.start()
    rec = _Recorder()
    try:
        for i in range(sends):
            p.send_async(Message(topic, ("async-a6-%d" % i).encode()), rec, 8000)
        # 立刻关：Java/Python 在这里等的是**零**（.NET 才会 join 池线程）
        p.shutdown()
        returned_at = time.monotonic()
        # 每一笔仍然会跑完准备段、仍然拿到终态回调（不等待 ≠ 凭空丢任务）
        check("A6 每一笔都拿到终态回调（不等待 ≠ 回调凭空消失）",
              _wait_until(lambda: rec.done >= sends, 30.0) and rec.done == sends,
              "done=%d 发送=%d" % (rec.done, sends))
        check("A6 Shutdown 不等在途：返回之后还有回调在发",
              rec.done >= sends
              and sum(1 for t in rec.ats if t > returned_at) >= sends - 1,
              "Shutdown 返回后才有 %d/%d 笔落到终态"
              % (sum(1 for t in rec.ats if t > returned_at), sends))
        # ⚠ 但代价是真的会丢：这些准备段是在「传输层已经关掉」的客户端上跑的，实测每一笔
        # 都撞死在路由刷新的超时上（wait response on the channel ... timeout），所以整轮
        # 全部报错、broker 上一条都没落（连 topic 都没建出来）。.NET 那个版本 join 完池子
        # 才关客户端，同样的用例能落满；这里照抄 Java 的 ``shutdown()``，锁的就是
        # 「调用方必须自己等回调再关」这条。
        check("A6 不等待的代价：客户端先关，在途发送基本全部报错",
              len(rec.errors) * 2 >= sends,
              "报错=%d/%d，例如：%s" % (len(rec.errors), sends, _first(rec.errors)))
        time.sleep(2.0)
        landed = _landed(admin, topic, quiet=True)
        check("A6 broker 上落地的远少于发送条数（不等待真的会丢消息）",
              0 <= max(0, landed) * 2 < sends,
              "landed=%d 发送=%d" % (landed, sends))
    except Exception as e:  # noqa: BLE001 — Shutdown 之后不该抛，抛了就是回归
        check("A6 Shutdown 排空在途准备段", False, "%s: %s" % (type(e).__name__, e))

    p2 = _producer("a6-after-%d" % STAMP)
    p2.start()
    after = _Recorder()
    p2.send_async(Message(topic, b"async-a6-after"), after, 8000)
    check("A6 关掉的池子不会被别的生产者复用（新生产者接着能发）",
          _wait_until(lambda: after.done >= 1, 20.0) and len(after.results) == 1,
          after.summary())
    p2.shutdown()


def _broker_addr(admin):
    try:
        return admin.fetch_broker_cluster_info().get_broker_addrs()[0]
    except Exception:  # noqa: BLE001
        return "127.0.0.1:10911"


def main():
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.set_timeout_millis(10000)
    admin.start()
    topics = {
        "a1": "AsyncReadBack_%d" % STAMP,
        "a2": "AsyncBurst_%d" % STAMP,
        "a3": "AsyncPinned_%d" % STAMP,
        "a4": "AsyncForbidden_%d" % STAMP,
        "a5": "AsyncBatch_%d" % STAMP,
        "a6": "AsyncDrain_%d" % STAMP,
    }
    try:
        a1_non_blocking_and_read_back(admin, topics["a1"])
        a2_burst_exactly_once_each(admin, topics["a2"])
        a3_pinned_queue(admin, topics["a3"])
        a4_forbidden_hook(admin, topics["a4"])
        a5_batch_async(admin, topics["a5"])
        a6_shutdown_does_not_wait(admin, topics["a6"])
    finally:
        addr = _broker_addr(admin)
        for topic in topics.values():
            try:
                admin.delete_topic_in_broker(addr, topic)
            except Exception:  # noqa: BLE001 — 清理失败不影响结论
                pass
        admin.shutdown()

    failed = [name for name, ok, _ in results if not ok]
    print("\n%d PASS / %d FAIL" % (len(results) - len(failed), len(failed)))
    for name in failed:
        print("  FAILED: %s" % name)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
