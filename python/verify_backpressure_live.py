# -*- coding: utf-8 -*-
"""异步发送背压（``enableBackpressureForAsyncMode`` 那一套信号量）真机验证。

离线单测（``tests/test_producer_async.py`` 的「异步发送背压」一节 +
``tests/test_backpressure.py``）锁的是语义；这个脚本锁的是**打到真 broker 时**的四件事：

  B1  默认容量（1024 条 / 100M 字节）开着背压发一整轮异步消息：全部 SEND_OK，
      且发完之后两个信号量**满额归还**（真机不泄配额 —— 泄了迟早把生产者自己锁死）。
  B2  条数闸夹到地板值 10、用 ``SendMessageHook.before`` 睡 600ms 把在途占满：
      第 11、12 笔在**调用方线程**上等不到许可，回调
      ``send message tryAcquire semaphoreAsyncNum timeout``（Java :654-658 原文案），
      而且 broker 上**一条都没多** —— 被拒的请求连路由都没查。
  B3  运行时把容量从 10 调到 12：正卡在闸上的调用方被叫醒，broker 上多出那 1 条，
      全部落地后空闲许可 = 新容量（这一轮在途睡 2s，留出足够的观察窗口）。
  B4  字节闸（容量 1M 地板值 + 600KB body ⇒ 在途只能 1 笔）：第二笔回调
      ``send message tryAcquire semaphoreAsyncSize timeout``（Java :667-671），
      在途时空闲字节许可正好是 ``1M - 600K``，broker 上只落 1 条。
  B5  关掉背压：同样的容量配置**完全不限流**，30 笔并发（含 300KB 大 body）全部落地。

前置：NameServer + Broker 已起，`autoCreateTopicEnable=true`。

用法：.venv/bin/python verify_backpressure_live.py [127.0.0.1:9876]
"""
from __future__ import annotations

import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rocketmq.client.admin import DefaultMQAdminExt
from rocketmq.client.backpressure import MIN_ASYNC_SEND_NUM, MIN_ASYNC_SEND_SIZE
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.client.send_result import SendStatus
from rocketmq.common.message import Message
from rocketmq.remoting.exception import RemotingTooMuchRequestException

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time())
GROUP = "PID_rmq_bp_%d" % STAMP

results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print("[%s] %s %s" % ("PASS" if ok else "FAIL", name, detail))


class _Recorder:
    """线程安全的回调记录（SendCallback 的鸭子类型）。"""

    def __init__(self):
        self.lock = threading.Lock()
        self.results = []
        self.errors = []

    def on_success(self, send_result):
        with self.lock:
            self.results.append(send_result)

    def on_exception(self, e):
        with self.lock:
            self.errors.append(e)

    @property
    def done(self):
        with self.lock:
            return len(self.results) + len(self.errors)

    def summary(self):
        with self.lock:
            first = str(self.errors[0]) if self.errors else ""
            return "ok=%d err=%d %s" % (len(self.results), len(self.errors), first)


class _SlowHook:
    """在 ``sendKernelImpl`` 的 before 钩子里睡一会儿。

    许可是在**闸上**拿的、在**链的终点**还的，所以占住在途最干净的办法是让链本身变慢：
    堵用户回调占不住许可（归还就在把结果交给用户之前一步）。
    """

    def __init__(self, millis=300):
        self.millis = millis

    def hook_name(self):
        return "slow-before"

    def send_message_before(self, context):
        time.sleep(self.millis / 1000.0)

    def send_message_after(self, context):
        pass


def _producer(instance_name, enable=True, num=None, size=None) -> DefaultMQProducer:
    p = DefaultMQProducer(GROUP)
    p.set_namesrv_addr(NAMESRV)
    p.set_instance_name(instance_name)
    p.set_enable_backpressure_for_async_mode(enable)
    if num is not None:
        p.set_back_pressure_for_async_send_num(num)
    if size is not None:
        p.set_back_pressure_for_async_send_size(size)
    return p


def _wait_until(pred, timeout=20.0, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


def _start_all(threads):
    for t in threads:
        t.start()


def _join_all(threads, timeout=30.0):
    deadline = time.monotonic() + timeout
    for t in threads:
        t.join(max(0.0, deadline - time.monotonic()))
    stuck = sum(1 for t in threads if t.is_alive())
    check("*  发送线程没有卡死", stuck == 0, "%d 条还活着" % stuck)


def _landed_count(admin, topic, quiet=False):
    """broker 上这个 topic 一共落了多少条（各队列 maxOffset-minOffset 之和）。

    这是「被拒的发送连请求都没发出去」的唯一硬证据：只看客户端回调的话，
    一个「回调报错但请求其实发出去了」的实现也能骗过去。读不到路由返回 -1。
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

    新建 topic 要等 broker 把 topicConfig 增量注册到 namesrv（秒级到十秒级），
    所以头几次读不到路由不算失败，重试到超时才算。
    """
    deadline = time.time() + timeout
    landed = _landed_count(admin, topic, quiet=True)
    while landed < expected and time.time() < deadline:
        time.sleep(0.5)
        landed = _landed_count(admin, topic, quiet=True)
    if landed < 0:
        _landed_count(admin, topic)      # 真读不到时把原因打出来，方便定位
    return landed


def _num_permits(p):
    return p.get_semaphore_async_send_num_available_permits()


def _size_permits(p):
    return p.get_semaphore_async_send_size_available_permits()


def main():
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.set_timeout_millis(10000)
    admin.start()
    topics = []
    try:
        # ------------------------------------------------ B1 默认容量不漏配额
        t1 = "BpDefault_%d" % STAMP
        topics.append(t1)
        p1 = _producer("bp-b1-%d" % STAMP)
        p1.start()
        rec = _Recorder()
        b1_threads = [threading.Thread(
            target=lambda i=i: p1.send_async(
                Message(t1, ("b1-%d" % i).encode() * 40), rec, 5000))
            for i in range(40)]
        _start_all(b1_threads)
        _join_all(b1_threads)
        check("B1 40 笔异步都走完回调", _wait_until(lambda: rec.done >= 40, timeout=15.0),
              rec.summary())
        check("B1 全部 SEND_OK",
              all(r.send_status == SendStatus.SEND_OK for r in rec.results)
              and len(rec.results) == 40, rec.summary())
        landed = _wait_landed(admin, t1, 40)
        check("B1 broker 上正好落了 40 条", landed == 40, "landed=%d" % landed)
        check("B1 条数许可满额归还", _num_permits(p1) == 1024,
              "available=%d" % _num_permits(p1))
        check("B1 字节许可满额归还", _size_permits(p1) == 100 * 1024 * 1024,
              "available=%d" % _size_permits(p1))
        p1.shutdown()

        # ------------------------------------- B2/B3 条数闸：拒绝、对账、扩容
        t2 = "BpNumGate_%d" % STAMP
        topics.append(t2)
        p2 = _producer("bp-b2-%d" % STAMP, num=MIN_ASYNC_SEND_NUM)
        slow2 = _SlowHook(600)
        p2.register_send_message_hook(slow2)
        p2.start()
        held, rejected, woken = _Recorder(), _Recorder(), _Recorder()
        holders = [threading.Thread(target=lambda: p2.send_async(
            Message(t2, b"held"), held, 8000)) for _ in range(MIN_ASYNC_SEND_NUM)]
        _start_all(holders)
        check("B2 在途占满后空闲条数为 0",
              _wait_until(lambda: _num_permits(p2) == 0, timeout=3.0),
              "available=%d" % _num_permits(p2))
        # 第 11、12 笔：预算只有 150ms，等不到许可
        rejected_threads = [threading.Thread(target=lambda: p2.send_async(
            Message(t2, b"rejected"), rejected, 150)) for _ in range(2)]
        began = time.monotonic()
        _start_all(rejected_threads)
        _join_all(rejected_threads, timeout=10.0)
        check("B2 闸等到预算耗尽才报错（不是看一眼就拒）",
              int((time.monotonic() - began) * 1000) >= 130,
              "调用方等了 %dms" % int((time.monotonic() - began) * 1000))
        check("B2 超限的两笔回调 TooMuchRequest，文案与 Java 逐字一致",
              len(rejected.errors) == 2
              and all(isinstance(e, RemotingTooMuchRequestException) for e in rejected.errors)
              and all("send message tryAcquire semaphoreAsyncNum timeout" in str(e)
                      for e in rejected.errors),
              rejected.summary())
        _join_all(holders, timeout=25.0)
        check("B2 在途的 10 笔都发出去了",
              _wait_until(lambda: len(held.results) == MIN_ASYNC_SEND_NUM, timeout=20.0),
              held.summary())
        landed2 = _wait_landed(admin, t2, MIN_ASYNC_SEND_NUM)
        check("B2 被拒的两笔在 broker 上一条没留（连请求都没发）",
              landed2 == MIN_ASYNC_SEND_NUM,
              "landed=%d 期望 %d" % (landed2, MIN_ASYNC_SEND_NUM))
        check("B2 全部落地后条数许可回到 10",
              _wait_until(lambda: _num_permits(p2) == MIN_ASYNC_SEND_NUM, timeout=20.0),
              "available=%d" % _num_permits(p2))

        # B3：再占满 10 个在途，把容量抬到 12 —— 卡在闸上的人应当被叫醒。
        # 这一轮把钩子睡到 2s：占住在途的时间必须远大于「起线程 + 轮询确认 + 起等待方」
        # 这几步的开销，否则检查还没做完整轮就已经归还了（300ms 时实测就会漏）。
        slow2.millis = 2000
        holders2 = _Recorder()
        round2 = [threading.Thread(target=lambda: p2.send_async(
            Message(t2, b"held2"), holders2, 8000)) for _ in range(MIN_ASYNC_SEND_NUM)]
        _start_all(round2)
        check("B3 第二轮在途同样占满", _wait_until(lambda: _num_permits(p2) == 0, timeout=3.0),
              "available=%d" % _num_permits(p2))
        waiter = threading.Thread(target=lambda: p2.send_async(
            Message(t2, b"woken"), woken, 8000))
        waiter.start()
        time.sleep(0.3)
        check("B3 扩容前调用方确实卡在闸上", waiter.is_alive() and _num_permits(p2) == 0,
              "alive=%s available=%d" % (waiter.is_alive(), _num_permits(p2)))
        p2.set_back_pressure_for_async_send_num(MIN_ASYNC_SEND_NUM + 2)
        waiter.join(15.0)
        # 调用方线程被叫醒就算「过闸了」，但结果要等整条链跑完（这一轮钩子睡 2s）才回到回调，
        # 所以这里等的是回调，不是线程退出。
        check("B3 扩容把卡在闸上的发送方叫醒并发了出去",
              not waiter.is_alive() and _wait_until(
                  lambda: any(r.send_status == SendStatus.SEND_OK for r in woken.results),
                  timeout=20.0),
              woken.summary())
        _join_all(round2, timeout=25.0)
        check("B3 全部归还后空闲许可 = 新容量 12",
              _wait_until(lambda: _num_permits(p2) == MIN_ASYNC_SEND_NUM + 2, timeout=25.0),
              "available=%d" % _num_permits(p2))
        expect3 = 2 * MIN_ASYNC_SEND_NUM + 1
        landed3 = _wait_landed(admin, t2, expect3)
        check("B3 broker 总数 = 两轮在途 + 被叫醒的那一笔", landed3 == expect3,
              "landed=%d 期望 %d" % (landed3, expect3))
        p2.shutdown()

        # ------------------------------------------- B4 字节闸（1M 地板 + 600KB）
        t3 = "BpSizeGate_%d" % STAMP
        topics.append(t3)
        p3 = _producer("bp-b4-%d" % STAMP, num=MIN_ASYNC_SEND_NUM,
                       size=MIN_ASYNC_SEND_SIZE)
        p3.register_send_message_hook(_SlowHook(400))
        p3.start()
        big = b"4" * (600 * 1024)
        one, two = _Recorder(), _Recorder()
        th_big = threading.Thread(target=lambda: p3.send_async(Message(t3, big), one, 8000))
        th_big.start()
        check("B4 在途字节许可正好扣掉 body 长度",
              _wait_until(lambda: _size_permits(p3) == MIN_ASYNC_SEND_SIZE - len(big),
                          timeout=3.0),
              "available=%d 期望 %d" % (_size_permits(p3), MIN_ASYNC_SEND_SIZE - len(big)))
        th_two = threading.Thread(target=lambda: p3.send_async(Message(t3, big), two, 150))
        th_two.start()
        th_two.join(10.0)
        check("B4 第二笔 600KB 过不了字节闸，文案与 Java 逐字一致",
              len(two.errors) == 1
              and "send message tryAcquire semaphoreAsyncSize timeout" in str(two.errors[0]),
              two.summary())
        check("B4 字节闸没过时条数许可已经归还",
              _wait_until(lambda: _num_permits(p3) == MIN_ASYNC_SEND_NUM, timeout=3.0),
              "available=%d" % _num_permits(p3))
        th_big.join(25.0)
        check("B4 第一笔正常落地", _wait_until(lambda: one.done >= 1, timeout=20.0),
              one.summary())
        landed4 = _wait_landed(admin, t3, 1)
        check("B4 broker 上只有第一笔（被拒的没留痕）", landed4 == 1, "landed=%d" % landed4)
        check("B4 全部归还：条数与字节都回到配置额",
              _num_permits(p3) == MIN_ASYNC_SEND_NUM
              and _size_permits(p3) == MIN_ASYNC_SEND_SIZE,
              "num=%d size=%d" % (_num_permits(p3), _size_permits(p3)))
        p3.shutdown()

        # ------------------------------------------------ B5 关掉背压就不限流
        t4 = "BpOff_%d" % STAMP
        topics.append(t4)
        p4 = _producer("bp-b5-%d" % STAMP, enable=False, num=MIN_ASYNC_SEND_NUM,
                       size=MIN_ASYNC_SEND_SIZE)
        p4.register_send_message_hook(_SlowHook(200))
        p4.start()
        rec5 = _Recorder()

        def _body(i):
            # 每三笔里有一笔 300KB：关着背压时连字节闸都不看，开着的话这里必然限流
            return Message(t4, b"x" * (300 * 1024) if i % 3 == 0 else b"small")

        b5_threads = [threading.Thread(
            target=lambda i=i: p4.send_async(_body(i), rec5, 8000))
            for i in range(30)]
        _start_all(b5_threads)
        _join_all(b5_threads)
        check("B5 关背压后 30 笔并发全部成功", _wait_until(lambda: rec5.done >= 30, timeout=25.0),
              rec5.summary())
        check("B5 全部 SEND_OK", len(rec5.results) == 30, rec5.summary())
        landed5 = _wait_landed(admin, t4, 30)
        check("B5 broker 上 30 条都在", landed5 == 30, "landed=%d" % landed5)
        p4.shutdown()
    finally:
        addr = _broker_addr(admin)
        for topic in topics:
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


def _broker_addr(admin):
    try:
        return admin.fetch_broker_cluster_info().get_broker_addrs()[0]
    except Exception:  # noqa: BLE001
        return "127.0.0.1:10911"


if __name__ == "__main__":
    sys.exit(main())
