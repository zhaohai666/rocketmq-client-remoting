#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""broker 真的死了：在途请求必须**立刻**有终态（Java ``failFast`` → ``requestFail``）真机验证。

用法（需本地 RocketMQ 5.5.1 集群；脚本会**停一次再拉起** broker，不删 store）：
    .venv/bin/python verify_fail_fast_live.py 127.0.0.1:9876

离线用例（``tests/test_fail_fast.py``）锁的是传输层契约：本机假对端读完就关，断言
"毫秒级判死 + 报 RemotingSendRequestException 而不是超时 + 回调只投一次"。但这条路径
存在的意义正是**真机上的 broker 重启 / 主备切换 / 网络抖动**，只有真集群能回答这几件事：

  L1 基线：真 broker 上发送与长轮询都正常（先确认后面的失败不是环境造成的）。
  L2 挂起：把三条**真的挂在 broker 上**的长轮询（suspend 20s、客户端超时 30s）钉在在途表里。
  L3 收口：杀掉 broker（连接被关掉，客户端读线程见到 EOF）→ 长轮询必须立刻拿到
      RemotingSendRequestException，而不是等满 30s 报一个 RemotingTimeoutException。
      类型不能错：producer 的异步重试分类按异常**类型**分流（``client/producer.py``
      ``_classify_async_failure``），报成超时等于换了一整套重试决策。
  L4 范围：判死只针对死掉那条连接所在的地址；同一个传输实例上的 namesrv 连接照常服务
      （``GET_ALL_TOPIC_LIST_FROM_NAMESERVER`` 仍返回 SUCCESS）。
      ⚠ 这条只能证"按地址隔离"。"同地址换连接时旧连接的收尾不误伤新连接"那一层真机给不出
      确定性的时间窗（一台 broker 一个地址一条连接），由离线用例
      ``test_same_address_new_connection_survives_old_reader`` 负责。
  L5 恢复：broker 拉起后同一个 producer 实例重新建连照常发送；**已经拿到 SEND_OK 的消息
      一条都不能少**（快速失败不能把已经落地的说成丢了）。

L3 的阈值（8s）远小于客户端超时（30s），也小于 broker 的 suspend 上限（20s）：少了
failFast 这条断言必然失败，不是碰运气。
"""
from __future__ import annotations

import os
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rocketmq.client.consumer import DefaultMQPullConsumer
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.client.send_result import SendStatus
from rocketmq.common.message import Message
from rocketmq.common.sysflag import PullSysFlag
from rocketmq.remoting.protocol.codes import RequestCode, ResponseCode
from rocketmq.remoting.protocol.remoting_command import RemotingCommand
from rocketmq.remoting.protocol.headers import PullMessageRequestHeader

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BROKER_CTL = os.path.join(ROOT, "scripts", "rmq_test_broker.sh")
TOPIC = "FailFastPy_%d" % int(time.time() * 1000)
GROUP = "fail_fast_py_" + TOPIC

# 客户端侧超时故意放到 30s：判死若走的是超时路径，至少要等这么久。
CLIENT_TIMEOUT_MILLIS = 30000
BROKER_SUSPEND_MILLIS = 20000
# failFast 应当是毫秒级；留 8s 给真机调度（EOF 到达 + 回调投递）。
FAIL_FAST_LIMIT_SECONDS = 8.0
BASELINE_MSGS = 5

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


def broker_ctl(action: str) -> tuple:
    """跑 scripts/rmq_test_broker.sh，返回 (returncode, 输出)。

    输出走**文件**而不是管道：start 会把 broker 拉成常驻进程，管道的写端被它继承，
    ``subprocess`` 就要等到写端全部关闭才返回 —— 表现为脚本明明已经打印结果，
    调用方还是卡死。文件重定向没有这个问题。
    """
    out_path = "/tmp/rmq_fail_fast_broker_ctl.%s.log" % action
    with open(out_path, "w") as fh:
        proc = subprocess.run(["sh", BROKER_CTL, action], stdout=fh, stderr=fh,
                              timeout=600, start_new_session=True)
    with open(out_path) as fh:
        return proc.returncode, fh.read().strip()


def send_ok(producer: DefaultMQProducer, tag: str, timeout: int = 5000):
    msg = Message(TOPIC, ("fail-fast-%s" % tag).encode())
    msg.set_keys("ff-" + tag)
    return producer.send(msg, timeout)


def parked_pull_request(group: str, mq, queue_offset: int) -> RemotingCommand:
    """手工构造一条 suspend=True 的长轮询：不经 pull consumer 的钳制，
    30s 客户端超时与 20s broker suspend 都由本脚本说了算。"""
    header = PullMessageRequestHeader()
    header.consumer_group = group
    header.topic = mq.topic
    header.queue_id = mq.queue_id
    header.queue_offset = queue_offset
    header.max_msg_nums = 32
    header.sys_flag = PullSysFlag.build_sys_flag(commit_offset=False, suspend=True,
                                                 subscription=True, class_filter=False)
    header.commit_offset = 0
    header.suspend_timeout_millis = BROKER_SUSPEND_MILLIS
    header.subscription = "*"
    header.sub_version = 0
    header.expression_type = "TAG"
    header.max_msg_bytes = -1
    header.request_source = 0
    return RemotingCommand.create_request_command(RequestCode.PULL_MESSAGE, header)


def main() -> int:
    code, out = broker_ctl("status")
    if code != 0:
        print("broker 没在跑：先按本地集群 runbook 起 namesrv + broker（%s）" % out)
        return 2

    producer = DefaultMQProducer(GROUP + "_p")
    producer.set_namesrv_addr(NAMESRV)
    producer.set_instance_name("ff_py_%d" % int(time.time()))
    consumer = DefaultMQPullConsumer(GROUP)
    consumer.set_namesrv_addr(NAMESRV)
    consumer.set_instance_name("ff_py_%d" % int(time.time()))
    threads: list = []

    try:
        # ------------------------------------------------------------------ L1
        producer.start()
        results = [send_ok(producer, "base-%d" % i) for i in range(BASELINE_MSGS)]
        landed = sum(1 for r in results if r.send_status == SendStatus.SEND_OK)
        check("L1 基线：%d 条同步发送 SEND_OK" % BASELINE_MSGS, landed == BASELINE_MSGS,
              "landed=%d" % landed)

        consumer.start()
        queues = consumer.fetch_subscribe_message_queues(TOPIC)
        check("L1 取到队列", bool(queues), "queues=%d" % len(queues))
        if not queues:
            return 1
        mq = queues[0]
        client = consumer._mq_client
        broker_addr = client.find_broker_addr_in_route(client.get_topic_route_data(TOPIC),
                                                      mq.broker_name)
        check("L1 拿到 broker 地址", bool(broker_addr), "addr=%s" % broker_addr)
        if not broker_addr:
            return 1
        max_offset = consumer.max_offset(mq)
        check("L1 待挂长轮询的队列有位点可用", max_offset is not None,
              "mq=%s max_offset=%s" % (mq, max_offset))
        if max_offset is None:
            return 1
        # 发送是跨队列轮转的，单条队列的队尾只覆盖落在它上面的那部分，
        # 所以"5 条真的落盘"要看全部队列的合计。
        total_max_offset = max_offset
        for q in queues[1:]:
            other = consumer.max_offset(q)
            if other is not None:
                total_max_offset += other
        check("L1 各队列队尾位点合计覆盖刚发的 %d 条（真的落盘）" % BASELINE_MSGS,
              total_max_offset >= BASELINE_MSGS, "max_offset 合计=%d" % total_max_offset)

        # ------------------------------------------------------------------ L2
        remoting = client.remoting_client
        baseline_in_flight = len(remoting._response_table)
        parked: list = []
        park_done = threading.Event()
        park_lock = threading.Lock()

        def park_one(tag: str) -> None:
            req = parked_pull_request(GROUP, mq, max_offset)
            started = time.monotonic()
            try:
                resp = remoting.invoke_sync(broker_addr, req, CLIENT_TIMEOUT_MILLIS)
                outcome, err = "response:%s" % resp.code, None
            except BaseException as e:  # noqa: BLE001 - 要的就是异常类型
                outcome, err = type(e).__name__, e
            cost = time.monotonic() - started
            with park_lock:
                parked.append((tag, outcome, cost, err))
                if len(parked) == 3:
                    park_done.set()

        threads = [threading.Thread(target=park_one, args=("park-%d" % i,)) for i in range(3)]
        for t in threads:
            t.start()
        time.sleep(2.0)
        still_parked = sum(1 for t in threads if t.is_alive())
        check("L2 三条长轮询真的挂在 broker 上（2s 后仍未返回）", still_parked == 3,
              "parked=%d" % still_parked)
        inflight = len(remoting._response_table)
        check("L2 在途表里有它们", inflight >= baseline_in_flight + 3,
              "in_flight=%d (baseline=%d)" % (inflight, baseline_in_flight))
        kill_at = time.monotonic()

        # ------------------------------------------------------------------ L3
        code, out = broker_ctl("stop")
        check("L3 停掉 broker", code == 0, out.replace("\n", " | "))
        got_all = park_done.wait(60.0)
        for t in threads:
            t.join(1.0)
        check("L3 挂起的长轮询全部返回（没有卡死）", got_all and len(parked) == 3,
              "got=%d" % len(parked))
        types = sorted({p[1] for p in parked})
        send_fail = [p for p in parked if p[1] == "RemotingSendRequestException"]
        timeout_like = [p for p in parked if "Timeout" in p[1]]
        check("L3 报的是 RemotingSendRequestException（Java failFast 的口径）",
              len(send_fail) == 3, "types=%s" % types)
        check("L3 一条都没被报成超时（类型错 = 重试决策错）", not timeout_like,
              "timeout=%d" % len(timeout_like))
        worst = max((p[2] for p in parked), default=0.0)
        check("L3 判死耗时远小于 %ds 客户端超时" % (CLIENT_TIMEOUT_MILLIS // 1000),
              worst < FAIL_FAST_LIMIT_SECONDS,
              "worst=%.2fs limit=%.0fs" % (worst, FAIL_FAST_LIMIT_SECONDS))
        if send_fail:
            check("L3 异常文案带着断连原因", "connection closed" in str(send_fail[0][3]),
                  str(send_fail[0][3]))
        left = len(remoting._response_table)
        check("L3 判死之后在途表排空", left < inflight, "in_flight=%d" % left)

        # ------------------------------------------------------------------ L4
        try:
            req = RemotingCommand.create_request_command(
                RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, None)
            resp = remoting.invoke_sync(NAMESRV, req, 3000)
            check("L4 namesrv 连接没被牵连（broker 死了它还在服务）",
                  resp.code == ResponseCode.SUCCESS, "code=%s" % resp.code)
        except BaseException as e:  # noqa: BLE001
            check("L4 namesrv 连接没被牵连（broker 死了它还在服务）", False, repr(e))

        # ------------------------------------------------------------------ L5
        code, out = broker_ctl("start")
        check("L5 broker 重新拉起", code == 0, out.replace("\n", " | ")[:160])
        recovered, err = None, None
        deadline = time.monotonic() + 150.0
        while time.monotonic() < deadline:
            try:
                recovered = send_ok(producer, "after-restart")
                break
            except BaseException as e:  # noqa: BLE001 - 恢复窗口内允许失败
                err, recovered = e, None
                time.sleep(2.0)
        check("L5 同一个 producer 实例重新建连后照常发送",
              recovered is not None and recovered.send_status == SendStatus.SEND_OK,
              "" if recovered else repr(err))

        # 已经拿到 SEND_OK 的消息必须还在：快速失败不能把已落地的说成丢了。
        # 重启后路由要重新注册，失败就重试几次。
        total_after, tried = 0, 0
        while tried < 10:
            tried += 1
            total_after = 0
            try:
                for q in consumer.fetch_subscribe_message_queues(TOPIC):
                    total_after += consumer.max_offset(q) or 0
                if total_after >= BASELINE_MSGS:
                    break
            except BaseException:  # noqa: BLE001 - 路由还没回来
                pass
            time.sleep(2.0)
        check("L5 重启后 broker 上仍有那 %d 条 SEND_OK 的消息" % BASELINE_MSGS,
              total_after >= BASELINE_MSGS, "max_offset 合计=%d" % total_after)
    finally:
        for t in threads:
            if t.is_alive():
                t.join(1.0)
        try:
            consumer.shutdown()
        except BaseException:  # noqa: BLE001
            pass
        try:
            producer.shutdown()
        except BaseException:  # noqa: BLE001
            pass
        # 中途抛异常也不能把集群留在停机状态：这条脚本借用了大家的测试集群。
        try:
            if broker_ctl("status")[0] != 0:
                print("  [WARN] 收尾时 broker 仍停着，重新拉起")
                code, out = broker_ctl("start")
                print("  [WARN] broker start: %s" % out.replace("\n", " | ")[:160])
        except BaseException as e:  # noqa: BLE001
            print("  [WARN] 收尾拉起 broker 失败：%r（需手工重启，见本地集群 runbook）" % e)

    print("\n%d PASS / %d FAIL" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
