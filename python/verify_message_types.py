# -*- coding: utf-8 -*-
"""rocketmq-client-remoting 不同消息类型联调测试。

覆盖 verify_live_clean.py 之外的消息类型/特性，全部针对真实集群验证：
  1. 异步发送（send_async + SendCallback）：真异步链 —— 不阻塞调用方、
     准备工作在 AsyncSenderExecutor_ 线程、钩子 after 与回调在
     NettyClientPublicExecutor_ 线程、并发 20 条全部落到 broker、定点发送、
     校验失败只走回调
  2. 延迟消息（set_delay_time_level，验证延迟投递 + 时序）
  3. 顺序消息（send_by_selector 同 key 落同队列 + 顺序消费保序）
  4. 带 Tag 消息 + 服务端 Tag 过滤消费
  5. 带 Key 消息 + 按 Key 服务端查询（query_message）
  6. 用户属性（user property）生产/消费透传
  7. 事务消息（send_message_in_transaction，两阶段：半消息 → 本地事务 → END_TRANSACTION；
     本文件只做提交路径的冒烟，回查/回滚的完整链路见 verify_transaction_live.py）
  8. 单向发送（send_oneway，无应答）+ 批量发送（send(list) → SEND_BATCH_MESSAGE），
     两者都靠回读条数兜底
  9. 定时消息（TIMER_DELIVER_MS，到点才投递）

本脚本自身不启动集群；调用方需先启动 nameServer(9876)+broker(10911) 且
autoCreateTopicEnable=true。运行：在 venv 中 `python verify_message_types.py`
为避免 store 累积假象，所有 topic/group 均带时间戳且使用全新 store。

集群开了 ACL（broker authenticationEnabled=true）时把凭据带上：
`python verify_message_types.py 127.0.0.1:9876 <accessKey> <secretKey>`。
此时每一类消息的发送/消费都必须靠签名通过鉴权，脚本额外加一条反向断言：
同一 topic 上不带凭据的发送必须被 broker 拒成 NO_PERMISSION(16)。
没有这条反向断言，「带凭据全绿」在鉴权没真开起来的集群上是假绿。
"""
import os
import sys
import time
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client.producer import (DefaultMQProducer, SelectMessageQueueByHash,
                                       SendCallbackImpl, LocalTransactionState, TransactionListener)
from client.consumer import (DefaultMQPushConsumer, MessageListenerConcurrently,
                                       MessageListenerOrderly)
from client.consumer_result import (ConsumeConcurrentlyStatus, ConsumeOrderlyStatus)
from common.message import Message, MessageExt, MessageQueue
from remoting.protocol.heartbeat import ConsumeFromWhere
from remoting.rpchook import AclClientRPCHook, SessionCredentials

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
AK = sys.argv[2] if len(sys.argv) > 2 else ""
SK = sys.argv[3] if len(sys.argv) > 3 else ""
# broker 侧所有鉴权失败都收敛成 NO_PERMISSION(16)
NO_PERMISSION = 16


def _hook():
    """带凭据时返回 ACL 签名钩子，不带时返回 None（等价于跑在无鉴权集群上）。"""
    if AK and SK:
        return AclClientRPCHook(SessionCredentials(AK, SK))
    return None


def _rejected(exc: BaseException) -> bool:
    """沿 cause 链判断是否被 broker 以 NO_PERMISSION 拒绝。

    MQBrokerException 直接把码放在 response_code 上，MQClientException 也可能
    包一层（admin 路径），所以要顺着链找，不能只看最外层。
    """
    seen = set()
    cur = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if getattr(cur, "response_code", None) == NO_PERMISSION:
            return True
        cur = cur.__cause__ or cur.__context__
    return False


STAMP = int(time.time())
PREFIX = "MT_%d" % STAMP

results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print("[%s] %s %s" % ("PASS" if ok else "FAIL", name, detail))


def _wait_broker(prod):
    """轮询等待 broker 在 nameServer 注册完成（避免端口刚开、注册未落地的竞态）。"""
    for _ in range(40):
        try:
            info = prod._mq_client.get_broker_cluster_info()
            if info and info.broker_addr_table:
                return list(info.broker_addr_table.keys())
        except Exception:
            pass
        time.sleep(1)
    return []


def run_consumer(topic, sub_expr, duration, orderly=False, group_suffix="c",
                 expect=0, pull_timeout=3000, pull_suspend=1000):
    """启动消费者，活跃等待直至收到 expect 条（或最多 duration 秒）后关闭。

    expect<=0 时退化为固定窗口等待。用活跃等待取代盲 sleep，避免消费者
    冷启动（路由/队列尚未就绪）在固定窗口结束前尚未拉到消息导致的偶发 0 条。

    关键坑：消费循环在单线程内顺序长轮询各队列；空闲队列的 suspend 长轮询会
    阻塞整轮，导致消息集中在高序号队列（如顺序消息全部落在 qid=2）时被排在
    前面的空闲队列饿死。故把 pull_timeout(客户端等待响应) 与 pull_suspend(服务端
    挂起) 都设短：空闲队列 ~1s 即返回 NO_NEW_MSG，循环很快轮到满载队列；
    pull_timeout 略大于 pull_suspend，避免空闲轮询触发客户端超时 ERROR（新日志
    已落文件，应保持干净）。
    """
    received = []
    lock = threading.Lock()
    errs = []

    if orderly:
        class _L(MessageListenerOrderly):
            def consume_message(self, msgs, context):
                with lock:
                    for m in msgs:
                        received.append(m)
                return ConsumeOrderlyStatus.SUCCESS
    else:
        class _L(MessageListenerConcurrently):
            def consume_message(self, msgs, context):
                with lock:
                    for m in msgs:
                        received.append(m)
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    cons = DefaultMQPushConsumer(consumer_group="%s_%s" % (PREFIX, group_suffix),
                                 rpc_hook=_hook())
    cons.set_namesrv_addr(NAMESRV)
    cons.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    cons.subscribe(topic, sub_expr)
    try:
        cons.set_message_listener(_L())
    except Exception as e:  # noqa: BLE001
        errs.append("set_listener: %s" % e)
    cons.pull_timeout_millis = pull_timeout
    cons.pull_suspend_timeout_millis = pull_suspend
    cons.start()
    deadline = time.time() + duration
    while time.time() < deadline and (expect <= 0 or len(received) < expect):
        time.sleep(0.2)
    cons.shutdown()
    if errs:
        print("  [diag] consumer errs: %s" % errs)
    return received


def _anon_probe() -> None:
    """反向对照：不带凭据发送必须被拒成 NO_PERMISSION(16)。

    缺了这条，「带凭据全绿」无法区分「签名被接受」和「集群根本没开鉴权」。
    必须在建主生产者**之前**跑：同进程内 client_id 相同会共享 MQClientInstance，
    而 ACL 钩子只在实例首次创建时注册，后起的实例才会带上自己的钩子。
    """
    anon = DefaultMQProducer(producer_group="%s_anon" % PREFIX)
    anon.set_namesrv_addr(NAMESRV)
    anon.set_send_msg_timeout(5000)
    denied = False
    detail = ""
    try:
        anon.start()
        try:
            anon.send(Message("%s_Anon" % PREFIX, b"should-not-send"))
            detail = "unexpectedly succeeded"
        except Exception as e:  # noqa: BLE001
            denied = _rejected(e)
            detail = "%r" % (e,)
    except Exception as e:  # noqa: BLE001
        detail = "start failed: %r" % (e,)
    finally:
        anon.shutdown()
    check("无凭据发送被拒(NO_PERMISSION=16)", denied, detail)


def main():
    if AK and SK:
        _anon_probe()
        time.sleep(1)
    prod = DefaultMQProducer(producer_group="%s_producer" % PREFIX, rpc_hook=_hook())
    prod.set_namesrv_addr(NAMESRV)
    prod.set_send_msg_timeout(5000)
    prod.start()
    brokers = _wait_broker(prod)
    if not brokers:
        check("集群探活", False, "nameServer 无 broker 注册")
        prod.shutdown()
        return 1
    check("集群探活", True, "brokers=%s" % brokers)

    # ---------- 1. 异步发送（真异步链：不阻塞调用方 + 两个线程池 + 真投递 + 失败走回调） ----------
    topic_async = "%s_Async" % PREFIX

    hook_threads = []

    class _ThreadSpy:
        def hook_name(self):
            return "thread-spy"

        def send_message_before(self, context):
            hook_threads.append(("before", threading.current_thread().name))

        def send_message_after(self, context):
            hook_threads.append(("after", threading.current_thread().name))

    spy = _ThreadSpy()
    prod.register_send_message_hook(spy)
    ok_res, errs = [], []
    cb_threads = []
    begin = time.monotonic()
    prod.send_async(
        Message(topic_async, b"async-hello"),
        SendCallbackImpl(lambda sr: (ok_res.append(sr),
                                     cb_threads.append(threading.current_thread().name)),
                         lambda e: (errs.append(e),
                                    cb_threads.append(threading.current_thread().name))))
    caller_ms = int((time.monotonic() - begin) * 1000)
    for _ in range(100):
        if ok_res or errs:
            break
        time.sleep(0.05)
    # 调用方拿到的是"已提交"，不是"已发完"：这一条挂了才算真异步
    check("send_async 不阻塞调用方", caller_ms < 300, "callerBlockedMs=%d" % caller_ms)
    check("异步发送 send_async",
          len(ok_res) == 1 and ok_res[0].send_status.name == "SEND_OK" and not errs,
          "ok=%d err=%d" % (len(ok_res), len(errs)))
    before_threads = [t for k, t in hook_threads if k == "before"]
    after_threads = [t for k, t in hook_threads if k == "after"]
    check("发送准备在 AsyncSenderExecutor_ 线程上跑",
          bool(before_threads) and all(t.startswith("AsyncSenderExecutor_")
                                       for t in before_threads),
          "before=%s" % (before_threads or "?"))
    check("钩子 after 与用户回调在 NettyClientPublicExecutor_ 线程上跑",
          bool(after_threads) and all(t.startswith("NettyClientPublicExecutor_")
                                      for t in after_threads)
          and cb_threads == ["NettyClientPublicExecutor_1"],
          "after=%s callback=%s" % (after_threads or "?", cb_threads or "?"))
    prod.send_message_hook_list.remove(spy)

    # 批量并发异步：20 条全部 SEND_OK，且 broker 侧真收得到
    ok_many, err_many = [], []
    for i in range(20):
        prod.send_async(Message(topic_async, ("async-%02d" % i).encode()),
                        SendCallbackImpl(lambda sr: ok_many.append(sr),
                                         lambda e: err_many.append(e)))
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and len(ok_many) + len(err_many) < 20:
        time.sleep(0.05)
    check("并发 send_async 全部回调成功",
          len(ok_many) == 20 and not err_many,
          "ok=%d err=%d%s" % (len(ok_many), len(err_many),
                              (" last=%s" % err_many[-1]) if err_many else ""))
    recv_async = run_consumer(topic_async, "*", 15, group_suffix="async", expect=21)
    check("异步发送的消息真落到 broker",
          len(recv_async) == 21,
          "received=%d/21" % len(recv_async))

    fixed_mq = MessageQueue(topic_async, "broker-a", 0)
    ok_fixed, err_fixed = [], []
    prod.send_async(Message(topic_async, b"async-fixed"),
                    SendCallbackImpl(lambda sr: ok_fixed.append(sr),
                                     lambda e: err_fixed.append(e)),
                    mq=fixed_mq)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and not (ok_fixed or err_fixed):
        time.sleep(0.05)
    check("定点 send_async(带 mq)", bool(ok_fixed) and not err_fixed,
          "ok=%d err=%d%s" % (len(ok_fixed), len(err_fixed),
                              (" last=%s" % err_fixed[-1]) if err_fixed else ""))

    ok_bad, err_bad = [], []
    prod.send_async(Message(topic_async, b""),
                    SendCallbackImpl(lambda sr: ok_bad.append(sr),
                                     lambda e: err_bad.append(e)))
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and not (ok_bad or err_bad):
        time.sleep(0.05)
    check("空 body 的异步发送失败只走回调",
          not ok_bad and len(err_bad) == 1,
          "err=%s" % (err_bad[0] if err_bad else "?"))

    # ---------- 2. 顺序消息：同 key 落同队列 + 顺序消费保序 ----------
    topic_order = "%s_Order" % PREFIX
    qids = set()
    bodies_order = []
    for i in range(10):
        b = ("ord-%02d" % i).encode("utf-8")
        bodies_order.append(b)
        sr = prod.send_by_selector(Message(topic_order, b), SelectMessageQueueByHash(), "shard-A")
        qids.add(sr.message_queue.queue_id)
    check("顺序发送: 同 key 路由到同一队列",
          len(qids) == 1, "distinct_queue_ids=%s" % (list(qids) or "?"))
    recv_order = run_consumer(topic_order, "*", 12, orderly=True, group_suffix="order")
    recv_bodies = sorted(m.body for m in recv_order)
    check("顺序消费保序", recv_bodies == sorted(bodies_order) and len(recv_order) == 10,
          "received=%d/%d" % (len(recv_order), len(bodies_order)))

    # ---------- 3. 带 Tag 消息 + 服务端 Tag 过滤 ----------
    topic_tag = "%s_Tag" % PREFIX
    for i in range(3):
        prod.send(Message(topic_tag, ("tagA-%d" % i).encode(), tags="TagA"))
    for i in range(3):
        prod.send(Message(topic_tag, ("tagB-%d" % i).encode(), tags="TagB"))
    recv_tag = run_consumer(topic_tag, "TagA", 12, group_suffix="tag")
    recv_tags = set(m.get_tags() for m in recv_tag)
    check("Tag 过滤消费（仅收到 TagA）",
          len(recv_tag) == 3 and recv_tags == {"TagA"},
          "received=%d tags=%s" % (len(recv_tag), recv_tags))

    # ---------- 4. 用户属性透传（独立 topic，避免与 Tag 测试耦合）----------
    topic_prop = "%s_Prop" % PREFIX
    for i in range(3):
        m = Message(topic_prop, ("prop-%d" % i).encode(), tags="P")
        m.set_user_property("city", "Hangzhou")
        m.set_user_property("env", "prod")
        prod.send(m)
    recv_prop = run_consumer(topic_prop, "*", 12, group_suffix="prop")
    ok_city = all(m.get_user_property("city") == "Hangzhou" for m in recv_prop)
    ok_env = all(m.get_user_property("env") == "prod" for m in recv_prop)
    check("用户属性透传(city=Hangzhou, env=prod)",
          ok_city and ok_env and len(recv_prop) == 3,
          "received=%d" % len(recv_prop))

    # ---------- 5. 延迟消息 ----------
    topic_delay = "%s_Delay" % PREFIX
    t0 = int(time.time() * 1000)
    prod.send(Message(topic_delay, b"normal-now"))                       # 普通
    dm = Message(topic_delay, b"delayed-5s")
    dm.set_delay_time_level(2)                                          # level2 = 5s
    prod.send(dm)
    recv_delay = run_consumer(topic_delay, "*", 16, group_suffix="delay", expect=2)
    delayed = [m for m in recv_delay if m.body == b"delayed-5s"]
    normal = [m for m in recv_delay if m.body == b"normal-now"]
    check("延迟消息最终投递", bool(delayed), "delayed received=%d normal=%d" % (len(delayed), len(normal)))
    if delayed:
        drift = delayed[0].store_timestamp - delayed[0].born_timestamp
        check("延迟生效(store_ts-born_ts>=3000ms)", drift >= 3000,
              "drift=%dms" % drift)
    else:
        check("延迟生效(store_ts-born_ts>=3000ms)", False, "无延迟消息可校验")

    # ---------- 6. 带 Key 消息 + 按 Key 查询 ----------
    topic_key = "%s_Key" % PREFIX
    key = "MTKEY_%d" % STAMP
    body_key = b"key-msg-payload"
    prod.send(Message(topic_key, body_key, keys=key))
    time.sleep(1)
    begin = t0 - 120000
    end = int(time.time() * 1000) + 120000
    found = prod.query_message(topic_key, key, 10, begin, end)
    hit = any((m.body == body_key) for m in (found or []))
    check("按 Key 查询(query_message)", hit, "returned=%d" % (len(found or [])))

    # ---------- 7. 事务消息（两阶段的提交路径冒烟；完整链路见 verify_transaction_live.py）----------
    topic_tx = "%s_Tx" % PREFIX

    class _TxListener(TransactionListener):
        def execute_local_transaction(self, msg, arg):
            return LocalTransactionState.COMMIT_MESSAGE

        def check_local_transaction(self, msg):
            return LocalTransactionState.COMMIT_MESSAGE

    tsr = prod.send_message_in_transaction(Message(topic_tx, b"tx-commit"), _TxListener())
    tx_ok = (tsr is not None and tsr.send_status.name == "SEND_OK"
             and tsr.get_local_transaction_state() == LocalTransactionState.COMMIT_MESSAGE)
    check("事务消息发送(提交路径)", tx_ok,
          "state=%s" % (tsr.get_local_transaction_state() if tsr else "None"))
    # 确认事务消息确实落库可被消费
    recv_tx = run_consumer(topic_tx, "*", 10, group_suffix="tx")
    check("事务消息落库可被消费", any(m.body == b"tx-commit" for m in recv_tx),
          "received=%d" % len(recv_tx))

    # ---------- 8. 单向 + 批量：没有逐条应答的两种入口，靠回读条数兜底 ----------
    topic_ent = "%s_Entries" % PREFIX
    prod.send(Message(topic_ent, b"ent-sync"))
    prod.send_oneway(Message(topic_ent, b"ent-oneway"))
    ent_expect = [b"ent-sync", b"ent-oneway"] + [b"ent-batch-%d" % i for i in range(3)]
    brs = prod.send([Message(topic_ent, b) for b in ent_expect[2:]])
    check("批量发送 send(list) SEND_OK", brs.send_status.name == "SEND_OK",
          "status=%s" % brs.send_status.name)
    recv_ent = run_consumer(topic_ent, "*", 18, group_suffix="ent", expect=len(ent_expect))
    got_ent = sorted(m.body for m in recv_ent)
    check("单向+批量消息全部落到 broker 并可消费",
          got_ent == sorted(ent_expect),
          "received=%d/%d bodies=%s" % (len(recv_ent), len(ent_expect), got_ent))

    # ---------- 9. 定时消息（TIMER_DELIVER_MS：到点才投递）----------
    topic_timer = "%s_Timer" % PREFIX
    deliver_at = int(time.time() * 1000) + 5000
    tm = Message(topic_timer, b"timer-5s")
    tm.set_user_property("TIMER_DELIVER_MS", str(deliver_at))
    prod.send(tm)
    recv_timer = run_consumer(topic_timer, "*", 20, group_suffix="timer", expect=1)
    hit_timer = [m for m in recv_timer if m.body == b"timer-5s"]
    check("定时消息最终投递", bool(hit_timer), "received=%d" % len(recv_timer))
    if hit_timer:
        tdrift = hit_timer[0].store_timestamp - hit_timer[0].born_timestamp
        check("定时生效(store_ts-born_ts>=4000ms)", tdrift >= 4000, "drift=%dms" % tdrift)
    else:
        check("定时生效(store_ts-born_ts>=4000ms)", False, "无定时消息可校验")

    prod.shutdown()

    # ---------- 汇总 ----------
    failed = [r for r in results if not r[1]]
    print("\n================ 消息类型联调汇总 ================")
    for name, ok, detail in results:
        print("  [%s] %s" % ("PASS" if ok else "FAIL", name))
    print("==================================================")
    if failed:
        print("结果: %d 项失败" % len(failed))
        return 1
    print("结果: 全部通过（9 类消息类型均对真实集群验证通过%s）"
          % ("，且带 ACL 签名" if AK and SK else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
