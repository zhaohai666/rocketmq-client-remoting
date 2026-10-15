# -*- coding: utf-8 -*-
"""Interop tests of the different message types for rocketmq-client-remoting.

Message types/features not covered by verify_live_clean.py, all verified against a real cluster:
  1. async send (send_async + SendCallback): a truly async chain -- does not block the caller,
     preparation runs on the AsyncSenderExecutor_ thread, the hook after and the callback on the
     NettyClientPublicExecutor_ thread, 20 concurrent sends all land on the broker, pinned send,
     and validation failures go through the callback only
  2. delayed messages (set_delay_time_level, verifying delayed delivery + timing)
  3. ordered messages (send_by_selector with the same key lands on the same queue + order preserved by ordered consumption)
  4. tagged messages + server-side Tag filtering on consume
  5. keyed messages + server-side query by key (query_message)
  6. user property pass-through on produce/consume
  7. transactional messages (send_message_in_transaction, two phases: half message -> local transaction -> END_TRANSACTION;
     this file only smoke-tests the commit path; the full check/rollback chain is in verify_transaction_live.py)
  8. oneway send (send_oneway, no response) + batch send (send(list) -> SEND_BATCH_MESSAGE),
     both are backed by read-back counts
  9. timer messages (TIMER_DELIVER_MS, delivered only when the time comes)

This script does not start the cluster itself; the caller must start nameServer(9876)+broker(10911) first with
autoCreateTopicEnable=true. Run: `python verify_message_types.py` in the venv
To avoid the store-accumulation artifact, all topics/groups carry a timestamp and use a brand-new store.

When the cluster has ACL enabled (broker authenticationEnabled=true), pass the credentials:
`python verify_message_types.py 127.0.0.1:9876 <accessKey> <secretKey>`.
In that case every message type's send/consume must pass auth via signing, and the script adds one extra reverse assertion:
a send on the same topic without credentials must be rejected by the broker with NO_PERMISSION(16).
Without this reverse assertion, "all green with credentials" is a false green on a cluster where auth is not really on.
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
# On the broker side all auth failures converge into NO_PERMISSION(16)
NO_PERMISSION = 16


def _hook():
    """Returns the ACL signing hook when credentials are given, otherwise None (equivalent to running on a cluster without auth)."""
    if AK and SK:
        return AclClientRPCHook(SessionCredentials(AK, SK))
    return None


def _rejected(exc: BaseException) -> bool:
    """Walk the cause chain to decide whether the broker rejected it with NO_PERMISSION.

    MQBrokerException puts the code directly on response_code, and MQClientException may
    wrap a layer (admin path), so you must follow the chain and not only look at the outermost layer.
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
    """Poll-wait for the broker to finish registering with nameServer (avoids the race where the port is open but registration has not landed)."""
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
    """Start the consumer, actively wait until expect messages are received (or at most duration seconds), then shut down.

    When expect<=0 this degrades to a fixed-window wait. Active waiting replaces a blind sleep, avoiding the occasional 0 messages
    caused by the consumer's cold start (route/queue not yet ready) not having pulled anything before the fixed window ends.

    Key pitfall: the consume loop sequentially long-polls each queue in a single thread; an idle queue's suspend long-poll blocks
    the whole round, so when messages concentrate on a high-index queue (e.g. all ordered messages landing on qid=2), they are starved
    by the idle queues ahead of it. Hence both pull_timeout(client wait for response) and pull_suspend(server
    suspend) are set short: an idle queue returns NO_NEW_MSG in ~1s and the loop quickly reaches the loaded queue;
    pull_timeout is slightly larger than pull_suspend, to avoid idle polling triggering a client timeout ERROR (new logs
    are written to a file and should stay clean).
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
    """Reverse control: a send without credentials must be rejected with NO_PERMISSION(16).

    Without this, "all green with credentials" cannot tell "the signature was accepted" from "the cluster has no auth at all".
    It must run before the main producer is created: within the same process instances with the same client_id share the MQClientInstance,
    and the ACL hook is only registered when the instance is first created, so only the later-started instance carries its own hook.
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
    check("send without credentials rejected (NO_PERMISSION=16)", denied, detail)


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
        check("cluster probe", False, "no broker registered with nameServer")
        prod.shutdown()
        return 1
    check("cluster probe", True, "brokers=%s" % brokers)

    # ---------- 1. async send (a truly async chain: non-blocking caller + two thread pools + real delivery + failures via callback) ----------
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
    # What the caller gets is "submitted", not "finished sending": this one counts only if it hangs
    check("send_async does not block the caller", caller_ms < 300, "callerBlockedMs=%d" % caller_ms)
    check("async send send_async",
          len(ok_res) == 1 and ok_res[0].send_status.name == "SEND_OK" and not errs,
          "ok=%d err=%d" % (len(ok_res), len(errs)))
    before_threads = [t for k, t in hook_threads if k == "before"]
    after_threads = [t for k, t in hook_threads if k == "after"]
    check("send preparation runs on the AsyncSenderExecutor_ thread",
          bool(before_threads) and all(t.startswith("AsyncSenderExecutor_")
                                       for t in before_threads),
          "before=%s" % (before_threads or "?"))
    check("the hook after and the user callback run on the NettyClientPublicExecutor_ thread",
          bool(after_threads) and all(t.startswith("NettyClientPublicExecutor_")
                                      for t in after_threads)
          and cb_threads == ["NettyClientPublicExecutor_1"],
          "after=%s callback=%s" % (after_threads or "?", cb_threads or "?"))
    prod.send_message_hook_list.remove(spy)

    # Concurrent batch async: all 20 SEND_OK, and the broker really receives them
    ok_many, err_many = [], []
    for i in range(20):
        prod.send_async(Message(topic_async, ("async-%02d" % i).encode()),
                        SendCallbackImpl(lambda sr: ok_many.append(sr),
                                         lambda e: err_many.append(e)))
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and len(ok_many) + len(err_many) < 20:
        time.sleep(0.05)
    check("concurrent send_async all callbacks succeeded",
          len(ok_many) == 20 and not err_many,
          "ok=%d err=%d%s" % (len(ok_many), len(err_many),
                              (" last=%s" % err_many[-1]) if err_many else ""))
    recv_async = run_consumer(topic_async, "*", 15, group_suffix="async", expect=21)
    check("async-sent messages really land on the broker",
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
    check("pinned send_async (with mq)", bool(ok_fixed) and not err_fixed,
          "ok=%d err=%d%s" % (len(ok_fixed), len(err_fixed),
                              (" last=%s" % err_fixed[-1]) if err_fixed else ""))

    ok_bad, err_bad = [], []
    prod.send_async(Message(topic_async, b""),
                    SendCallbackImpl(lambda sr: ok_bad.append(sr),
                                     lambda e: err_bad.append(e)))
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and not (ok_bad or err_bad):
        time.sleep(0.05)
    check("an async send with an empty body fails via the callback only",
          not ok_bad and len(err_bad) == 1,
          "err=%s" % (err_bad[0] if err_bad else "?"))

    # ---------- 2. ordered messages: the same key lands on the same queue + order preserved by ordered consumption ----------
    topic_order = "%s_Order" % PREFIX
    qids = set()
    bodies_order = []
    for i in range(10):
        b = ("ord-%02d" % i).encode("utf-8")
        bodies_order.append(b)
        sr = prod.send_by_selector(Message(topic_order, b), SelectMessageQueueByHash(), "shard-A")
        qids.add(sr.message_queue.queue_id)
    check("ordered send: the same key routes to the same queue",
          len(qids) == 1, "distinct_queue_ids=%s" % (list(qids) or "?"))
    recv_order = run_consumer(topic_order, "*", 12, orderly=True, group_suffix="order")
    recv_bodies = sorted(m.body for m in recv_order)
    check("ordered consumption preserves order", recv_bodies == sorted(bodies_order) and len(recv_order) == 10,
          "received=%d/%d" % (len(recv_order), len(bodies_order)))

    # ---------- 3. tagged messages + server-side Tag filtering ----------
    topic_tag = "%s_Tag" % PREFIX
    for i in range(3):
        prod.send(Message(topic_tag, ("tagA-%d" % i).encode(), tags="TagA"))
    for i in range(3):
        prod.send(Message(topic_tag, ("tagB-%d" % i).encode(), tags="TagB"))
    recv_tag = run_consumer(topic_tag, "TagA", 12, group_suffix="tag")
    recv_tags = set(m.get_tags() for m in recv_tag)
    check("Tag filtering on consume (only TagA is received)",
          len(recv_tag) == 3 and recv_tags == {"TagA"},
          "received=%d tags=%s" % (len(recv_tag), recv_tags))

    # ---------- 4. user property pass-through (a separate topic, to avoid coupling with the Tag test) ----------
    topic_prop = "%s_Prop" % PREFIX
    for i in range(3):
        m = Message(topic_prop, ("prop-%d" % i).encode(), tags="P")
        m.set_user_property("city", "Hangzhou")
        m.set_user_property("env", "prod")
        prod.send(m)
    recv_prop = run_consumer(topic_prop, "*", 12, group_suffix="prop")
    ok_city = all(m.get_user_property("city") == "Hangzhou" for m in recv_prop)
    ok_env = all(m.get_user_property("env") == "prod" for m in recv_prop)
    check("user property pass-through (city=Hangzhou, env=prod)",
          ok_city and ok_env and len(recv_prop) == 3,
          "received=%d" % len(recv_prop))

    # ---------- 5. delayed messages ----------
    topic_delay = "%s_Delay" % PREFIX
    t0 = int(time.time() * 1000)
    prod.send(Message(topic_delay, b"normal-now"))                       # normal
    dm = Message(topic_delay, b"delayed-5s")
    dm.set_delay_time_level(2)                                          # level2 = 5s
    prod.send(dm)
    recv_delay = run_consumer(topic_delay, "*", 16, group_suffix="delay", expect=2)
    delayed = [m for m in recv_delay if m.body == b"delayed-5s"]
    normal = [m for m in recv_delay if m.body == b"normal-now"]
    check("the delayed message is eventually delivered", bool(delayed), "delayed received=%d normal=%d" % (len(delayed), len(normal)))
    if delayed:
        drift = delayed[0].store_timestamp - delayed[0].born_timestamp
        check("the delay takes effect (store_ts-born_ts>=3000ms)", drift >= 3000,
              "drift=%dms" % drift)
    else:
        check("the delay takes effect (store_ts-born_ts>=3000ms)", False, "no delayed message to verify")

    # ---------- 6. keyed messages + query by key ----------
    topic_key = "%s_Key" % PREFIX
    key = "MTKEY_%d" % STAMP
    body_key = b"key-msg-payload"
    prod.send(Message(topic_key, body_key, keys=key))
    time.sleep(1)
    begin = t0 - 120000
    end = int(time.time() * 1000) + 120000
    found = prod.query_message(topic_key, key, 10, begin, end)
    hit = any((m.body == body_key) for m in (found or []))
    check("query by key (query_message)", hit, "returned=%d" % (len(found or [])))

    # ---------- 7. transactional messages (commit-path smoke of the two phases; the full chain is in verify_transaction_live.py) ----------
    topic_tx = "%s_Tx" % PREFIX

    class _TxListener(TransactionListener):
        def execute_local_transaction(self, msg, arg):
            return LocalTransactionState.COMMIT_MESSAGE

        def check_local_transaction(self, msg):
            return LocalTransactionState.COMMIT_MESSAGE

    tsr = prod.send_message_in_transaction(Message(topic_tx, b"tx-commit"), _TxListener())
    tx_ok = (tsr is not None and tsr.send_status.name == "SEND_OK"
             and tsr.get_local_transaction_state() == LocalTransactionState.COMMIT_MESSAGE)
    check("transactional message send (commit path)", tx_ok,
          "state=%s" % (tsr.get_local_transaction_state() if tsr else "None"))
    # Confirm the transactional message really lands and can be consumed
    recv_tx = run_consumer(topic_tx, "*", 10, group_suffix="tx")
    check("the transactional message lands and can be consumed", any(m.body == b"tx-commit" for m in recv_tx),
          "received=%d" % len(recv_tx))

    # ---------- 8. oneway + batch: two entries without per-message responses, backed by read-back counts ----------
    topic_ent = "%s_Entries" % PREFIX
    prod.send(Message(topic_ent, b"ent-sync"))
    prod.send_oneway(Message(topic_ent, b"ent-oneway"))
    ent_expect = [b"ent-sync", b"ent-oneway"] + [b"ent-batch-%d" % i for i in range(3)]
    brs = prod.send([Message(topic_ent, b) for b in ent_expect[2:]])
    check("batch send(list) SEND_OK", brs.send_status.name == "SEND_OK",
          "status=%s" % brs.send_status.name)
    recv_ent = run_consumer(topic_ent, "*", 18, group_suffix="ent", expect=len(ent_expect))
    got_ent = sorted(m.body for m in recv_ent)
    check("oneway+batch messages all land on the broker and can be consumed",
          got_ent == sorted(ent_expect),
          "received=%d/%d bodies=%s" % (len(recv_ent), len(ent_expect), got_ent))

    # ---------- 9. timer messages (TIMER_DELIVER_MS: delivered only when the time comes) ----------
    topic_timer = "%s_Timer" % PREFIX
    deliver_at = int(time.time() * 1000) + 5000
    tm = Message(topic_timer, b"timer-5s")
    tm.set_user_property("TIMER_DELIVER_MS", str(deliver_at))
    prod.send(tm)
    recv_timer = run_consumer(topic_timer, "*", 20, group_suffix="timer", expect=1)
    hit_timer = [m for m in recv_timer if m.body == b"timer-5s"]
    check("the timer message is eventually delivered", bool(hit_timer), "received=%d" % len(recv_timer))
    if hit_timer:
        tdrift = hit_timer[0].store_timestamp - hit_timer[0].born_timestamp
        check("the timer takes effect (store_ts-born_ts>=4000ms)", tdrift >= 4000, "drift=%dms" % tdrift)
    else:
        check("the timer takes effect (store_ts-born_ts>=4000ms)", False, "no timer message to verify")

    prod.shutdown()

    # ---------- summary ----------
    failed = [r for r in results if not r[1]]
    print("\n================ message type interop summary ================")
    for name, ok, detail in results:
        print("  [%s] %s" % ("PASS" if ok else "FAIL", name))
    print("==================================================")
    if failed:
        print("result: %d item(s) failed" % len(failed))
        return 1
    print("result: all passed (all 9 message types verified against a real cluster%s)"
          % (", with ACL signing" if AK and SK else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
