# -*- coding: utf-8 -*-
"""生产者退出时真的发了 ``UNREGISTER_CLIENT``(35) —— 真机验证。

Java 的 ``DefaultMQProducerImpl.shutdown():313`` 会调 ``mQClientFactory.unregisterProducer(group)``
（``MQClientInstance:1198-1201``），后者进私有的 ``unregisterClient(group, null)``
（``:1158-1182``）：给 ``brokerAddrTable`` 里**每台 broker（含 slave）**同步发一发 code 35，
超时 ``getMqClientApiTimeout()``（3000ms），任何异常只 ``log.warn``。
``MQClientAPIImpl.unregisterClient:1615-1639`` 组的头是
``UnregisterClientRequestHeader{clientID, producerGroup, consumerGroup}`` —— 注意键名是大写 ID 的
``clientID``，生产者退出时 ``consumerGroup`` 是 null（不上线）。

本脚本证的是**线上真的走了这一发**，四件事：

  U1  生产者起来并真的发了消息（组注册的前置条件）。
  U2  broker 侧 204 ``GET_PRODUCER_CONNECTION_LIST`` 能看到本 clientId —— 注册确实发生过，
      不是「一直就没有所以消失也说明不了什么」。注册靠心跳上线（30s 一轮），所以要轮询等。
  U3  ``shutdown()`` 期间钩子抓到 code 35：``clientID`` 是自己的、``producerGroup`` 是本组、
      ``consumerGroup`` 不存在（Java 传 null），并且**每台已知 broker 各一发**。
  U4  每一发 35 都拿到 SUCCESS：说明它走的是**还开着的**那条长连接（broker 的
      ``ClientManageProcessor.unregisterClient:213-249`` 只摘除「这条 frame 到达的那条 channel」，
      连接都断了就不可能回 SUCCESS）。
  U5  紧接着查 204：这个组已经不在了（broker 回 SYSTEM_ERROR
      ``the producer group[...] not exist``，Java 的 ``mqadmin`` 也是这么判的）。
  U6  对照组：另一个**没退出**的生产者组仍能被 204 看见 —— 排除「broker 把所有连接都清了」
      这种假阳性。

⚠ 关于「这一发 35 到底有没有用」的判据强度：Python/C++/.NET 三套实现里每个生产者各自持有一份
``MQClientInstance``（各自一条 TCP 连接），所以退出时连接也会关掉，broker 的通道扫描同样会把
组摘掉 —— 单看 U5 分不出是 35 的功劳还是断连的功劳。U3/U4 是直接证据（抓帧），而**行为级**的
判别式证明在 ``rust/examples/live_producer.rs`` 的 P11：Rust 按 clientId 复用实例，两个同
instanceName、不同组的生产者共用一条连接，先退的那个连接还活着，组能消失只可能是因为 35。

前置：NameServer + Broker 已起，``autoCreateTopicEnable=true``。

用法：.venv/bin/python verify_producer_unregister_live.py [127.0.0.1:9876]
"""
from __future__ import annotations

import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rocketmq.client.admin import DefaultMQAdminExt
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.client.send_result import SendStatus
from rocketmq.common.message import Message
from rocketmq.remoting.protocol.codes import RemotingSysResponseCode, RequestCode
from rocketmq.remoting.protocol.remoting_command import RemotingCommand
from rocketmq.remoting.rpchook import RPCHook

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


class UnregisterProbe(RPCHook):
    """记录每一个 ``UNREGISTER_CLIENT``(35) 请求及其响应码。

    钩子挂在传输层（``register_rpc_hook``），编码前调用，所以抓到的就是上线的那份 extFields。
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.requests = []     # [(remote_addr, ext_fields 快照)]
        self.responses = []    # [(remote_addr, response_code 或 None)]
        self.codes = []        # 按顺序记录所有请求码，用于确认 35 排在发送之后

    def do_before_request(self, remote_addr: str, request: RemotingCommand) -> None:
        ext = dict(request.ext_fields or {})
        if not ext and request.custom_header is not None:
            # 头还挂在 custom_header 上，编码时才摊进 extFields（Java `headerEncode` 同一步）；
            # 这里只取快照，不改 request。
            ext = dict(request.custom_header.to_ext_fields() or {})
        with self.lock:
            self.codes.append(request.code)
            if request.code == RequestCode.UNREGISTER_CLIENT:
                self.requests.append((remote_addr, ext))

    def do_after_response(self, remote_addr, request, response) -> None:
        if request.code != RequestCode.UNREGISTER_CLIENT:
            return
        with self.lock:
            self.responses.append((remote_addr, None if response is None else response.code))

    def snapshot(self):
        with self.lock:
            return list(self.requests), list(self.responses), list(self.codes)


def _producer(group, instance_name):
    p = DefaultMQProducer(group)
    p.set_namesrv_addr(NAMESRV)
    p.set_instance_name(instance_name)
    return p


def _connection_client_ids(admin, broker_addr, group):
    """204 看到的 clientId 列表；broker 说「组不存在」时回 None。"""
    try:
        pc = admin.examine_producer_connection_info(group, broker_addr)
    except Exception as e:  # noqa: BLE001
        if "not exist" in str(e):
            return None
        raise
    return [c.client_id for c in pc.connection_set]


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
    # 已知 broker：主从都算，35 必须每台各一发
    all_addrs = []
    for ids in cluster.broker_addr_table.values():
        all_addrs.extend(ids.values())
    broker_addr = all_addrs[0]
    check("集群探活", True, "brokers=%s" % ",".join(all_addrs))

    topic = "Unreg_%d" % STAMP
    group = "PID_unreg_%d" % STAMP
    peer_group = "PID_unreg_peer_%d" % STAMP

    probe = UnregisterProbe()
    p = _producer(group, "unreg-%d" % STAMP)
    p.rpc_hook = probe
    p.start()
    peer = _producer(peer_group, "unreg-peer-%d" % STAMP)
    peer.start()

    try:
        r = p.send(Message(topic, b"unreg-probe"))
        check("U1 生产者发送成功", r.send_status == SendStatus.SEND_OK,
              "msgId=%s" % r.msg_id)
        peer.send(Message(topic, b"peer"))

        client_id = p.client_id
        deadline = time.time() + 70
        seen = None
        while time.time() < deadline:
            seen = _connection_client_ids(admin, broker_addr, group)
            if seen and client_id in seen:
                break
            time.sleep(1)
        check("U2 心跳后 204 能看到本 clientId", bool(seen) and client_id in seen,
              "clientId=%s 当前=%s" % (client_id, seen))
        peer_seen = _connection_client_ids(admin, broker_addr, peer_group)
        check("U2b 对照组注册可见（说明 204 这条判据本身有效）",
              bool(peer_seen) and peer.client_id in peer_seen, "当前=%s" % peer_seen)

        requests, responses, codes = probe.snapshot()
        check("U3 前置：还没退出时不应有 35", not requests,
              "count=%d" % len(requests))

        p.shutdown()
        requests, responses, codes = probe.snapshot()

        check("U3 shutdown 发了 code 35（每台已知 broker 各一发）",
              len(requests) == len(all_addrs),
              "count=%d brokers=%d" % (len(requests), len(all_addrs)))
        shape_ok = bool(requests)
        for addr, ext in requests:
            if ext.get("clientID") != client_id or ext.get("producerGroup") != group:
                shape_ok = False
            # Java 的 unregisterClient(group, null)：消费者槽位不上线
            if ext.get("consumerGroup"):
                shape_ok = False
            if addr not in all_addrs:
                shape_ok = False
        check("U3b 35 的头是 clientID + producerGroup，consumerGroup 不上线（Java 传 null）",
              shape_ok, "ext=%s" % (requests[0][1] if requests else None))
        all_ok = bool(responses) and all(addr in all_addrs for addr, _ in requests) \
            and all(rc == RemotingSysResponseCode.SUCCESS for _, rc in responses)
        check("U4 每一发 35 都回 SUCCESS（走的是还开着的连接）",
              all_ok and len(responses) == len(requests),
              "responses=%s" % responses)
        # 顺序：35 必须排在业务发送之后（否则注销的是没建起来的注册）
        last_send = max((i for i, c in enumerate(codes)
                         if c in (RequestCode.SEND_MESSAGE, RequestCode.SEND_MESSAGE_V2)),
                        default=-1)
        first_unreg = next((i for i, c in enumerate(codes)
                            if c == RequestCode.UNREGISTER_CLIENT), -1)
        check("U4b 35 排在业务发送之后", last_send >= 0 and first_unreg > last_send,
              "last_send=%d first_unreg=%d" % (last_send, first_unreg))

        gone = _wait_until(lambda: _connection_client_ids(admin, broker_addr, group) is None, 10)
        check("U5 退出后 204 查不到这个组（broker 回 not exist）", gone,
              "当前=%s" % _connection_client_ids(admin, broker_addr, group))
        peer_after = _connection_client_ids(admin, broker_addr, peer_group)
        check("U6 对照组仍在（排除 broker 全清这种假阳性）",
              bool(peer_after) and peer.client_id in peer_after, "当前=%s" % peer_after)
    finally:
        peer.shutdown()
        try:
            admin.delete_topic(topic)
        except Exception as e:  # noqa: BLE001
            print("    (delete_topic(%s) 失败: %s)" % (topic, e))
        admin.shutdown()

    passed = sum(1 for _, ok, _ in results if ok)
    print("\n== 结果: %d/%d 通过 ==" % (passed, len(results)))
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
