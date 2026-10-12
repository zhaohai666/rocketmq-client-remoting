# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client.admin import DefaultMQAdminExt
from client.exception import MQBrokerException, MQClientException
from client.producer import DefaultMQProducer
from client.send_result import SendStatus
from common.message import Message
from common.mix_all import MixAll

NAMESRV = os.environ.get("ROCKETMQ_NAMESRV", "127.0.0.1:9876")
STAMP = int(time.time())

MAIN_QUEUES = 4
ORDERLY_QUEUES = 1

results = []


def check(name, ok, detail=""):
    results.append(ok)
    print(("PASS  " if ok else "FAIL  ") + name + (("  [" + detail + "]") if detail else ""))


def _admin():
    # 注意 DefaultMQAdminExt 的第一个位置参数是 rpc_hook，不是 clientId / group！
    # 传字符串进去会让每个请求在 `hook.do_before_request` 上炸 AttributeError，
    # 而路由查询那层把异常吞掉、只表现为「topic not exist」，非常难查。
    a = DefaultMQAdminExt()
    a.set_namesrv_addr(NAMESRV)
    a.start()
    return a


def ensure_topic(admin, topic, queues):
    """建 topic 并等路由可见；已存在则把队列数覆盖成我们想要的。"""
    admin.create_topic(MixAll.DEFAULT_TOPIC, topic, queues)
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            route = admin.examine_topic_route(topic)
        except MQClientException:
            route = None
        if route is not None:
            if len(route.get_all_subscribe_message_queue(topic)) == queues:
                return True
            # 路由在、队列数不对：覆盖一次再看。
            admin.create_topic(MixAll.DEFAULT_TOPIC, topic, queues)
        time.sleep(0.5)
    return False


def producer():
    p = DefaultMQProducer("PID_go_consumer_feed_%d" % STAMP)
    p.set_namesrv_addr(NAMESRV)
    p.set_instance_name("go-consumer-feed-%d" % STAMP)
    return p


def produce(topic, count, orderly_topic=None, orderly_count=0):
    """主 topic：偶数号 TagA / 奇数号 TagB，body 里写明 tag 便于对拍。

    顺序 topic：单队列，body 按序号递增，给 Go 的顺序消费用例对顺序用。
    """
    admin = _admin()
    try:
        wanted = [(topic, MAIN_QUEUES)]
        if orderly_topic:
            wanted.append((orderly_topic, ORDERLY_QUEUES))
        for name, queues in wanted:
            ok = ensure_topic(admin, name, queues)
            check("建 topic %s（%d 队列）" % (name, queues), ok)
    finally:
        admin.shutdown()

    p = producer()
    p.start()
    sent = 0
    try:
        for i in range(count):
            tag = "TagA" if i % 2 == 0 else "TagB"
            body = ("%s-%03d" % (tag, i)).encode("utf-8")
            r = p.send(Message(topic=topic, body=body, tags=tag))
            if r.send_status == SendStatus.SEND_OK:
                sent += 1
            else:
                print("send %d -> %s" % (i, r.send_status))
        check("produce %d 条到 %s" % (count, topic), sent == count, "sent=%d" % sent)
        print("PRODUCED=%d" % sent)

        if orderly_topic and orderly_count:
            osent = 0
            for i in range(orderly_count):
                body = ("Ord-%03d" % i).encode("utf-8")
                r = p.send(Message(topic=orderly_topic, body=body, tags="TagA"))
                if r.send_status == SendStatus.SEND_OK:
                    osent += 1
            check("produce %d 条到顺序 topic %s" % (orderly_count, orderly_topic),
                  osent == orderly_count, "sent=%d" % osent)
            print("PRODUCED_ORDERLY=%d" % osent)
    finally:
        p.shutdown()
    return sent


def verify(topic, group, count, unreg_group=None):
    admin = _admin()
    try:
        # 消费侧要的是「读队列」（get_all_subscribe_message_queue），不是发布队列：
        # 只读 topic、以及主掉线只剩从节点的路由上，两者给出的队列集合不同。
        queues = admin.examine_topic_route(topic).get_all_subscribe_message_queue(topic)
        total = 0
        per_queue = {}
        for mq in queues:
            off = admin.examine_consumer_offset(group, mq)
            if off is None:
                off = -1
            per_queue[mq.queue_id] = off
            if off > 0:
                total += off
        check(
            "broker 上 %s 在 %s 的已提交位点合计 = %d" % (group, topic, count),
            total == count,
            "total=%d per_queue=%s" % (total, sorted(per_queue.items())),
        )

        if unreg_group:
            # 优雅注销：Go 侧 shutdown 发了 UNREGISTER_CLIENT，broker 的消费连接
            # 列表里就不该再有它。组彻底下线时 broker 回 SUBSCRIPTION_NOT_EXIST
            # ("not online")，客户端把它翻成 MQBrokerException —— 那正是要的结果。
            # 注意 MQBrokerException 与 MQClientException 是**平级**的，只 catch
            # 后者会让这条检查以异常收场（第一版就是这么挂的）。
            try:
                conn = admin.examine_consumer_connection_info(unreg_group)
                ids = list(getattr(conn, "connection_set", []) or [])
                check("停机后 broker 上无 %s 的残留连接" % unreg_group,
                      len(ids) == 0, "connections=%d" % len(ids))
            except MQBrokerException as e:
                text = str(e)
                check("停机后 %s 已从 broker 注销（组不在线）" % unreg_group,
                      "not online" in text, "code=%s %s" % (e.response_code, text))
            except MQClientException as e:
                check("停机后 %s 已从 broker 注销（组不在线）" % unreg_group, True, str(e))
    finally:
        admin.shutdown()

    print("PASS_TOTAL=%d FAIL_TOTAL=%d"
          % (sum(1 for r in results if r), sum(1 for r in results if not r)))
    return 0 if all(results) else 1


def main():
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2
    mode = sys.argv[1]
    if mode == "produce":
        topic = sys.argv[2]
        count = int(sys.argv[3]) if len(sys.argv) > 3 else 12
        orderly_topic = sys.argv[4] if len(sys.argv) > 4 else None
        orderly_count = int(sys.argv[5]) if len(sys.argv) > 5 else 0
        produce(topic, count, orderly_topic, orderly_count)
        return 0 if all(results) else 1
    if mode == "verify":
        if len(sys.argv) < 5:
            print("usage: verify <topic> <group> <count> [unreg_group]", file=sys.stderr)
            return 2
        unreg = sys.argv[5] if len(sys.argv) > 5 else None
        return verify(sys.argv[2], sys.argv[3], int(sys.argv[4]), unreg)
    print("unknown mode %s" % mode, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
