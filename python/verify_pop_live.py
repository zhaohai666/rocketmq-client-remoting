#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""POP 模式（5.x 轻量消费）真机验证。

用法（需本地 RocketMQ 5.5.1 集群，broker 需 autoCreateTopicEnable/autoCreateSubscriptionGroup）：
    .venv/bin/python verify_pop_live.py 127.0.0.1:9876

为什么单独测：POP 是 5.x 的「轻量消费」管道，本项目此前只有常量、没有实现。
POP 与 pull 的语义差异是断言的重心：
  - **不提交位点**，靠 ack 确认；不 ack 的消息在 invisibleTime 后被复活重投；
  - broker 在普通 topic 消息上**不写** POP_CK，客户端必须自己反构 8 段 CK 串。

场景：
  S1 建 topic(8 队列) + 发 10 条
  S2 多队列 POP（queue_id=-1, init_mode=0）→ FOUND 且拿到消息
  S3 每条消息都被盖上 **8 段** POP_CK，且 brokerName/queueId 与消息实际一致
  S4 ack 一条 → SUCCESS
  S5 change_invisible_time → SUCCESS 且返回**新的** 8 段 extraInfo
  S6 校验真的生效：非法 queueId → MESSAGE_ILLEGAL；越界 offset → NO_MESSAGE
  S7 单队列 POP（新消费组 + queue_id=0）→ FOUND 且消息 queueId 全为 0
  S8 不 ack 会复活：小 invisibleTime POP 后不 ack，等待后重 POP 能拿到同一条消息，
     且它的 POP_CK 是 retryFlag=1（来自 %RETRY%<group>_<topic>）—— 至少一次语义
"""
from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")

from rocketmq.client.consumer_result import PopStatus
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message
from rocketmq.common.message_const import MessageConst
from rocketmq.remoting.protocol import extra_info as ei
from rocketmq.remoting.protocol.codes import ResponseCode

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "PopLive_%d" % STAMP
TOPIC_REVIVE = "PopLiveRevive_%d" % STAMP
GROUP = "GID_PopLive_%d" % STAMP
GROUP_SINGLE = "GID_PopLiveSingle_%d" % STAMP
GROUP_REVIVE = "GID_PopLiveRevive_%d" % STAMP
QUEUE_NUM = 8
N_MSG = 10
REVIVE_QUEUE_NUM = 4
REVIVE_N_MSG = 3
INVISIBLE_SHORT = 5000
REVIVE_WAIT_SEC = 20

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


def main() -> int:
    prod = DefaultMQProducer("PG_PopLive_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    client = prod._mq_client
    assert isinstance(client, MQClientInstance)

    try:
        # ---------------- S1 建 topic + 发消息 ----------------
        print("=== S1 准备 topic 与消息 ===")
        prod.create_topic("TBW102", TOPIC, QUEUE_NUM)
        prod.create_topic("TBW102", TOPIC_REVIVE, REVIVE_QUEUE_NUM)
        for i in range(N_MSG):
            prod.send(Message(TOPIC, ("pop-live-%d" % i).encode("utf-8"),
                              keys="pk%d" % i))
        print("  sent %d msgs to %s" % (N_MSG, TOPIC))

        route = client.get_topic_route_data(TOPIC)
        check("S1 topic 路由可用", route is not None and len(route.get_broker_datas()) > 0)
        broker_name = route.get_broker_datas()[0].broker_name
        addr = MQClientInstance.find_broker_addr_in_route(route, broker_name)
        print("  brokerName=%s addr=%s" % (broker_name, addr))

        # ---------------- S2 多队列 POP ----------------
        print("=== S2 多队列 POP（addr 走自动解析路径）===")
        res = client.pop_message(GROUP, TOPIC, queue_id=-1, max_msg_nums=32,
                                 invisible_time=60000, poll_time=0, init_mode=0)
        check("S2 POP 取到消息", res.status == PopStatus.FOUND and len(res.msg_found_list) > 0,
              "status=%s count=%d startOffsetInfo=%r" %
              (res.status, len(res.msg_found_list), res.start_offset_info))
        if not res.msg_found_list:
            print("############ PASS=%d FAIL=%d ############" % (PASS, FAIL))
            return 1

        # ---------------- S3 POP_CK 反构 ----------------
        print("=== S3 POP_CK 反构（8 段）===")
        all8 = True
        match = True
        sample = None
        for m in res.msg_found_list:
            ck = m.properties.get(MessageConst.PROPERTY_POP_CK)
            if not ck:
                all8 = False
                break
            seg = ei.split(ck)
            if len(seg) != 8:
                all8 = False
                break
            if ei.get_broker_name(seg) != broker_name or ei.get_queue_id(seg) != m.queue_id:
                match = False
            if sample is None:
                sample = ck
        check("S3a 每条消息都有 8 段 POP_CK", all8, "sample=%r" % sample)
        check("S3b CK 的 brokerName/queueId 与消息一致", match)
        check("S3c 1ST_POP_TIME 已补",
              all(m.properties.get(MessageConst.PROPERTY_FIRST_POP_TIME) is not None
                  for m in res.msg_found_list))

        # ---------------- S4 ack ----------------
        print("=== S4 ACK ===")
        first = res.msg_found_list[0]
        ck1 = first.properties[MessageConst.PROPERTY_POP_CK]
        seg1 = ei.split(ck1)
        code = client.ack_message(GROUP, TOPIC, ei.get_queue_id(seg1), ck1,
                                  ei.get_queue_offset(seg1), addr=addr)
        check("S4 ack 返回 SUCCESS", code == ResponseCode.SUCCESS,
              "code=%d queueId=%d offset=%d" %
              (code, ei.get_queue_id(seg1), ei.get_queue_offset(seg1)))

        # ---------------- S5 change invisible time ----------------
        print("=== S5 CHANGE_MESSAGE_INVISIBLETIME ===")
        second = res.msg_found_list[1]
        ck2 = second.properties[MessageConst.PROPERTY_POP_CK]
        seg2 = ei.split(ck2)
        res5 = client.change_invisible_time(GROUP, TOPIC, ei.get_queue_id(seg2), ck2,
                                            ei.get_queue_offset(seg2), 30000, addr=addr)
        new_seg = ei.split(res5.extra_info) if res5.extra_info else []
        check("S5a 延长不可见时间成功", res5.success,
              "code=%d popTime=%d invisibleTime=%d" %
              (res5.response_code, res5.pop_time, res5.invisible_time))
        check("S5b 返回新的 8 段 extraInfo 且用新值",
              len(new_seg) == 8 and ei.get_invisible_time(new_seg) == res5.invisible_time
              and ei.get_pop_time(new_seg) == res5.pop_time,
              "new=%r" % res5.extra_info)

        # ---------------- S6 校验真的生效 ----------------
        print("=== S6 非法参数必须被拒绝 ===")
        bad_queue = client.ack_message(GROUP, TOPIC, QUEUE_NUM + 90, ck1,
                                       ei.get_queue_offset(seg1), addr=addr)
        check("S6a 非法 queueId 被拒（MESSAGE_ILLEGAL）",
              bad_queue == ResponseCode.MESSAGE_ILLEGAL, "code=%d" % bad_queue)
        bad_offset = client.ack_message(GROUP, TOPIC, ei.get_queue_id(seg1), ck1,
                                        1 << 40, addr=addr)
        check("S6b 越界 offset 被拒（NO_MESSAGE）",
              bad_offset == ResponseCode.NO_MESSAGE, "code=%d" % bad_offset)

        # ---------------- S7 单队列 POP ----------------
        print("=== S7 单队列 POP ===")
        # 用新消费组：老组在同一 (topic) 上已有 pop 位点，重复 POP 拿不到东西
        res7 = client.pop_message(GROUP_SINGLE, TOPIC, queue_id=0, max_msg_nums=32,
                                  invisible_time=60000, poll_time=0, init_mode=0, addr=addr)
        only_q0 = all(m.queue_id == 0 for m in res7.msg_found_list)
        check("S7 单队列 POP 取到队列 0 的消息",
              res7.status == PopStatus.FOUND and len(res7.msg_found_list) > 0 and only_q0,
              "status=%s count=%d queueIds=%s" %
              (res7.status, len(res7.msg_found_list),
               sorted({m.queue_id for m in res7.msg_found_list})))

        # ---------------- S8 不 ack 会复活 ----------------
        print("=== S8 不 ack → 复活重投（至少一次语义）===")
        for i in range(REVIVE_N_MSG):
            prod.send(Message(TOPIC_REVIVE, ("pop-revive-%d" % i).encode("utf-8"),
                              keys="rv%d" % i))
        route_rv = client.get_topic_route_data(TOPIC_REVIVE)
        addr_rv = MQClientInstance.find_broker_addr_in_route(
            route_rv, route_rv.get_broker_datas()[0].broker_name)
        res8a = client.pop_message(GROUP_REVIVE, TOPIC_REVIVE, queue_id=-1,
                                   invisible_time=INVISIBLE_SHORT, poll_time=0,
                                   init_mode=0, addr=addr_rv)
        first_keys = {m.properties.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                      or m.msg_id for m in res8a.msg_found_list}
        check("S8a 首轮 POP 取到消息（不 ack）",
              res8a.status == PopStatus.FOUND and len(res8a.msg_found_list) > 0,
              "count=%d" % len(res8a.msg_found_list))

        print("  等待 %ds 让 broker 复活..." % REVIVE_WAIT_SEC)
        time.sleep(REVIVE_WAIT_SEC)

        res8b = client.pop_message(GROUP_REVIVE, TOPIC_REVIVE, queue_id=-1,
                                   invisible_time=60000, poll_time=0, init_mode=0,
                                   addr=addr_rv)
        revived = [m for m in res8b.msg_found_list
                   if (m.properties.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                       or m.msg_id) in first_keys]
        retry_flags = set()
        for m in revived:
            ck = m.properties.get(MessageConst.PROPERTY_POP_CK)
            if ck:
                retry_flags.add(ei.get_retry(ei.split(ck)))
        check("S8b 未 ack 的消息被复活重投", len(revived) > 0,
              "revived=%d total=%d" % (len(revived), len(res8b.msg_found_list)))
        check("S8c 复活消息的 POP_CK retryFlag=1（来自 %%RETRY%%<group>_<topic>）",
              "1" in retry_flags,
              "retryFlags=%s" % sorted(retry_flags))

    finally:
        prod.shutdown()

    print("############ PASS=%d FAIL=%d ############" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
