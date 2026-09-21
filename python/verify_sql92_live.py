#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""SQL92 过滤 + CHECK_CLIENT_CONFIG(46) 真机验证（本地 RocketMQ 5.5.1 集群）。

用法：
    .venv/bin/python verify_sql92_live.py 127.0.0.1:9876

前置条件：broker 必须 ``enablePropertyFilter=true``（本仓库的 dev broker 配置已开）。

为什么必须在真集群上测：SQL92 这条链路最容易「静默失效」。broker 的
``ExpressionMessageFilter`` 拿不到编译好的过滤数据时**直接放行全部消息**（``return true``），
于是两种错都表现为「消费者正常启动、消息也都收到了」：
  1. broker 没开 enablePropertyFilter ⇒ 表达式根本没被编译；
  2. 表达式语法错 ⇒ 同上；只有 Java 的 ``checkClientConfig`` 会把它变成启动错误。
离线单测（tests/test_check_client_config.py）锁得住协议形状，锁不住 broker 真的按属性
过滤了。这里四段都验：
  S1 线上取证：SQL92 订阅 ⇒ 启动时正好一笔 46（body 是 CheckClientRequestBody）；
     纯 TAG 订阅 ⇒ 一笔都不发（Java ``ExpressionType.isTagType`` 短路）
  S2 真过滤：消费者**先起来再发消息**（新消费组的 CONSUME_FROM_LAST_OFFSET 会跳过启动
     前的消息，先发消息这一段就是假绿）：SQL92 只订阅 red ⇒ 恰好 3 条 red；
     TAG '*' 对照组 ⇒ 6 条全收
  S3 空结果腿：订阅永不匹配的 color='green' ⇒ 一条都不收（排除"其实全放行了"的假绿）
  S4 反证：语法错的表达式让 start() 抛 SUBSCRIPTION_PARSE_FAILED(23)，且启动就地回滚
     （同一个对象换成合法表达式能重新 start）
"""
from __future__ import annotations

import json
import sys
import time
from typing import List, Optional, Tuple

sys.path.insert(0, ".")

from rocketmq.client.consumer import (DefaultMQPushConsumer, MessageSelector,
                                      SimpleMessageListener)
from rocketmq.client.consumer_result import ConsumeConcurrentlyStatus
from rocketmq.client.exception import MQClientException
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.client.send_result import SendStatus
from rocketmq.common.message import Message
from rocketmq.remoting.protocol.codes import RequestCode, ResponseCode
from rocketmq.remoting.protocol.remoting_command import RemotingCommand
from rocketmq.remoting.rpchook import RPCHook

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "Sql92Live_%d" % STAMP
GROUP = "GID_Sql92Live_%d" % STAMP

PASS = 0
FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print("  PASS  %s" % name)
    else:
        FAIL += 1
        print("  FAIL  %s%s" % (name, ("  <- " + detail) if detail else ""))


def wait_for(cond, timeout: float = 20.0, interval: float = 0.3) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(interval)
    return bool(cond())


class CheckConfigProbe(RPCHook):
    """钩在 transport 上，抓启动期真正发出去的 46 号请求（含 body）。"""

    def __init__(self):
        self.requests: List[tuple] = []        # (addr, code)
        self.bodies: List[dict] = []

    def do_before_request(self, remote_addr: str, request: RemotingCommand) -> None:
        self.requests.append((remote_addr, request.code))
        if request.code == RequestCode.CHECK_CLIENT_CONFIG and request.body:
            self.bodies.append(json.loads(request.body.decode("utf-8")))

    @property
    def codes(self) -> List[int]:
        return [code for _, code in self.requests]

    @property
    def check_count(self) -> int:
        return sum(1 for code in self.codes if code == RequestCode.CHECK_CLIENT_CONFIG)


class Collector:
    """攒下消费到的消息。SimpleMessageListener 只吃一个纯函数，所以包一层给出 listener。"""

    def __init__(self):
        self.msgs = []

    def listener(self) -> SimpleMessageListener:
        return SimpleMessageListener(self._consume)

    def _consume(self, msgs):
        self.msgs.extend(msgs)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def bodies(self) -> List[str]:
        return sorted(m.body.decode() for m in self.msgs)


def new_consumer(group: str, selector: Optional[MessageSelector] = None,
                 expression: Optional[str] = None,
                 probe: Optional[CheckConfigProbe] = None
                 ) -> Tuple[DefaultMQPushConsumer, Collector]:
    """建一个订阅本 topic 的 push 消费者（probe 非空时把 RPC 钩子装上取证）。"""
    c = DefaultMQPushConsumer(group)
    c.set_namesrv_addr(NAMESRV)
    if probe is not None:
        c.rpc_hook = probe
    if selector is not None:
        c.subscribe_with_selector(TOPIC, selector)
    else:
        c.subscribe(TOPIC, expression or "*")
    col = Collector()
    c.set_message_listener(col.listener())
    return c, col


def main() -> int:
    print("SQL92 / CHECK_CLIENT_CONFIG live check on %s" % NAMESRV)
    prod = DefaultMQProducer(GROUP + "_P")
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    try:
        prod.create_topic("TBW102", TOPIC, 4)
        check("T0 topic 路由就绪（4 队列）",
              wait_for(lambda: len(
                  prod._mq_client.get_topic_publish_info(TOPIC).msg_queue_list) >= 4, 20.0))

        # ---------- S1 线上取证：46 号请求的形状 ----------
        print("\n---------- S1 启动期的 46 号请求 ----------")
        probe = CheckConfigProbe()
        c1, _ = new_consumer(GROUP + "_SQL", MessageSelector.by_sql("color = 'red'"),
                             probe=probe)
        c1.start()
        check("SQL92 订阅触发恰好一笔 CHECK_CLIENT_CONFIG(46)", probe.check_count == 1,
              "codes=%s" % probe.codes)
        body = probe.bodies[0] if probe.bodies else {}
        sd = body.get("subscriptionData") or {}
        check("body 是 CheckClientRequestBody（clientId / group / subscriptionData）",
              body.get("group") == GROUP + "_SQL" and body.get("clientId") == c1.client_id
              and sd.get("expressionType") == "SQL92"
              and sd.get("subString") == "color = 'red'" and sd.get("topic") == TOPIC,
              json.dumps(body))
        c1.shutdown()

        tag_probe = CheckConfigProbe()
        c2, _ = new_consumer(GROUP + "_TAG", probe=tag_probe, expression="tagA || tagB")
        c2.start()
        check("纯 TAG 订阅一笔 46 都不发（Java isTagType 短路）",
              tag_probe.check_count == 0, str(tag_probe.codes))
        c2.shutdown()

        # ---------- S2 / S3：消费者先起来，再发消息 ----------
        print("\n---------- S2 broker 按属性过滤 / S3 永不匹配的表达式 ----------")
        red_c, red_col = new_consumer(GROUP + "_FILTER",
                                      MessageSelector.by_sql("color = 'red'"))
        green_c, green_col = new_consumer(GROUP + "_NONE",
                                          MessageSelector.by_sql("color = 'green'"))
        all_c, all_col = new_consumer(GROUP + "_ALL")
        for c in (red_c, green_c, all_c):
            c.start()
        sent = {"red": [], "blue": []}
        for color in ("red", "blue"):
            for i in range(3):
                text = "body-%s-%d" % (color, i)
                m = Message(TOPIC, text.encode("utf-8"), keys="%s-%s-%d" % (STAMP, color, i))
                m.put_property("color", color)
                r = prod.send(m)
                check("发送 %s 成功" % text, r.send_status == SendStatus.SEND_OK,
                      str(r.send_status))
                sent[color].append(text)

        wait_for(lambda: len(all_col.msgs) >= 6, 25.0)
        time.sleep(4.0)                        # 再等一会，确认 green 不是"来得晚"
        red_bodies, all_bodies = red_col.bodies(), all_col.bodies()
        check("SQL92(color=red) 收到 3 条，且正好是发出去的那 3 条",
              red_bodies == sorted(sent["red"]), str(red_bodies))
        check("TAG '*' 对照组收到 6 条（红+蓝全在）",
              all_bodies == sorted(sent["red"] + sent["blue"]), str(all_bodies))
        check("blue 的 3 条没漏进 SQL92 消费者（证明 broker 真在过滤）",
              all(b not in red_bodies for b in sent["blue"]), str(red_bodies))
        props = {m.properties.get("color") for m in red_col.msgs}
        check("收到的消息属性 color 可读且都是 red", props == {"red"}, str(props))
        check("订阅永不匹配的 color='green' ⇒ 一条都没收到",
              len(green_col.msgs) == 0, green_col.bodies())
        for c in (red_c, green_c, all_c):
            c.shutdown()

        # ---------- S4 反证：非法表达式让启动失败 ----------
        print("\n---------- S4 非法表达式在启动期失败 ----------")
        c4, _ = new_consumer(GROUP + "_BAD", MessageSelector.by_sql("color =="))
        err: Optional[MQClientException] = None
        t0 = time.time()
        try:
            c4.start()
        except MQClientException as e:
            err = e
        cost = time.time() - t0
        check("start() 抛出 MQClientException", err is not None, repr(err))
        check("错误码是 broker 的 SUBSCRIPTION_PARSE_FAILED(23)",
              err is not None
              and err.response_code == ResponseCode.SUBSCRIPTION_PARSE_FAILED,
              "code=%s remark=%s" % (getattr(err, "response_code", None), err))
        check("失败后消费者没留在已启动状态", c4._started is False)
        check("非法表达式立刻失败（<5s，不是等超时兜底）", cost < 5.0, "%.2fs" % cost)
        # 回滚干净了？同一个对象换个合法表达式应当能重新 start
        retry_err: Optional[Exception] = None
        try:
            c4.subscribe_with_selector(TOPIC, MessageSelector.by_sql("color = 'blue'"))
            c4.start()
        except Exception as e:                  # noqa: BLE001
            retry_err = e
        check("失败后修正表达式可重新 start（启动已就地回滚）", retry_err is None,
              repr(retry_err))
        if retry_err is None:
            c4.shutdown()
    finally:
        prod.shutdown()

    print("\n%d PASS / %d FAIL" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
