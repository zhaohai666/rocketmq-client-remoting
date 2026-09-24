# -*- coding: utf-8 -*-
"""MQClientInstance：RocketMQ 客户端核心编排（对应 org.apache.rocketmq.client.impl.factory.MQClientInstance
与 MQClientAPIImpl 的核心调用面，Python 单机版）。

职责：NameServer 地址管理、Topic 路由获取与缓存、Broker 地址解析、
消息发送（SEND_MESSAGE / SEND_MESSAGE_V2）、拉取（PULL_MESSAGE）、
offset 查询/更新、心跳、管理类 API（创建/删除 Topic、集群信息等）。
"""
from __future__ import annotations

import random
import threading
import time
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional

from ..common.message import Message, MessageBatch, MessageExt, MessageQueue
from ..common.message_accessor import MessageAccessor
from ..common.message_client_id_setter import get_uniq_id, set_uniq_id
from ..common.message_const import MessageConst
from ..common.message_decoder import (decode_message, decode_messages, decompress_body,
                                      message_properties_2_string,
                                      string_2_message_properties)
from ..common.mix_all import MixAll
from ..common.subscription_data import ExpressionType, SubscriptionData
from ..common.sysflag import MessageSysFlag
from ..common.topic_config import TopicFilterType
from ..logging import get_logger
from ..remoting.client import RemotingClient
from ..remoting.protocol.body import (CheckClientRequestBody, ClusterInfo,
                                      ConsumerRunningInfo, ConsumeMessageDirectlyResult,
                                      GetConsumerStatusBody,
                                      GetConsumerListByGroupResponseBody, ResetOffsetBody,
                                      TopicList)

if TYPE_CHECKING:
    from .consumer import DefaultMQPushConsumer
from ..remoting.protocol.codes import RequestCode, ResponseCode, SerializeType
from ..remoting.protocol.headers import (ConsumeMessageDirectlyResultRequestHeader,
                                         CreateTopicRequestHeader,
                                         GetConsumerListByGroupRequestHeader,
                                         GetConsumerRunningInfoRequestHeader,
                                         GetConsumerStatusRequestHeader,
                                         GetMaxOffsetRequestHeader,
                                         GetMaxOffsetResponseHeader, GetMinOffsetRequestHeader,
                                         GetMinOffsetResponseHeader,
                                         NotifyConsumerIdsChangedRequestHeader,
                                         PullMessageRequestHeader,
                                         PullMessageResponseHeader, QueryConsumerOffsetRequestHeader,
                                         QueryConsumerOffsetResponseHeader, QueryMessageRequestHeader,
                                         QueryMessageResponseHeader,
                                         RecallMessageRequestHeader, RecallMessageResponseHeader,
                                         ReplyMessageRequestHeader,
                                         ResetOffsetRequestHeader,
                                         SearchOffsetRequestHeader,
                                         SearchOffsetResponseHeader, SendMessageRequestHeader,
                                         SendMessageRequestHeaderV2, SendMessageResponseHeader,
                                         UpdateConsumerOffsetRequestHeader)
from ..remoting.protocol.heartbeat import HeartbeatData
from ..remoting.protocol.remoting_command import RemotingCommand
from ..remoting.protocol.route import TopicRouteData
from ..remoting.rpchook import StreamTypeRPCHook
from .consumer_stats import ConsumerStatsManager
from .exception import ClientErrorCode, MQBrokerException, MQClientException
from .request_reply import REQUEST_FUTURE_HOLDER, is_reply_message
from .send_result import SendResult, SendStatus
from .top_addressing import DefaultTopAddressing

logger = get_logger()


class TopicPublishInfo:
    """对应 org.apache.rocketmq.client.impl.producer.TopicPublishInfo。"""

    def __init__(self):
        self.order_topic = False
        self.msg_queue_list: List[MessageQueue] = []
        self.topic_route_data: Optional[TopicRouteData] = None
        self._index = 0
        self._lock = threading.Lock()

    def ok(self) -> bool:
        return len(self.msg_queue_list) > 0

    def reset_index(self) -> None:
        self._index = 0

    def select_one_message_queue(self, *filters) -> Optional[MessageQueue]:
        """轮询选队列（对应 Java TopicPublishInfo.selectOneMessageQueue）。

        ``*filters`` 为可调用 ``f(mq) -> bool``，全部通过才选中。无过滤器时固定返回一个
        轮询队列（永不返回 None）；带过滤器且一轮内无匹配时返回 None，由调用方退化选择。
        """
        with self._lock:
            if not self.msg_queue_list:
                raise MQClientException("no message queue for publish info")
            n = len(self.msg_queue_list)
            if not filters:
                mq = self.msg_queue_list[self._index % n]
                self._index += 1
                return mq
            for _ in range(n):
                mq = self.msg_queue_list[self._index % n]
                self._index += 1
                if all(f(mq) for f in filters):
                    return mq
            return None

    def to_dict(self) -> dict:
        return {
            "orderTopic": self.order_topic,
            "messageQueueList": [
                {"topic": q.topic, "brokerName": q.broker_name, "queueId": q.queue_id}
                for q in self.msg_queue_list
            ],
        }


class MQClientInstance:
    INSTANCE_MAP: Dict[str, "MQClientInstance"] = {}
    INSTANCE_LOCK = threading.Lock()

    def __init__(self, client_id: str, name_server_addrs: List[str],
                 connect_timeout_millis: int = 3000, invoke_timeout_millis: int = 15000,
                 tls_enable: Optional[bool] = None,
                 enable_stream_request_type: bool = False,
                 unit_name: Optional[str] = None):
        self.client_id = client_id
        self.name_server_addrs: List[str] = list(name_server_addrs)
        self.remoting_client = RemotingClient(connect_timeout_millis, invoke_timeout_millis,
                                              tls_enable=tls_enable)
        # 对应 Java `MQClientAPIImpl:329-332`：stream 钩子必须注册在用户 rpcHook 之前，
        # 这样 `ReqT` 才会被算进 ACL 签名内容（注释原文 "Inject stream rpc hook first
        # to make reserve field signature"）。各 facade 都是在构造完本实例之后才
        # `register_rpc_hook(self.rpc_hook)`，所以在这里注册天然满足顺序。
        if enable_stream_request_type:
            self.remoting_client.register_rpc_hook(StreamTypeRPCHook())
        self.topic_route_table: Dict[str, TopicRouteData] = {}
        self.topic_publish_info_table: Dict[str, TopicPublishInfo] = {}
        self.topic_route_lock = threading.RLock()
        self._started = False
        self._last_route_fetch = 0.0
        # 本客户端「在用」的 topic（消费者订阅 + 生产者发过的），对应 Java 的
        # MQConsumerInner.subscriptions() / MQProducerInner.getPublishTopicList()，
        # 由周期任务 updateTopicRouteInfoFromNameServer() 逐个刷新路由。
        self._topics_in_use: set = set()
        self._route_refresh_thread: Optional[threading.Thread] = None
        self._route_refresh_stop = threading.Event()
        # Request-Reply：broker 用 PUSH_REPLY_MESSAGE_TO_CLIENT(326) 把应答推回来。
        # 对应 Java MQClientAPIImpl 构造函数里的
        # ``registerProcessor(RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, clientRemotingProcessor, null)``
        # —— 它是**客户端实例级**的（与具体 producer 无关，应答按 clientId 推回），所以在这里注册。
        self.remoting_client.register_processor(
            RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, self._process_reply_message)
        # broker 主动请求 220/221/307/309（对应 Java ClientRemotingProcessor）：
        # 与 326 一样是**客户端实例级**注册，但处理时要按 consumerGroup 找到对应的消费者
        # 实例（同一进程多消费组时不能只认最后一个）。注意这些回调跑在 remoting 的**读线程**
        # 上（见 client.py _dispatch），因此任何会触发 invokeSync 的动作（如 220 的 rebalance）
        # 必须丢到后台线程，否则会卡死该连接上所有响应（静默自死锁）。
        self.remoting_client.register_processor(
            RequestCode.RESET_CONSUMER_CLIENT_OFFSET, self._process_reset_offset)
        self.remoting_client.register_processor(
            RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT, self._process_get_consumer_status)
        self.remoting_client.register_processor(
            RequestCode.GET_CONSUMER_RUNNING_INFO, self._process_get_consumer_running_info)
        self.remoting_client.register_processor(
            RequestCode.CONSUME_MESSAGE_DIRECTLY, self._process_consume_message_directly)
        # NOTIFY_CONSUMER_IDS_CHANGED(40)：消费组成员变化时 broker 沿长连接反向推过来。
        # 对应 Java ``MQClientAPIImpl`` 构造函数 —— 它注册的是**实例级**的
        # ``clientRemotingProcessor``，不是每个消费者各注册一份：处理器按 code 建表，
        # 同组多消费者时后注册的会覆盖前一个（Java 里一个 clientId 只有一个连接，
        # 覆盖就等于「只有最后一个消费者会被叫醒」）。
        self.remoting_client.register_processor(
            RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, self._process_notify_consumer_ids_changed)
        # 40 只能由 broker 发出，用例无法注入反向请求，所以计数是唯一能断言
        # 「本端确实收到并处理过」的落点（见 python/tests/test_broker_requests.py）。
        self._consumer_ids_changed_count = 0
        self._consumer_table: Dict[str, "DefaultMQPushConsumer"] = {}
        # 动态 name server（对应 Java MQClientAPIImpl.topAddressing）。
        # 未配置 ROCKETMQ_NAMESRV_DOMAIN 时 ws_addr 为空串 → fetch 是 no-op，行为不变。
        # unitName 要透传（Java `new DefaultTopAddressing(MixAll.getWSAddr(), clientConfig.getUnitName())`）：
        # 有 unit 时取址 URL 变成 `<wsAddr>-<unitName>?nofix=1`，取到的是该单元的 name server 列表。
        self.top_addressing = DefaultTopAddressing(unit_name=unit_name or "")
        # 消费统计（Java MQClientFactory.getConsumerStatsManager，实例级共享）
        self.consumer_stats_manager = ConsumerStatsManager()
        self._namesrv_refresh_stop = threading.Event()
        self._namesrv_refresh_thread: Optional[threading.Thread] = None
        # 线程弹性巡检线程（对应 Java MQClientInstance.startScheduledTask 里
        # scheduleAtFixedRate(adjustThreadPool, 1, 1, MINUTES)）
        self._adjust_pool_stop = threading.Event()
        self._adjust_pool_thread: Optional[threading.Thread] = None
        MQClientInstance.INSTANCE_MAP[client_id] = self

    def fetch_name_server_addr(self) -> Optional[str]:
        """对应 Java ``MQClientAPIImpl.fetchNameServerAddr``：地址**变化才应用**。

        应用 = 按 ``;`` 切分后更新本实例的 name_server_addrs（Java 的
        ``updateNameServerAddressList``）。返回新地址串；没变化 / 不可用返回 None。
        """
        if self.top_addressing is None or not self.top_addressing.ws_addr:
            return None
        changed = self.top_addressing.fetch_and_apply()
        if changed:
            addrs = [a.strip() for a in changed.split(";") if a.strip()]
            self.update_name_server_address_list(addrs)
        return changed

    def adjust_thread_pool(self) -> None:
        """对应 Java ``MQClientInstance.adjustThreadPool``：遍历 consumerTable。

        ⚠ 被调用的 push consumer 的 ``adjust_thread_pool()`` 在 Java 5.5.1 是 no-op
        （inc/dec 空实现），本实现照抄。异常按 Java 语义逐实例吞掉（``catch (Exception
        ignored)``），保证一个消费者的异常不影响其它消费者。
        """
        for group, consumer in list(self._consumer_table.items()):
            if consumer is None:
                continue
            try:
                consumer.adjust_thread_pool()
            except Exception as e:  # noqa: BLE001 —— Java 也是 catch (Exception ignored)
                logger.debug("adjustThreadPool failed for group %s: %s", group, e)

    # ---------------- 消费者注册（broker 主动请求按 group 分派） ----------------
    def register_consumer(self, group: str, consumer: "DefaultMQPushConsumer") -> None:
        """登记一个消费者实例，供 broker 主动请求按 consumerGroup 分派到正确的实例。

        对应 Java ``MQClientInstance.consumerTable`` + ``findConsumer``。
        """
        self._consumer_table[group] = consumer

    def unregister_consumer(self, group: str) -> None:
        self._consumer_table.pop(group, None)

    def find_consumer(self, group: str) -> Optional["DefaultMQPushConsumer"]:
        return self._consumer_table.get(group)

    # ---------------- CHECK_CLIENT_CONFIG(46)：订阅表达式向 broker 求证 ----------------
    def check_client_in_broker(self) -> None:
        """对应 Java ``MQClientInstance#checkClientInBroker:534``。

        遍历本实例登记的每个消费者的订阅，只把**非 TAG**（SQL92 / CLASS_FILTER）的表达式
        发给 broker 校验。为什么必须发：SQL92 表达式写错时 broker 的
        ``ExpressionMessageFilter`` 在 ConsumeQueue 阶段拿不到编译好的过滤数据就**直接放行**
        （返回 true），也就是静默变成「订阅全部消息」，消费者启动照样成功、一条错误都不报。
        这一调用把「写错的表达式」变成启动期的一次显式失败。

        与 Java 一致的两个细节：
        * 某个消费者「无订阅」时是 ``return`` 而不是 ``continue``（Java 源码如此，
          后面的消费者这一轮就不再查了）；
        * 查不到路由（``findBrokerAddrByTopic`` 返回 None）时**跳过**该订阅而不是报错。
        """
        for group, consumer in list(self._consumer_table.items()):
            subs = consumer.subscriptions() if consumer is not None else None
            if not subs:
                # 对齐 Java 的 `return`：不是 continue。
                return
            self.check_subscriptions_in_broker(group, subs)

    def check_subscriptions_in_broker(self, group: str,
                                      subs: Iterable[SubscriptionData]) -> None:
        """``check_client_in_broker`` 的内层循环，单独暴露给未登记进 consumerTable 的消费者
        （拉模式 / lite 消费者的订阅存在自己那份，Java 通过 ``MQConsumerInner#subscriptions``
        走同一张表，本端口的 consumerTable 只收推模式消费者）。
        """
        for sub in subs:
            # Java ``ExpressionType.isTagType``：null / "" / "TAG" 都算 TAG，一律跳过。
            if sub is None or not sub.expression_type or sub.expression_type == ExpressionType.TAG:
                continue
            addr = self.find_broker_addr_by_topic(sub.topic or "")
            if addr is None:
                continue
            try:
                self.check_client_config(addr, group, self.client_id, sub)
            except MQClientException:
                raise
            except Exception as e:  # noqa: BLE001
                # 连不上/超时也当启动失败：Java 抛的是同一段文案的 MQClientException，
                # 由调用方（consumer.start）收拾。老 broker 不认 46 码时就落在这里。
                raise MQClientException(
                    "Check client in broker error, maybe because you use %s to filter "
                    "message, but server has not been upgraded to support!This error would "
                    "not affect the launch of consumer, but may has impact on message "
                    "receiving if you have use the new features which are not supported by "
                    "server, please check the log!" % sub.expression_type, cause=e)

    # ---------------- 40 NOTIFY_CONSUMER_IDS_CHANGED ----------------
    @property
    def consumer_ids_changed_count(self) -> int:
        """本实例收到过多少次 broker 的 ``NOTIFY_CONSUMER_IDS_CHANGED(40)``。"""
        return self._consumer_ids_changed_count

    def rebalance_immediately(self) -> None:
        """对应 Java ``MQClientInstance#rebalanceImmediately``（一行 ``rebalanceService.wakeup()``）。

        Java 只有一个共享的重平衡线程，唤醒它即可；这里没有实例级重平衡线程，
        改成对 consumerTable 里每个消费者点一次名，让它叫醒自己那份循环
        （等价于 Java ``doRebalance()`` 逐个 ``impl.tryRebalance()``）。
        拉模式消费者没有后台循环，``rebalance_immediately`` 在它们那边是 no-op。
        """
        for consumer in list(self._consumer_table.values()):
            if consumer is None:
                continue
            wake = getattr(consumer, "rebalance_immediately", None)
            if wake is None:
                continue
            try:
                wake()
            except Exception as e:  # noqa: BLE001 —— Java 整段包在 try/catch 里
                logger.warning("rebalance_immediately failed: %s", e)

    def _process_notify_consumer_ids_changed(self, cmd: RemotingCommand,
                                             addr: str) -> Optional[RemotingCommand]:
        """对应 Java ``ClientRemotingProcessor#notifyConsumerIdsChanged``。

        broker 用 ``invokeOneway`` 发的，Java 返回 null ⇒ 不回包。group 只用于日志：
        Java 不读它来决定叫醒谁，整组一起唤醒（本实现照抄）。
        """
        header = NotifyConsumerIdsChangedRequestHeader()
        header.from_ext_fields(cmd.ext_fields)
        self._consumer_ids_changed_count += 1
        logger.info("receive broker's notification[%s], the consumer group: %s changed, "
                    "rebalance immediately", addr, header.consumer_group)
        self.rebalance_immediately()
        return None

    # ---------------- broker 主动请求处理（ClientRemotingProcessor） ----------------
    def _process_reset_offset(self, cmd: RemotingCommand, addr: str) -> Optional[RemotingCommand]:
        """RESET_CONSUMER_CLIENT_OFFSET(220)：broker 用 invokeOneway 发的，无需应答。

        但重置逻辑里会触发 rebalance（lock/unlock/batch 等 invokeSync），不能在读线程上同步
        跑——丢到后台线程，立即返回 None（oneway）。
        """
        header = ResetOffsetRequestHeader()
        header.from_ext_fields(cmd.ext_fields)
        group = header.group
        consumer = self.find_consumer(group) if group else None
        if consumer is None:
            logger.warning("RESET_CONSUMER_CLIENT_OFFSET: no consumer for group=%s", group)
            return None
        try:
            body = ResetOffsetBody.decode(cmd.body) if cmd.body else ResetOffsetBody()
        except Exception as e:  # noqa: BLE001
            logger.warning("RESET_CONSUMER_CLIENT_OFFSET: bad body: %s", e)
            return None
        topic = header.topic
        offset_table: Dict[MessageQueue, int] = body.offset_table

        def _run() -> None:
            try:
                consumer.reset_offset(topic, offset_table)
            except Exception as e:  # noqa: BLE001
                logger.warning("reset offset failed (group=%s topic=%s): %s",
                               group, topic, e)

        t = threading.Thread(target=_run, daemon=True,
                             name="rmq-reset-offset-%s" % (group or "?"))
        t.start()
        return None

    def _process_get_consumer_status(self, cmd: RemotingCommand, addr: str) -> Optional[RemotingCommand]:
        """GET_CONSUMER_STATUS_FROM_CLIENT(221)：返回已消费位点表（Map<MessageQueue,Long>）。"""
        header = GetConsumerStatusRequestHeader()
        header.from_ext_fields(cmd.ext_fields)
        group = header.group
        consumer = self.find_consumer(group) if group else None
        if consumer is None:
            return RemotingCommand.create_response_command(
                ResponseCode.SYSTEM_ERROR, "no consumer for group=%s" % group, None)
        status = consumer.get_consumer_status(header.topic)
        body = GetConsumerStatusBody()
        body.message_queue_table = dict(status)
        resp = RemotingCommand.create_response_command(ResponseCode.SUCCESS, None, None)
        resp.body = body.encode()
        return resp

    def _process_get_consumer_running_info(self, cmd: RemotingCommand, addr: str) -> Optional[RemotingCommand]:
        """GET_CONSUMER_RUNNING_INFO(307)：返回本消费者运行信息。"""
        header = GetConsumerRunningInfoRequestHeader()
        header.from_ext_fields(cmd.ext_fields)
        group = header.consumer_group
        consumer = self.find_consumer(group) if group else None
        if consumer is None:
            return RemotingCommand.create_response_command(
                ResponseCode.SYSTEM_ERROR, "no consumer for group=%s" % group, None)
        info = consumer.consumer_running_info()
        resp = RemotingCommand.create_response_command(ResponseCode.SUCCESS, None, None)
        resp.body = info.encode()
        return resp

    def _process_consume_message_directly(self, cmd: RemotingCommand, addr: str) -> Optional[RemotingCommand]:
        """CONSUME_MESSAGE_DIRECTLY(309)：broker 把一条消息推下来，要求本地真实消费一次。"""
        header = ConsumeMessageDirectlyResultRequestHeader()
        header.from_ext_fields(cmd.ext_fields)
        group = header.consumer_group
        consumer = self.find_consumer(group) if group else None
        if consumer is None:
            return RemotingCommand.create_response_command(
                ResponseCode.SYSTEM_ERROR, "no consumer for group=%s" % group, None)
        if not cmd.body:
            return RemotingCommand.create_response_command(
                ResponseCode.SYSTEM_ERROR, "empty message body", None)
        msg = decode_message(cmd.body, check_crc=False)
        if msg is None:
            return RemotingCommand.create_response_command(
                ResponseCode.SYSTEM_ERROR, "decode message failed", None)
        result = consumer.consume_message_directly(msg, header.broker_name)
        resp = RemotingCommand.create_response_command(ResponseCode.SUCCESS, None, None)
        resp.body = result.encode()
        return resp

    # ---------------- 生命周期 ----------------
    def start(self) -> None:
        self._started = True
        # 消费统计采样线程（Java ConsumerStatsManager.start 是空的——采样挂在
        # 各 StatsItem 的调度器上；这里收敛为一个统一线程，采样精度不变 10s）
        self.consumer_stats_manager.start()
        # 动态 name server（Java MQClientInstance.start:344-348）：**当且仅当**没配置
        # 地址时先 fetch 一次；取不到就直接报错（比 Java 更严格——Java 会让运行期各处
        # 各自失败，这里在 start 时给一个明确错误，行为可预期）。
        dynamic_ns = (not self.name_server_addrs
                      and self.top_addressing is not None
                      and bool(self.top_addressing.ws_addr))
        if dynamic_ns:
            self.fetch_name_server_addr()
            if not self.name_server_addrs:
                # Java 在这一步不报错（``MQClientInstance.start`` 只是 fetch 一次，取不到
                # 照样启动），故障要等到第一次发送才以 ``validateNameServerSetting`` 的
                # 10004 冒出来；本端口在 start 时就失败（更可预期），码值仍用同一条 10004，
                # 别让"配了地址服务器却没返回地址"落到 no-route(10005) 那种误导性文案上。
                raise MQClientException(
                    "name server address is not set and address server (%s) returned none"
                    % self.top_addressing.ws_addr,
                    ClientErrorCode.NO_NAME_SERVER_EXCEPTION)
            # 周期刷新（Java scheduleAtFixedRate(fetchNameServerAddr, 10s, 2min)）
            self._namesrv_refresh_stop.clear()
            t = threading.Thread(target=self._namesrv_refresh_loop, daemon=True,
                                 name="rmq-namesrv-refresh-%s" % self.client_id)
            t.start()
            self._namesrv_refresh_thread = t
        if self._route_refresh_thread is None:
            self._route_refresh_stop.clear()
            t = threading.Thread(target=self._route_refresh_loop, daemon=True,
                                 name="rmq-route-refresh-%s" % self.client_id)
            t.start()
            self._route_refresh_thread = t
        if self._adjust_pool_thread is None:
            self._adjust_pool_stop.clear()
            t = threading.Thread(target=self._adjust_thread_pool_loop, daemon=True,
                                 name="rmq-adjust-pool-%s" % self.client_id)
            t.start()
            self._adjust_pool_thread = t

    def shutdown(self) -> None:
        self._started = False
        self._route_refresh_stop.set()
        self._adjust_pool_stop.set()
        self._namesrv_refresh_stop.set()
        self.consumer_stats_manager.shutdown()
        self.remoting_client.shutdown()

    def _namesrv_refresh_loop(self) -> None:
        """动态 name server 周期刷新。

        Java ``MQClientInstance.startScheduledTask``::

            if (null == this.clientConfig.getNamesrvAddr()) {
                scheduledExecutorService.scheduleAtFixedRate(() -> fetchNameServerAddr(),
                    1000 * 10, 1000 * 60 * 2, MILLISECONDS);
            }

        ——首次延迟 10s、周期 2 分钟，且只在"未配置静态地址"时调度。
        """
        if self._namesrv_refresh_stop.wait(10.0):    # Java initialDelay = 10s
            return
        while not self._namesrv_refresh_stop.wait(120.0):
            if not self._started:
                return
            try:
                self.fetch_name_server_addr()
            except Exception as e:  # noqa: BLE001
                logger.debug("fetchNameServerAddr exception: %s", e)

    def _adjust_thread_pool_loop(self) -> None:
        """周期触发线程弹性巡检。

        Java ``MQClientInstance.startScheduledTask``::

            scheduleAtFixedRate(() -> adjustThreadPool(), 1, 1, TimeUnit.MINUTES);

        ——首次延迟 1 分钟、周期 1 分钟。虽然 inc/dec 是空实现（见
        ``DefaultMQPushConsumer.adjust_thread_pool``），调度本身照抄以保持行为一致。
        """
        if self._adjust_pool_stop.wait(60.0):    # Java initialDelay = 1 分钟
            return
        while not self._adjust_pool_stop.wait(60.0):
            if not self._started:
                return
            self.adjust_thread_pool()

    def register_topic_in_use(self, topic: str) -> None:
        """登记需要在后台周期刷新路由的 topic（对应 Java 的订阅/发布 topic 列表）。"""
        if topic:
            self._topics_in_use.add(topic)

    def _route_refresh_loop(self) -> None:
        """周期刷新在用 topic 的路由（对应 Java MQClientInstance.startScheduledTask 中
        ``scheduleAtFixedRate(updateTopicRouteInfoFromNameServer, 10, pollNameServerInterval)``，
        默认 30s）。没有这个任务，路由变化（如新 topic 被 broker 创建、队列扩容）只能等
        消费者自己的 rebalance 轮次或生产者的下次发送才被发现。
        """
        if self._route_refresh_stop.wait(0.01):  # Java 首个任务延迟 10ms
            return
        while not self._route_refresh_stop.wait(30.0):
            if not self._started:
                return
            for topic in list(self._topics_in_use):
                try:
                    self.update_topic_route_info_from_name_server(topic)
                except Exception as e:  # noqa: BLE001
                    logger.debug("route refresh failed for %s: %s", topic, e)

    def update_name_server_address_list(self, addrs: List[str]) -> None:
        if addrs:
            self.name_server_addrs = list(addrs)

    # ---------------- 底层 invoke ----------------
    def _invoke_sync(self, addr: str, request: RemotingCommand,
                     timeout_millis: Optional[int] = None) -> RemotingCommand:
        return self.remoting_client.invoke_sync(addr, request, timeout_millis)

    def _check_response(self, response: RemotingCommand) -> RemotingCommand:
        if response.code == ResponseCode.SUCCESS:
            return response
        raise MQBrokerException(response.code, response.remark or "")

    # ---------------- 路由管理 ----------------
    def update_topic_route_info_from_name_server(self, topic: str, timeout_millis: int = 5000,
                                                 is_default: bool = False) -> bool:
        """拉取并落库 topic 路由。

        ``is_default`` 对应 Java ``MQClientInstance.updateTopicRouteInfoFromNameServer(topic,
        isDefault, defaultMQProducer)``：**只有生产者**在真实路由拉不到时才回退到默认 topic
        （TBW102）来为新 topic 合成发布信息（见 Java DefaultMQProducerImpl:905）。
        消费者路径**绝不允许**兜底——否则 ``%RETRY%group`` 这类尚未由 broker 创建的主题会
        被合成出一组假队列，两个实例在不同时刻拉取会得到不同的队列数，rebalance 视图不一致。
        """
        if not self.name_server_addrs:
            # Java 这里只 log.warn 并返回 false，故障最终由生产者的
            # ``validateNameServerSetting`` 以 10004 报出；本端口的路由拉取是拉不到就抛，
            # 所以直接把同一个码带上 —— 一个地址都没有时不能报成"这个 topic 没路由"(10005)。
            raise MQClientException("name server address list is empty",
                                    ClientErrorCode.NO_NAME_SERVER_EXCEPTION)

        def _fetch(t: str):
            request = RemotingCommand.create_request_command(RequestCode.GET_ROUTEINFO_BY_TOPIC, None)
            request.ext_fields["topic"] = t
            last_exc = None
            for ns_addr in self.name_server_addrs:
                try:
                    response = self._invoke_sync(ns_addr, request, timeout_millis)
                    if response.code == ResponseCode.SUCCESS and response.body:
                        return TopicRouteData.decode(response.body)
                    break
                except Exception as e:  # noqa: BLE001
                    last_exc = e
            if last_exc is not None and not isinstance(last_exc, MQBrokerException):
                raise last_exc
            return None

        route = _fetch(topic)
        if route is None and is_default and topic != MixAll.DEFAULT_TOPIC:
            # RocketMQ 5.x nameServer 不为未知 topic 合成默认路由（返回 TOPIC_NOT_EXIST），
            # **生产者**需要像 Java 客户端那样回退到默认 topic（TBW102）来为该 topic
            # 构造发布信息。消费者不做这个兜底（见方法 docstring）。
            route = _fetch(MixAll.DEFAULT_TOPIC)
            if route is not None:
                # 新 topic 由 broker 用 default_topic_queue_nums 创建队列，而默认 topic 自身
                # 可能配置了更多队列，这里按 broker 实际创建数裁剪，避免选中非法 queueId。
                cap = MixAll.DEFAULT_TOPIC_QUEUE_NUMS
                for qd in route.queue_datas:
                    qd.write_queue_nums = min(qd.write_queue_nums, cap)
                    qd.read_queue_nums = min(qd.read_queue_nums, cap)
        if route is None:
            return False
        with self.topic_route_lock:
            self.topic_route_table[topic] = route
            publish = self.topic_publish_info_table.setdefault(topic, TopicPublishInfo())
            publish.order_topic = route.order_topic_conf is not None
            publish.topic_route_data = route
            publish.msg_queue_list = route.get_all_message_queue(topic)
        return True

    def get_topic_publish_info(self, topic: str, is_default: bool = False) -> TopicPublishInfo:
        with self.topic_route_lock:
            info = self.topic_publish_info_table.get(topic)
            if info is not None and info.ok():
                return info
        self.update_topic_route_info_from_name_server(topic, is_default=is_default)
        with self.topic_route_lock:
            info = self.topic_publish_info_table.get(topic)
            if info is None or not info.ok():
                raise MQClientException("Can not find Message Queue for topic: %s" % topic)
            return info

    def get_topic_route_data(self, topic: str) -> Optional[TopicRouteData]:
        with self.topic_route_lock:
            route = self.topic_route_table.get(topic)
            if route is not None:
                return route
        try:
            self.update_topic_route_info_from_name_server(topic)
        except Exception:  # noqa: BLE001
            pass
        with self.topic_route_lock:
            return self.topic_route_table.get(topic)

    @staticmethod
    def find_broker_addr_in_route(route: TopicRouteData, broker_name: str) -> Optional[str]:
        for broker_data in route.get_broker_datas():
            if broker_data.broker_name == broker_name:
                return broker_data.select_broker_addr()
        return None

    def find_broker_addr_by_topic(self, topic: str) -> Optional[str]:
        """对应 Java ``MQClientInstance#findBrokerAddrByTopic:1390``：从路由里随机挑一个
        broker（优先 master 地址）；查不到返回 ``None``（不抛）。

        与 ``_broker_addr_for_topic``（拿不到就抛，给管理类 API 用）的分工照抄 Java：
        这里返回 null 由调用方决定跳过。与 Java 的一点不同：Java 只读缓存（它依赖调用前的
        ``updateTopicSubscribeInfoWhenSubscriptionChanged`` 预热），这里用
        ``get_topic_route_data``，缓存空了会顺手补拉一次。
        """
        route = self.get_topic_route_data(topic)
        if route is None:
            return None
        brokers = route.get_broker_datas()
        if not brokers:
            return None
        return random.choice(brokers).select_broker_addr()

    # ---------------- 消息发送 ----------------
    def send_message(self, producer_group: str, msg: Message, mq: MessageQueue,
                     timeout_millis: int = 3000, sys_flag: int = 0,
                     unit_mode: bool = False,
                     default_topic: Optional[str] = None,
                     default_topic_queue_nums: Optional[int] = None) -> SendResult:
        addr = self.find_broker_addr_in_route(self.get_topic_route_data(mq.topic), mq.broker_name) if self.get_topic_route_data(mq.topic) else None
        if addr is None:
            route = self.get_topic_route_data(mq.topic)
            if route is None:
                raise MQClientException("No route info of this topic: %s" % mq.topic)
            addr = MQClientInstance.find_broker_addr_in_route(route, mq.broker_name)
            if addr is None:
                raise MQClientException("Broker %s not found in route of topic %s" % (mq.broker_name, mq.topic))
        request = self._build_send_request(producer_group, msg, mq, timeout_millis, sys_flag,
                                           unit_mode, default_topic, default_topic_queue_nums)
        response = self._invoke_sync(addr, request, timeout_millis)
        return self._parse_send_response(response, msg, mq)

    def send_message_to_addr(self, producer_group: str, msg: Message, mq: MessageQueue,
                             addr: str, timeout_millis: int = 3000,
                             sys_flag: int = 0, unit_mode: bool = False,
                             default_topic: Optional[str] = None,
                             default_topic_queue_nums: Optional[int] = None) -> SendResult:
        request = self._build_send_request(producer_group, msg, mq, timeout_millis, sys_flag,
                                           unit_mode, default_topic, default_topic_queue_nums)
        response = self._invoke_sync(addr, request, timeout_millis)
        return self._parse_send_response(response, msg, mq)

    def send_message_oneway(self, producer_group: str, msg: Message, mq: MessageQueue,
                            addr: str, timeout_millis: int = 3000,
                            sys_flag: int = 0, unit_mode: bool = False,
                            default_topic: Optional[str] = None,
                            default_topic_queue_nums: Optional[int] = None) -> None:
        request = self._build_send_request(producer_group, msg, mq, timeout_millis, sys_flag,
                                           unit_mode, default_topic, default_topic_queue_nums)
        request.mark_oneway_rpc()
        self.remoting_client.invoke_oneway(addr, request)

    def build_send_request(self, producer_group: str, msg: Message, mq: MessageQueue,
                           timeout_millis: int = 3000, sys_flag: int = 0,
                           unit_mode: bool = False,
                           default_topic: Optional[str] = None,
                           default_topic_queue_nums: Optional[int] = None) -> RemotingCommand:
        """只**构建** SEND_MESSAGE 请求对象、不发送。

        异步发送链需要跨重试复用同一个请求（Java ``onExceptionImpl:728-730`` 只做
        ``request.setOpaque(createNewRequestId())`` 再递归），所以请求构建要拆出来给调用方持有。
        """
        return self._build_send_request(producer_group, msg, mq, timeout_millis, sys_flag,
                                        unit_mode, default_topic, default_topic_queue_nums)

    def send_message_async(self, addr: str, request: RemotingCommand, msg: Message,
                           mq: MessageQueue, timeout_millis: int,
                           on_complete: Callable[[Optional[SendResult], Optional[BaseException]],
                                                 None]) -> None:
        """异步发出一个已构建好的发送请求（对应 Java ``MQClientAPIImpl#sendMessageAsync``）。

        ``on_complete(result, error)`` 恰好回调一次（由 remoting 层保证）。响应解析也放在这一层
        做，和 Java 一样：``processSendResponse`` 是在 ``operationSucceed`` 里调的，它抛出的
        ``MQBrokerException`` 会流进 ``onExceptionImpl`` —— 也就是说 broker 明确回了错误码时
        **不会**换 broker 重试（needRetry=false），只有连不上/超时/发不出去才重试。这一点和同步
        发送的 ``retryResponseCodes`` 语义**不同**，别照搬。

        抛异常（``addr`` 解析不到、remoting 层就地拒绝）不走回调，直接向上抛，
        对应 Java ``sendMessageAsync`` 外层 try-catch 的 ``needRetry=true`` 分支。
        """
        def _callback(response: Optional[RemotingCommand],
                      error: Optional[BaseException]) -> None:
            if error is not None:
                on_complete(None, error)
                return
            try:
                on_complete(self._parse_send_response(response, msg, mq), None)
            except Exception as parse_error:  # noqa: BLE001 — 解析失败也要交给调用方
                on_complete(None, parse_error)

        self.remoting_client.invoke_async(addr, request, _callback, timeout_millis)

    def _build_send_request(self, producer_group: str, msg: Message, mq: MessageQueue,
                            timeout_millis: int = 3000, sys_flag: int = 0,
                            unit_mode: bool = False,
                            default_topic: Optional[str] = None,
                            default_topic_queue_nums: Optional[int] = None) -> RemotingCommand:
        """sys_flag 由 Producer 算好（压缩标志 + 压缩类型位），见
        DefaultMQProducer.try_to_compress_message。

        ``default_topic`` / ``default_topic_queue_nums`` 对位 Java
        ``sendKernelImpl:996-997`` 读的生产者 ``createTopicKey`` 与
        ``defaultTopicQueueNums``（不传则用 ``TBW102`` / 4 这两个 Java 默认值）。
        """
        # 对齐 Java DefaultMQProducerImpl.sendKernelImpl:932-935：非批量消息在
        # **发请求之前**补一个客户端唯一 ID（UNIQ_KEY）；批量消息的 ID 在
        # MessageBatch.generateFromList 时已逐条写好，不覆盖。
        # 这个字段决定 SendResult.msgId 的取值，也是消息轨迹里 msgId 的来源 ——
        # 控制台正是按它把发送轨迹与消费轨迹串起来。
        if not isinstance(msg, MessageBatch):
            set_uniq_id(msg)
        header = SendMessageRequestHeaderV2()
        header.producer_group = producer_group
        header.topic = msg.topic
        header.default_topic = default_topic or MixAll.DEFAULT_TOPIC
        header.default_topic_queue_nums = (default_topic_queue_nums
                                          if default_topic_queue_nums is not None
                                          else MixAll.DEFAULT_TOPIC_QUEUE_NUMS)
        header.queue_id = mq.queue_id
        header.sys_flag = sys_flag
        header.born_timestamp = int(time.time() * 1000)
        header.flag = msg.flag
        header.properties = message_properties_2_string(msg.properties)
        header.reconsume_times = 0
        header.unit_mode = unit_mode
        # ⚠ maxReconsumeTimes 只在「发往 %RETRY% 且消息带 MAX_RECONSUME_TIMES 属性」时
        # 才下发（Java sendKernelImpl:1003-1018）。客户端版本升到 V3_4_9 之后 broker
        # 会无条件采信这个字段（AbstractSendMessageProcessor:172-179），固定发 0 会让
        # 重试消息第一次回投就判定 reconsumeTimes(0) >= 0 直接进 %DLQ%。
        header.max_reconsume_times = None
        # Java sendKernelImpl:1004-1018：发往 ``%RETRY%<group>`` 时这两个重试属性要**抬进
        # 请求头**，因为 broker 的 handleRetryAndDLQ（SendMessageProcessor:197-210）读的是
        # requestHeader.reconsumeTimes / maxReconsumeTimes，而不是报文里的属性；漏抬的后果
        # 不是报错而是**静默错位**：broker 退回用订阅组默认的 retryMaxTimes(16) 判定，
        # 该三次就进死信的消息会在重试 topic 上一直转。
        # ⚠ 抬完的 clear 只作用在本地这条 msg 上：properties 字符串在上面已经序列化过，
        #   Java 也是这个先后顺序（线上属性里 RECONSUME_TIME 依然在），别"顺手"调换。
        if header.topic.startswith(MixAll.RETRY_GROUP_TOPIC_PREFIX):
            reconsume_times = MessageAccessor.get_reconsume_time(msg)
            if reconsume_times is not None:
                header.reconsume_times = int(reconsume_times)
                MessageAccessor.clear_property(msg, MessageConst.PROPERTY_RECONSUME_TIME)
            max_reconsume_times = MessageAccessor.get_max_reconsume_times(msg)
            if max_reconsume_times is not None:
                header.max_reconsume_times = int(max_reconsume_times)
                MessageAccessor.clear_property(msg, MessageConst.PROPERTY_MAX_RECONSUME_TIMES)
        header.batch = isinstance(msg, MessageBatch)
        # Java sendKernelImpl:1007 `requestHeader.setBrokerName(brokerName)` —— V2 的键是
        # 单字母 `n`（SendMessageRequestHeaderV2:63）。发往哪台 broker 由路由选中，
        # 这里跟着 mq 走：请求跨重试复用时它也保持第一次的那个值（Java 同样只建一次头）。
        header.broker_name = mq.broker_name or None
        # Request-Reply：MSG_TYPE == "reply" 的应答消息走 SEND_REPLY_MESSAGE_V2(325)，
        # 而不是普通的 SEND_MESSAGE_V2(310)。broker 只在 324/325 上注册了
        # ReplyMessageProcessor（它负责按 REPLY_TO_CLIENT 把应答推回请求方）。
        # 批量消息走 SEND_BATCH_MESSAGE(320)：Java 的判据是 msg instanceof MessageBatch
        # （MQClientAPIImpl:562），先判 reply 再判 batch。
        # 对齐 Java MQClientAPIImpl.sendMessage:550-563（sendSmartMsg 默认 true → V2）。
        if is_reply_message(msg):
            code = RequestCode.SEND_REPLY_MESSAGE_V2
        elif header.batch:
            code = RequestCode.SEND_BATCH_MESSAGE
        else:
            code = RequestCode.SEND_MESSAGE_V2
        request = RemotingCommand.create_request_command(code, header)
        request.body = self._encode_body(msg)
        return request

    @staticmethod
    def _encode_body(msg: Message) -> bytes:
        """对应 Java MQClientAPIImpl.sendMessage：request.setBody(msg.getBody())。

        普通消息的 body 就是原始消息体；批量消息的 body 由 MessageBatch 在构造时
        用 MessageDecoder.encodeMessages 编码好（DefaultMQProducer.batch）。
        """
        return msg.get_body() or b""

    def _parse_send_response(self, response: RemotingCommand, msg: Message, mq: MessageQueue) -> SendResult:
        status_map = {
            ResponseCode.SUCCESS: SendStatus.SEND_OK,
            ResponseCode.FLUSH_DISK_TIMEOUT: SendStatus.FLUSH_DISK_TIMEOUT,
            ResponseCode.FLUSH_SLAVE_TIMEOUT: SendStatus.FLUSH_SLAVE_TIMEOUT,
            ResponseCode.SLAVE_NOT_AVAILABLE: SendStatus.SLAVE_NOT_AVAILABLE,
        }
        if response.code in status_map:
            header = SendMessageResponseHeader()
            header.from_ext_fields(response.ext_fields)
            # 对应 Java MQClientAPIImpl.processSendResponse(:785-793, :794-806)：
            #   msgId         = 客户端唯一 ID：批量消息用**批量自身**的 UNIQ_KEY
            #                   （inner-batch 时 broker 会把它原样回在 batchUniqId 里）
            #   offsetMsgId   = 响应头里的 msgId（broker 生成的 offset 消息 ID）
            #   regionId      = 响应头 MSG_REGION，缺省回落 DefaultRegion
            #   traceOn       = 响应头 TRACE_ON != "false"（broker 默认 true）
            # ⚠ 有意偏离 Java：Java 在 broker **没**回 batchUniqId（= 普通 topic 上的客户端
            # 批量，不是 inner-batch）时把 msgId 换成「逐条子消息 UNIQ_KEY 的逗号串」
            # （processSendResponse:786-793）。本端口的解析层只拿得到「这一条发出去的消息」，
            # 拿不到子消息列表（Rust/C++ 同样如此），而且四语言要能互相对拍 —— 所以统一用批量
            # 自身的 ID。要紧的那一半已对齐：每条子消息在编码前就写好了自己的 UNIQ_KEY
            # （DefaultMQProducer._send_batch），broker 拆开后消费端与轨迹看到的逐条 ID 与
            # Java 完全一致。
            uniq_msg_id = get_uniq_id(msg) or header.batch_uniq_id
            result = SendResult(
                send_status=status_map[response.code],
                msg_id=uniq_msg_id or header.msg_id,
                message_queue=MessageQueue(mq.topic, mq.broker_name, header.queue_id if header.queue_id is not None else mq.queue_id),
                queue_offset=header.queue_offset or 0,
                transaction_id=header.transaction_id,
                offset_msg_id=header.msg_id,
            )
            ext = response.ext_fields or {}
            region_id = ext.get(MessageConst.PROPERTY_MSG_REGION)
            result.region_id = region_id if region_id else MixAll.DEFAULT_TRACE_REGION_ID
            result.trace_on = str(ext.get(MessageConst.PROPERTY_TRACE_SWITCH)) != "false"
            result.recall_handle = header.recall_handle
            return result
        raise MQBrokerException(response.code, response.remark or "")

    # ---------------- 定时消息撤回 ----------------
    def recall_message(self, addr: str, header: RecallMessageRequestHeader,
                       timeout_millis: int) -> str:
        """RECALL_MESSAGE(370)，对应 Java ``MQClientAPIImpl#recallMessage``(:3749-3767)。

        与 Java 一样：SUCCESS 才取响应头的 ``msgId``（被撤回那条消息的 UNIQ_KEY），
        其余码一律抛 ``MQBrokerException``。
        """
        request = RemotingCommand.create_request_command(RequestCode.RECALL_MESSAGE, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        if response.code == ResponseCode.SUCCESS:
            resp_header = RecallMessageResponseHeader()
            resp_header.from_ext_fields(response.ext_fields)
            if not resp_header.msg_id:
                raise MQBrokerException(response.code,
                                        "recall message response has no msgId, addr %s" % addr)
            return resp_header.msg_id
        raise MQBrokerException(response.code, response.remark or "")

    # ---------------- Request-Reply：接收 broker 推回的应答 ----------------
    def _process_reply_message(self, cmd: RemotingCommand, addr: str) -> Optional[RemotingCommand]:
        """处理 PUSH_REPLY_MESSAGE_TO_CLIENT(326)：把应答交给等待中的 request()。

        对应 Java ``ClientRemotingProcessor#receiveReplyMessage``（:222-271）。
        与 Java 一样**必须回一个响应**：broker 侧 ``Broker2Client.callClient`` 是
        ``invokeSync``（10s 超时），不回响应它那边就会超时并记 ``push reply message to
        <id> fail``，应答虽然已经投递成功，broker 日志里却是失败。
        """
        header = ReplyMessageRequestHeader()
        header.from_ext_fields(cmd.ext_fields)
        try:
            body = cmd.body or b""
            # sysFlag 里带压缩标志时要先解压：326 推的是**裸包**，不走消息解码路径
            # （对齐 Java 同处的 Compressor 分支）。
            if MessageSysFlag.is_compressed(header.sys_flag or 0):
                body = decompress_body(body, MessageSysFlag.get_compression_type(header.sys_flag or 0))
            msg = MessageExt(topic=header.topic or "", body=body)
            msg.queue_id = header.queue_id or 0
            msg.store_timestamp = header.store_timestamp or 0
            msg.flag = header.flag or 0
            msg.born_timestamp = header.born_timestamp or 0
            msg.reconsume_times = header.reconsume_times or 0
            if header.born_host:
                msg.born_host = header.born_host
            if header.store_host:
                msg.store_host = header.store_host
            msg.properties = string_2_message_properties(header.properties)
            msg.properties[MessageConst.PROPERTY_REPLY_MESSAGE_ARRIVE_TIME] = str(
                int(time.time() * 1000))
            correlation_id = msg.properties.get(MessageConst.PROPERTY_CORRELATION_ID)
            if REQUEST_FUTURE_HOLDER.put_response(correlation_id, msg) is None:
                # 查不到是正常情况（请求已超时 / 应答重复），Java 此处也是 warn
                logger.warning("receive reply message, but not matched any request, "
                               "CorrelationId: %s, reply from host: %s",
                               correlation_id, header.born_host)
            return RemotingCommand.create_response_command(ResponseCode.SUCCESS, None, None)
        except Exception as e:  # noqa: BLE001
            logger.warning("unknown err when receiveReplyMsg", exc_info=True)
            return RemotingCommand.create_response_command(
                ResponseCode.SYSTEM_ERROR, "process reply message fail: %s" % e, None)

    # ---------------- 消息拉取 ----------------
    def pull_message(self, consumer_group: str, mq: MessageQueue, queue_offset: int,
                     max_msg_nums: int, sys_flag: int, commit_offset: int,
                     subscription: str, sub_version: int, expression_type: str,
                     timeout_millis: int = 30000, max_msg_bytes: int = -1,
                     suspend_timeout_millis: int = 15000, addr: Optional[str] = None,
                     request_source: int = 0) -> "PullResult":
        from .consumer_result import PullResult, PullStatus
        if addr is None:
            route = self.get_topic_route_data(mq.topic)
            if route is None:
                raise MQClientException("No route info of this topic: %s" % mq.topic)
            addr = MQClientInstance.find_broker_addr_in_route(route, mq.broker_name)
            if addr is None:
                raise MQClientException("Broker %s not found in route of topic %s" % (mq.broker_name, mq.topic))
        header = PullMessageRequestHeader()
        header.consumer_group = consumer_group
        header.topic = mq.topic
        header.queue_id = mq.queue_id
        header.queue_offset = queue_offset
        header.max_msg_nums = max_msg_nums
        header.sys_flag = sys_flag
        header.commit_offset = commit_offset
        header.suspend_timeout_millis = suspend_timeout_millis
        header.subscription = subscription
        header.sub_version = sub_version
        header.expression_type = expression_type
        header.max_msg_bytes = max_msg_bytes
        header.request_source = request_source
        request = RemotingCommand.create_request_command(RequestCode.PULL_MESSAGE, header)
        response = self._invoke_sync(addr, request, timeout_millis)

        status = PullStatus.NO_NEW_MSG
        if response.code == ResponseCode.SUCCESS:
            status = PullStatus.FOUND
        elif response.code == ResponseCode.PULL_NOT_FOUND:
            status = PullStatus.NO_NEW_MSG
        elif response.code == ResponseCode.PULL_OFFSET_MOVED:
            status = PullStatus.OFFSET_ILLEGAL
        elif response.code == ResponseCode.PULL_RETRY_IMMEDIATELY:
            status = PullStatus.NO_MATCHED_MSG
        else:
            raise MQBrokerException(response.code, response.remark or "")

        resp_header = PullMessageResponseHeader()
        resp_header.from_ext_fields(response.ext_fields)
        found = []
        if response.body:
            found = decode_messages(response.body)
            for m in found:
                m.broker_name = mq.broker_name
                m.queue_id = mq.queue_id
        return PullResult(status, resp_header.next_begin_offset or 0,
                          resp_header.min_offset or 0, resp_header.max_offset or 0, found)

    # ---------------- POP 模式（5.x 轻量消费） ----------------

    def pop_message(self, consumer_group: str, topic: str, queue_id: int = -1,
                    max_msg_nums: int = 32, invisible_time: int = 60000,
                    poll_time: int = 0, init_mode: int = 0,
                    exp: Optional[str] = None, exp_type: Optional[str] = None,
                    order: bool = False, broker_name: Optional[str] = None,
                    timeout_millis: int = 10000, addr: Optional[str] = None) -> "PopResult":
        """POP 弹取消息（``RequestCode.POP_MESSAGE = 200050``）。

        与 pull 的语义差别：
        - **不需要提交位点**，消费完成后用 ``ack_message`` 确认；
        - 不 ack 的消息在 ``invisible_time`` 之后被 broker 复活并重投到
          ``%RETRY%<group>_<topic>``（V1），下次 POP 会再弹回来 —— 至少一次语义；
        - ``queue_id = -1`` 表示弹该 topic 的所有队列。

        ``born_time`` 必须是当前毫秒时间戳：broker 用
        ``now - bornTime - pollTime > 500`` 判定"超时太久"并直接回
        ``POLLING_TIMEOUT(210)``，填 0 会必然失败。
        """
        from .consumer_result import PopResult, PopStatus
        from ..remoting.protocol import extra_info as ei
        from ..remoting.protocol.headers import (PopMessageRequestHeader,
                                                 PopMessageResponseHeader)

        if addr is None or broker_name is None:
            route = self.get_topic_route_data(topic)
            if route is None:
                raise MQClientException("No route info of this topic: %s" % topic)
            brokers = route.get_broker_datas()
            if not brokers:
                raise MQClientException("No broker in route of topic: %s" % topic)
            bd = brokers[0]
            broker_name = broker_name or bd.broker_name
            if addr is None:
                addr = bd.select_broker_addr()
                if addr is None:
                    raise MQClientException("No available broker addr for topic: %s" % topic)

        header = PopMessageRequestHeader()
        header.consumer_group = consumer_group
        header.topic = topic
        header.queue_id = queue_id
        header.max_msg_nums = max_msg_nums
        header.invisible_time = invisible_time
        header.poll_time = poll_time
        header.born_time = int(time.time() * 1000)
        header.init_mode = init_mode
        header.exp_type = exp_type
        header.exp = exp
        header.order = order
        request = RemotingCommand.create_request_command(RequestCode.POP_MESSAGE, header)
        response = self._invoke_sync(addr, request, timeout_millis)

        # 响应码映射照 Java MQClientAPIImpl.processPopResponse。
        if response.code == ResponseCode.SUCCESS:
            status = PopStatus.FOUND
        elif response.code == ResponseCode.POLLING_FULL:
            status = PopStatus.POLLING_FULL
        elif response.code == ResponseCode.POLLING_TIMEOUT:
            status = PopStatus.POLLING_NOT_FOUND
        elif response.code == ResponseCode.PULL_NOT_FOUND:
            status = PopStatus.POLLING_NOT_FOUND
        else:
            raise MQBrokerException(response.code, response.remark or "")

        resp_header = PopMessageResponseHeader()
        resp_header.from_ext_fields(response.ext_fields)

        found: List[MessageExt] = []
        if status == PopStatus.FOUND and response.body:
            found = decode_messages(response.body)
            # 必须在改写 topic 之前反构 POP_CK —— retryFlag 是从消息**原始** topic
            # 推出来的（broker 可能改写 topic，见 Java buildQueueOffsetSortedMap 注释）。
            self._stamp_pop_ck(found, broker_name, resp_header)

        # Java processPopResponse 收尾：统一盖 brokerName，并把 topic 还原成请求的
        # topic（不带命名空间），这样消费方不用关心 retry topic。
        for m in found:
            m.broker_name = broker_name
            m.topic = topic

        return PopResult(status, found,
                         resp_header.rest_num or 0,
                         resp_header.pop_time or 0,
                         resp_header.invisible_time or 0,
                         resp_header.revive_qid or 0,
                         resp_header.start_offset_info,
                         resp_header.msg_offset_info,
                         resp_header.order_count_info)

    @staticmethod
    def _pop_queue_map_key(extra, m: MessageExt) -> str:
        """``startOffsetInfo`` / ``msgOffsetInfo`` / sortMap 的查表 key。

        Java ``getStartOffsetInfoMapKey(topic, popCk, key)`` 的规则：消息**已带**
        ``POP_CK``（说明是从 retry topic 弹回来的）时 retryFlag 取自 POP_CK 第 5 段，
        否则由消息 topic 推断 —— 因为 broker 可能改写 topic。
        """
        ck = m.properties.get(MessageConst.PROPERTY_POP_CK)
        if ck:
            return extra.get_retry(extra.split(ck)) + "@" + str(m.queue_id)
        return extra.get_start_offset_info_map_key(m.topic, m.queue_id)

    def _stamp_pop_ck(self, found: List[MessageExt], broker_name: str, resp_header) -> None:
        """给 POP 出来的消息反构 ``POP_CK`` 与 ``1ST_POP_TIME``。

        **这是 POP 最容易踩的坑**：普通 topic 直连 POP 时 broker **不写** ``POP_CK``
        属性（只在 retry topic 重编码路径才写），必须由客户端用响应头的
        ``startOffsetInfo`` / ``msgOffsetInfo`` 反构 —— 没有它就无法发 ACK。
        逐条对齐 Java ``MQClientAPIImpl.processPopResponse``。
        """
        from ..remoting.protocol import extra_info as extra

        pop_time = resp_header.pop_time or 0
        invisible_time = resp_header.invisible_time or 0
        revive_qid = resp_header.revive_qid or 0

        if not resp_header.start_offset_info:
            # Java 的 startOffsetInfo == null 分支：用消息自身 queueOffset 当
            # ckQueueOffset 拼 7 段，再手工补一段凑成 8 段。
            per_queue: Dict[str, str] = {}
            for m in found:
                key = str(m.topic) + str(m.queue_id)
                if key not in per_queue:
                    per_queue[key] = extra.build_extra_info(
                        m.queue_offset, pop_time, invisible_time, revive_qid,
                        m.topic, broker_name, m.queue_id)
                m.properties[MessageConst.PROPERTY_POP_CK] = (
                    per_queue[key] + extra.KEY_SEPARATOR + str(m.queue_offset))
        else:
            start_map = extra.parse_start_offset_info(resp_header.start_offset_info) or {}
            msg_map = extra.parse_msg_offset_info(resp_header.msg_offset_info) or {}

            # Java 先按队列收集 queueOffset 并排序，再用 indexOf 求下标去取
            # msgOffsetInfo 里对应的 msgQueueOffset。
            sorted_offsets: Dict[str, List[int]] = {}
            for m in found:
                queue_key = self._pop_queue_map_key(extra, m)
                sorted_offsets.setdefault(queue_key, []).append(m.queue_offset)
            for offsets in sorted_offsets.values():
                offsets.sort()

            for m in found:
                # retry topic 弹回来的消息 broker 已经写好 POP_CK，不能覆盖。
                if m.properties.get(MessageConst.PROPERTY_POP_CK) is not None:
                    continue
                queue_key = self._pop_queue_map_key(extra, m)
                start_offset = start_map.get(queue_key)
                offsets = msg_map.get(queue_key)
                if start_offset is None or not offsets:
                    continue
                try:
                    index = sorted_offsets[queue_key].index(m.queue_offset)
                except ValueError:
                    continue
                if index >= len(offsets):
                    continue
                m.properties[MessageConst.PROPERTY_POP_CK] = extra.build_extra_info(
                    start_offset, pop_time, invisible_time, revive_qid,
                    m.topic, broker_name, m.queue_id, offsets[index])

        # Java 用 computeIfAbsent：只在缺失时补。
        for m in found:
            m.properties.setdefault(MessageConst.PROPERTY_FIRST_POP_TIME, str(pop_time))

    def _addr_for(self, topic: str, broker_name: Optional[str] = None) -> str:
        """按 topic（可选再按 brokerName）解析 broker 地址。"""
        route = self.get_topic_route_data(topic)
        if route is None:
            raise MQClientException("No route info of this topic: %s" % topic)
        if broker_name:
            addr = MQClientInstance.find_broker_addr_in_route(route, broker_name)
            if addr is None:
                raise MQClientException(
                    "Broker %s not found in route of topic %s" % (broker_name, topic))
            return addr
        brokers = route.get_broker_datas()
        if not brokers:
            raise MQClientException("No broker in route of topic: %s" % topic)
        addr = brokers[0].select_broker_addr()
        if addr is None:
            raise MQClientException("No available broker addr for topic: %s" % topic)
        return addr

    def ack_message(self, consumer_group: str, topic: str, queue_id: int,
                    extra_info: str, offset: int, broker_name: Optional[str] = None,
                    timeout_millis: int = 3000, addr: Optional[str] = None) -> int:
        """确认一条 POP 消息（``RequestCode.ACK_MESSAGE = 200051``）。

        ``extra_info`` 用消息上的 ``POP_CK`` 属性；``offset`` 必须是
        **consumeQueue offset**（即 CK 串第 8 段 / msgQueueOffset），不是 commitlog
        offset —— 传错 broker 会回 ``NO_MESSAGE``。

        返回 broker 响应码，``ResponseCode.SUCCESS`` 即确认成功。
        """
        from ..remoting.protocol import extra_info as extra
        from ..remoting.protocol.headers import AckMessageRequestHeader

        if broker_name is None:
            broker_name = extra.get_broker_name(extra.split(extra_info))
        if addr is None:
            addr = self._addr_for(topic, broker_name)

        header = AckMessageRequestHeader()
        header.consumer_group = consumer_group
        header.topic = topic
        header.queue_id = queue_id
        header.extra_info = extra_info
        header.offset = offset
        request = RemotingCommand.create_request_command(RequestCode.ACK_MESSAGE, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        return response.code

    def change_invisible_time(self, consumer_group: str, topic: str, queue_id: int,
                              extra_info: str, offset: int, invisible_time: int,
                              broker_name: Optional[str] = None,
                              timeout_millis: int = 3000,
                              addr: Optional[str] = None) -> "ChangeInvisibleTimeResult":
        """延长 POP 消息的不可见时间（``CHANGE_MESSAGE_INVISIBLETIME = 200053``）。

        用于"还在处理、别让 broker 复活重投"的场景。响应返回的是**新的**
        popTime / invisibleTime / reviveQid，客户端据此重建 8 段 extraInfo
        （返回在结果的 ``extra_info`` 字段里）供后续 ACK 使用 —— 不是原来那个旧串。
        """
        from .consumer_result import ChangeInvisibleTimeResult
        from ..remoting.protocol import extra_info as extra
        from ..remoting.protocol.headers import (ChangeInvisibleTimeRequestHeader,
                                                 ChangeInvisibleTimeResponseHeader)

        if broker_name is None:
            broker_name = extra.get_broker_name(extra.split(extra_info))
        if addr is None:
            addr = self._addr_for(topic, broker_name)

        header = ChangeInvisibleTimeRequestHeader()
        header.consumer_group = consumer_group
        header.topic = topic
        header.queue_id = queue_id
        header.extra_info = extra_info
        header.offset = offset
        header.invisible_time = invisible_time
        request = RemotingCommand.create_request_command(
            RequestCode.CHANGE_MESSAGE_INVISIBLETIME, header)
        response = self._invoke_sync(addr, request, timeout_millis)

        resp_header = ChangeInvisibleTimeResponseHeader()
        resp_header.from_ext_fields(response.ext_fields)

        new_extra = None
        if response.code == ResponseCode.SUCCESS:
            # 与 Java MQClientAPIImpl.changeInvisibleTimeAsync 一致：
            # 7 段 build（ckQueueOffset 用本次的 offset）再补一段凑 8 段。
            new_extra = extra.build_extra_info(
                offset, resp_header.pop_time, resp_header.invisible_time,
                resp_header.revive_qid, topic, broker_name, queue_id,
                offset)
        return ChangeInvisibleTimeResult(response.code,
                                         resp_header.pop_time or 0,
                                         resp_header.invisible_time or 0,
                                         resp_header.revive_qid or 0,
                                         new_extra)

    # ---------------- Offset 查询/更新 ----------------
    # set_zero_if_not_found 默认 False：Java 的 fetchConsumeOffsetFromBroker 从不设置该字段，
    # 新消费组因此回 QUERY_NOT_FOUND（None）而非 0，调用方才会按 consume_from_where 算起点。
    # 默认 True 会把首次启动的消费者钉在队首重放历史消息，并让 LAST_OFFSET / TIMESTAMP 形同虚设。
    def query_consumer_offset(self, consumer_group: str, mq: MessageQueue,
                              timeout_millis: int = 5000, addr: Optional[str] = None,
                              set_zero_if_not_found: bool = False) -> Optional[int]:
        if addr is None:
            addr = self._broker_addr(mq)
        header = QueryConsumerOffsetRequestHeader()
        header.consumer_group = consumer_group
        header.topic = mq.topic
        header.queue_id = mq.queue_id
        header.set_zero_if_not_found = set_zero_if_not_found
        request = RemotingCommand.create_request_command(RequestCode.QUERY_CONSUMER_OFFSET, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        if response.code == ResponseCode.QUERY_NOT_FOUND:
            return None
        self._check_response(response)
        resp_header = QueryConsumerOffsetResponseHeader()
        resp_header.from_ext_fields(response.ext_fields)
        return resp_header.offset

    def update_consumer_offset(self, consumer_group: str, mq: MessageQueue, commit_offset: int,
                               timeout_millis: int = 5000, addr: Optional[str] = None) -> None:
        if addr is None:
            addr = self._broker_addr(mq)
        header = UpdateConsumerOffsetRequestHeader()
        header.consumer_group = consumer_group
        header.topic = mq.topic
        header.queue_id = mq.queue_id
        header.commit_offset = commit_offset
        request = RemotingCommand.create_request_command(RequestCode.UPDATE_CONSUMER_OFFSET, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        self._check_response(response)

    def get_max_offset(self, mq: MessageQueue, timeout_millis: int = 5000, addr: Optional[str] = None) -> int:
        if addr is None:
            addr = self._broker_addr(mq)
        header = GetMaxOffsetRequestHeader()
        header.topic = mq.topic
        header.queue_id = mq.queue_id
        request = RemotingCommand.create_request_command(RequestCode.GET_MAX_OFFSET, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        self._check_response(response)
        resp_header = GetMaxOffsetResponseHeader()
        resp_header.from_ext_fields(response.ext_fields)
        return resp_header.offset or 0

    def get_min_offset(self, mq: MessageQueue, timeout_millis: int = 5000, addr: Optional[str] = None) -> int:
        if addr is None:
            addr = self._broker_addr(mq)
        header = GetMinOffsetRequestHeader()
        header.topic = mq.topic
        header.queue_id = mq.queue_id
        request = RemotingCommand.create_request_command(RequestCode.GET_MIN_OFFSET, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        self._check_response(response)
        resp_header = GetMinOffsetResponseHeader()
        resp_header.from_ext_fields(response.ext_fields)
        return resp_header.offset or 0

    def search_offset_by_timestamp(self, mq: MessageQueue, timestamp: int,
                                   timeout_millis: int = 5000, addr: Optional[str] = None) -> int:
        if addr is None:
            addr = self._broker_addr(mq)
        header = SearchOffsetRequestHeader()
        header.topic = mq.topic
        header.queue_id = mq.queue_id
        header.timestamp = timestamp
        request = RemotingCommand.create_request_command(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        self._check_response(response)
        resp_header = SearchOffsetResponseHeader()
        resp_header.from_ext_fields(response.ext_fields)
        return resp_header.offset or 0

    def query_message(self, topic: str, key: str, max_num: int, begin_timestamp: int,
                      end_timestamp: int, timeout_millis: int = 15000,
                      addr: Optional[str] = None,
                      index_type: Optional[str] = None,
                      uniq_key: bool = False) -> Optional[bytes]:
        """按 key 查消息（对应 Java MQClientAPIImpl.queryMessage）。

        ``index_type`` 取 MessageConst.INDEX_KEY_TYPE("K") / INDEX_UNIQUE_TYPE("U")；
        ``uniq_key=True`` 时还会额外下发 extFields["_UNIQUE_KEY_QUERY"]="true"，
        broker 端据此强制走到 uniqKey 索引（并覆盖 maxNum 为默认查询条数）。
        """
        if addr is None:
            addr = self._broker_addr_for_topic(topic)
        header = QueryMessageRequestHeader()
        header.topic = topic
        header.key = key
        header.max_num = max_num
        header.begin_timestamp = begin_timestamp
        header.end_timestamp = end_timestamp
        header.index_type = index_type
        request = RemotingCommand.create_request_command(RequestCode.QUERY_MESSAGE, header)
        if uniq_key:
            request.ext_fields[MixAll.UNIQUE_MSG_QUERY_FLAG] = "true"
        response = self._invoke_sync(addr, request, timeout_millis)
        if response.code == ResponseCode.QUERY_NOT_FOUND:
            return None
        self._check_response(response)
        return response.body

    def _broker_addr_for_topic(self, topic: str) -> str:
        route = self.get_topic_route_data(topic)
        if route is None:
            raise MQClientException("No route info of this topic: %s" % topic)
        brokers = route.get_broker_datas()
        if not brokers:
            raise MQClientException("No broker in route of topic: %s" % topic)
        addr = brokers[0].select_broker_addr()
        if addr is None:
            raise MQClientException("No available broker addr for topic: %s" % topic)
        return addr

    def query_message_all_brokers(self, topic: str, key: str, max_num: int,
                                  begin_timestamp: int, end_timestamp: int,
                                  index_type: Optional[str] = None,
                                  uniq_key: bool = False,
                                  timeout_millis: int = 15000) -> List:
        """对应 Java MQAdminImpl.queryMessage：查该 topic **所有** broker 并合并去重后的消息。

        Java 还会做客户端侧二次校验（uniqKey 命中要求 msgId == key；普通 key 命中要求
        message.keys 拆分后含 key 且 topic 相同），这里保持一致。
        """
        from ..common.message_const import MessageConst
        messages: List = []
        route = self.get_topic_route_data(topic)
        if route is None:
            return messages
        for broker_data in route.get_broker_datas():
            addr = broker_data.select_broker_addr()
            if not addr:
                continue
            try:
                body = self.query_message(topic, key, max_num, begin_timestamp,
                                          end_timestamp, timeout_millis, addr,
                                          index_type, uniq_key)
            except Exception:  # noqa: BLE001
                continue
            if not body:
                continue
            for m in decode_messages(body):
                m.broker_name = broker_data.broker_name
                if uniq_key:
                    if m.msg_id == key:
                        messages.append(m)
                else:
                    keys = m.get_keys()
                    if keys:
                        for k in keys.split(MessageConst.KEY_SEPARATOR):
                            if k == key and m.topic == topic:
                                messages.append(m)
                                break
        messages.sort(key=lambda x: (x.queue_offset or 0))
        return messages[:max_num] if max_num > 0 else messages

    # ---------------- 队列锁（顺序消费） ----------------

    def lock_batch_mq(self, consumer_group: str, client_id: str, mqs: List[MessageQueue],
                      timeout_millis: int = 1000) -> List[MessageQueue]:
        """批量锁队列（对应 Java MQClientAPIImpl.lockBatchMQ，RequestCode.LOCK_BATCH_MQ）。

        按 broker 分组发送；返回 broker 确认锁定成功的队列集（lockOKMQSet）。
        body：LockBatchRequestBody{consumerGroup, clientId, mqSet(JSON 数组)}；
        响应：LockBatchResponseBody.lockOKMQSet。
        """
        from ..remoting.protocol.body import LockBatchRequestBody, LockBatchResponseBody
        from ..remoting.protocol.codes import RequestCode
        from ..remoting.protocol.headers import LockBatchMqRequestHeader
        lock_ok: List[MessageQueue] = []
        by_broker: Dict[str, List[MessageQueue]] = {}
        for mq in mqs:
            by_broker.setdefault(mq.broker_name, []).append(mq)
        for broker_name, broker_mqs in by_broker.items():
            addr = self.broker_addr_of(broker_name)
            if addr is None:
                continue
            body = LockBatchRequestBody()
            body.consumer_group = consumer_group
            body.client_id = client_id
            body.mq_set = [{"topic": m.topic, "brokerName": m.broker_name, "queueId": m.queue_id}
                           for m in broker_mqs]
            request = RemotingCommand.create_request_command(RequestCode.LOCK_BATCH_MQ, LockBatchMqRequestHeader())
            request.body = body.encode()
            try:
                response = self._invoke_sync(addr, request, timeout_millis)
                self._check_response(response)
                rb = LockBatchResponseBody.decode(response.body)
                for d in rb.lock_ok_mq_set:
                    lock_ok.append(MessageQueue(d.get("topic"), d.get("brokerName"), int(d.get("queueId") or 0)))
            except Exception as e:  # noqa: BLE001
                logger.warning("lock_batch_mq failed for broker %s: %s", broker_name, e)
        return lock_ok

    def unlock_batch_mq(self, consumer_group: str, client_id: str, mqs: List[MessageQueue],
                        timeout_millis: int = 1000) -> None:
        """批量解锁队列（对应 Java MQClientAPIImpl.unlockBatchMQ，RequestCode.UNLOCK_BATCH_MQ）。"""
        from ..remoting.protocol.body import UnlockBatchRequestBody
        from ..remoting.protocol.codes import RequestCode
        from ..remoting.protocol.headers import UnlockBatchMqRequestHeader
        by_broker: Dict[str, List[MessageQueue]] = {}
        for mq in mqs:
            by_broker.setdefault(mq.broker_name, []).append(mq)
        for broker_name, broker_mqs in by_broker.items():
            addr = self.broker_addr_of(broker_name)
            if addr is None:
                continue
            body = UnlockBatchRequestBody()
            body.consumer_group = consumer_group
            body.client_id = client_id
            body.mq_set = [{"topic": m.topic, "brokerName": m.broker_name, "queueId": m.queue_id}
                           for m in broker_mqs]
            request = RemotingCommand.create_request_command(RequestCode.UNLOCK_BATCH_MQ, UnlockBatchMqRequestHeader())
            request.body = body.encode()
            try:
                response = self._invoke_sync(addr, request, timeout_millis)
                self._check_response(response)
            except Exception as e:  # noqa: BLE001
                logger.warning("unlock_batch_mq failed for broker %s: %s", broker_name, e)

    # ---------------- 心跳 / 注销 ----------------
    def send_heartbeat(self, addr: str, heartbeat_data: HeartbeatData, timeout_millis: int = 5000) -> None:
        request = RemotingCommand.create_request_command(RequestCode.HEART_BEAT, None)
        request.body = heartbeat_data.encode()
        response = self._invoke_sync(addr, request, timeout_millis)
        self._check_response(response)

    def unregister_client(self, addr: str, client_id: str, producer_group: str,
                          consumer_group: str, timeout_millis: int = 3000) -> None:
        """UNREGISTER_CLIENT(35)：把本 clientId 从这台 broker 摘掉。

        超时用 3000ms —— Java ``MQClientInstance#unregisterClient:1170`` 传的是
        ``clientConfig.getMqClientApiTimeout()``（``ClientConfig.java:81`` = 3 * 1000），
        与发送/拉取的超时预算无关；注销在 shutdown 路径上，预算越短退出越快。
        """
        from ..remoting.protocol.headers import UnregisterClientRequestHeader
        header = UnregisterClientRequestHeader()
        header.client_id = client_id
        # Java 没用的那个槽位传的是 null，`_ext` 会把 None 整个丢掉 —— 空串不一样，它会带着
        # `consumerGroup: ""` 上线，而 broker 的 ClientManageProcessor:228/237 判的是
        # `group != null`，于是会拿 "" 去 findSubscriptionGroupConfig 并白做一轮 unregister。
        # 纯空白同样按「没这个组」处理：合法组名不可能全是空白（Validators 那一关就过不去），
        # 传进来只可能是调用方漏了值，与空串同处理。
        header.producer_group = (producer_group or "").strip() or None
        header.consumer_group = (consumer_group or "").strip() or None
        request = RemotingCommand.create_request_command(RequestCode.UNREGISTER_CLIENT, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        self._check_response(response)

    def check_client_config(self, broker_addr: str, consumer_group: str, client_id: str,
                            subscription_data: SubscriptionData,
                            timeout_millis: int = 3000) -> None:
        """CHECK_CLIENT_CONFIG(46)：让 broker 校验一份订阅表达式（对应 Java
        ``MQClientAPIImpl#checkClientInBroker:3256``）。

        请求头是 null、body 是 ``CheckClientRequestBody`` 的 JSON；broker 非 SUCCESS 时
        Java 用 **响应码** 抛 MQClientException（``SUBSCRIPTION_PARSE_FAILED=23``、
        ``SYSTEM_ERROR=1``「broker 没开 enablePropertyFilter」都走这里）。
        Java 的 ``brokerVIPChannel(vipChannelEnabled=false)`` 是恒等变换，这里不实现。
        """
        request = RemotingCommand.create_request_command(RequestCode.CHECK_CLIENT_CONFIG, None)
        body = CheckClientRequestBody()
        body.client_id = client_id
        body.group = consumer_group
        body.subscription_data = subscription_data
        request.body = body.encode()
        response = self._invoke_sync(broker_addr, request, timeout_millis)
        if response is None:
            raise MQClientException("checkClientConfig got no response from %s" % broker_addr)
        if response.code != ResponseCode.SUCCESS:
            raise MQClientException(response.remark or "", response.code)

    # ---------------- 管理类 API ----------------
    def get_broker_cluster_info(self, timeout_millis: int = 10000) -> ClusterInfo:
        request = RemotingCommand.create_request_command(RequestCode.GET_BROKER_CLUSTER_INFO, None)
        for ns_addr in self.name_server_addrs:
            try:
                response = self._invoke_sync(ns_addr, request, timeout_millis)
                if response.code == ResponseCode.SUCCESS and response.body:
                    return ClusterInfo.decode(response.body)
            except Exception:  # noqa: BLE001
                continue
        raise MQClientException("Failed to get broker cluster info from name server")

    def get_all_topic_list_from_name_server(self, timeout_millis: int = 10000) -> TopicList:
        request = RemotingCommand.create_request_command(RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, None)
        for ns_addr in self.name_server_addrs:
            try:
                response = self._invoke_sync(ns_addr, request, timeout_millis)
                if response.code == ResponseCode.SUCCESS and response.body:
                    return TopicList.decode(response.body)
            except Exception:  # noqa: BLE001
                continue
        raise MQClientException("Failed to get all topic list from name server")

    def create_topic_in_broker(self, broker_addr: str, default_topic: str, topic: str,
                               read_queue_nums: int = 4, write_queue_nums: int = 4,
                               perm: int = 6, topic_sys_flag: int = 0,
                               topic_filter_type: str = TopicFilterType.SINGLE_TAG,
                               order: bool = False, attributes: Optional[str] = None,
                               timeout_millis: int = 5000, retry_times: int = 5) -> None:
        """对应 Java MQClientAPIImpl.createTopic。

        ⚠ 必须下发 topicFilterType：broker 的 CreateTopicRequestHeader.checkFields()
        会把它转成枚举，为空直接报 "topicFilterType = [null] value invalid"。
        Java 的 MQAdminImpl.createTopic 对每个 broker 还会重试 5 次。
        """
        header = CreateTopicRequestHeader()
        header.topic = topic
        header.default_topic = default_topic
        header.read_queue_nums = read_queue_nums
        header.write_queue_nums = write_queue_nums
        header.perm = perm
        header.topic_filter_type = topic_filter_type
        header.topic_sys_flag = topic_sys_flag
        header.order = order
        # Java: AttributeParser.parseToString(map) —— 空 map 输出 ""，不是 null
        header.attributes = attributes if attributes is not None else ""
        header.force = False
        request = RemotingCommand.create_request_command(RequestCode.UPDATE_AND_CREATE_TOPIC, header)

        last_exc: Optional[Exception] = None
        for attempt in range(max(1, retry_times)):
            try:
                response = self._invoke_sync(broker_addr, request, timeout_millis)
                self._check_response(response)
                return
            except MQBrokerException:
                raise
            except Exception as e:  # noqa: BLE001
                last_exc = e
                if attempt == retry_times - 1:
                    raise
        if last_exc is not None:
            raise last_exc

    def create_topic_in_route(self, topic: str, read_queue_nums: int = 4, write_queue_nums: int = 4,
                              perm: int = 6, topic_sys_flag: int = 0,
                              attributes: Optional[str] = None,
                              timeout_millis: int = 5000) -> None:
        """对应 Java MQAdminImpl.createTopic：只对默认 topic 路由里的 **master** 下发。"""
        route = self.get_topic_route_data(MixAll.DEFAULT_TOPIC)
        if route is None:
            raise MQClientException("No route info of default topic %s" % MixAll.DEFAULT_TOPIC)
        created_at_least_once = False
        last_exc: Optional[Exception] = None
        for broker_data in route.get_broker_datas():
            addr = broker_data.select_broker_addr()
            if not addr:
                continue
            try:
                self.create_topic_in_broker(addr, MixAll.DEFAULT_TOPIC, topic,
                                            read_queue_nums, write_queue_nums, perm,
                                            topic_sys_flag, attributes=attributes,
                                            timeout_millis=timeout_millis)
                created_at_least_once = True
            except Exception as e:  # noqa: BLE001
                last_exc = e
        if not created_at_least_once and last_exc is not None:
            raise MQClientException("create new topic failed", cause=last_exc)

    def delete_topic_in_broker(self, broker_addr: str, topic: str, timeout_millis: int = 5000) -> None:
        request = RemotingCommand.create_request_command(RequestCode.DELETE_TOPIC_IN_BROKER, None)
        request.ext_fields["topic"] = topic
        response = self._invoke_sync(broker_addr, request, timeout_millis)
        self._check_response(response)

    def delete_topic_in_namesrv(self, topic: str, timeout_millis: int = 5000) -> None:
        request = RemotingCommand.create_request_command(RequestCode.DELETE_TOPIC_IN_NAMESRV, None)
        request.ext_fields["topic"] = topic
        for ns_addr in self.name_server_addrs:
            try:
                response = self._invoke_sync(ns_addr, request, timeout_millis)
                self._check_response(response)
                return
            except Exception:  # noqa: BLE001
                continue
        raise MQClientException("Failed to delete topic %s in name server" % topic)

    def get_consumer_list_by_group(self, consumer_group: str, timeout_millis: int = 5000,
                                   addr: Optional[str] = None) -> GetConsumerListByGroupResponseBody:
        from ..remoting.protocol.headers import GetConsumerListByGroupRequestHeader
        if addr is None:
            raise MQClientException("broker addr required for get consumer list")
        header = GetConsumerListByGroupRequestHeader()
        header.consumer_group = consumer_group
        request = RemotingCommand.create_request_command(RequestCode.GET_CONSUMER_LIST_BY_GROUP, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        self._check_response(response)
        if response.body:
            return GetConsumerListByGroupResponseBody.decode(response.body)
        return GetConsumerListByGroupResponseBody()

    # ---------------- 工具 ----------------
    def _broker_addr(self, mq: MessageQueue) -> str:
        route = self.get_topic_route_data(mq.topic)
        if route is None:
            raise MQClientException("No route info of this topic: %s" % mq.topic)
        addr = MQClientInstance.find_broker_addr_in_route(route, mq.broker_name)
        if addr is None:
            raise MQClientException("Broker %s not found in route of topic %s" % (mq.broker_name, mq.topic))
        return addr

    def broker_addr_of(self, broker_name: str) -> Optional[str]:
        for route in self.topic_route_table.values():
            addr = MQClientInstance.find_broker_addr_in_route(route, broker_name)
            if addr:
                return addr
        return None

    def get_route_of_all_brokers(self) -> List[str]:
        addrs = []
        for route in self.topic_route_table.values():
            for broker_data in route.get_broker_datas():
                a = broker_data.select_broker_addr()
                if a and a not in addrs:
                    addrs.append(a)
        return addrs

    def get_all_broker_addrs(self) -> List[str]:
        """路由里出现过的**每一台** broker 地址（主 + 从），不去重到「一个 brokerName 一台」。

        `get_route_of_all_brokers` 走的是 `select_broker_addr()`（主优先，没主才随机），
        心跳、拉取这类「问到一台就行」的调用用它。注销(35) 不行：Java
        `MQClientInstance#unregisterClient`:1158-1182 遍历的是 `brokerAddrTable` 的
        **每个 brokerId**，从节点也在里面 —— broker 的 `ProducerManager` /
        `ConsumerManager` 是**每台 broker 各自一份**状态，漏掉从节点就等于那台的注册要等
        通道扫描（默认 ~120s）才回收。
        """
        addrs = []
        for route in self.topic_route_table.values():
            for broker_data in route.get_broker_datas():
                for a in broker_data.broker_addrs.values():
                    if a and a not in addrs:
                        addrs.append(a)
        return addrs

    def get_consumer_id_list_by_group(self, topic: str, consumer_group: str,
                                      timeout_millis: int = 5000) -> Optional[List[str]]:
        """查询消费组内所有 clientId（对应 Java MQClientInstance.findConsumerIdList）。

        Java 取该 topic 路由里的 master broker 发 GET_CONSUMER_LIST_BY_GROUP(38)：
        所有客户端都会向集群内每台 broker 心跳注册，故任取一台即持有**完整**消费者列表。
        查不到（无路由 / 非 SUCCESS / 异常）返回 None；调用方按 Java 语义「保留当前分配」，
        不要回退成"自己独占全部队列"（那会让多实例互相重复消费）。
        """
        try:
            addr = self._broker_addr_for_topic(topic)
        except MQClientException as e:
            logger.debug("get_consumer_id_list_by_group: no broker for topic %s: %s", topic, e)
            return None
        header = GetConsumerListByGroupRequestHeader()
        header.consumer_group = consumer_group
        request = RemotingCommand.create_request_command(RequestCode.GET_CONSUMER_LIST_BY_GROUP,
                                                         header)
        try:
            response = self._invoke_sync(addr, request, timeout_millis)
        except Exception as e:  # noqa: BLE001
            logger.debug("get_consumer_id_list_by_group failed, %s %s: %s",
                         addr, consumer_group, e)
            return None
        if response.code != ResponseCode.SUCCESS or response.body is None:
            return None
        try:
            body = GetConsumerListByGroupResponseBody.decode(response.body)
        except Exception as e:  # noqa: BLE001
            logger.debug("get_consumer_id_list_by_group decode failed: %s", e)
            return None
        return list(body.consumer_id_list)

    def unregister_client_all_brokers(self, client_id: str, producer_group: str,
                                      consumer_group: str, timeout_millis: int = 3000) -> None:
        """向所有已知 broker 注销本 clientId（对应 Java MQClientInstance.unregisterClient）。

        Java 在生产者/消费者 shutdown 时会逐台 broker 发 UNREGISTER_CLIENT(35)。
        不发的话 broker 端 Producer/ConsumerManager 只能等心跳超时（默认 ~120s）清理，
        期间事务回查、消费者变更通知仍可能发往已退出的实例。
        单台失败只记 debug —— shutdown 路径不应因网络抖动抛异常。
        """
        # 主 + 从都要发：Java :1159-1166 遍历的是 brokerAddrTable 的**每个 brokerId**，
        # 而 Producer/ConsumerManager 是每台 broker 各自一份状态，漏掉从节点就等于那台的
        # 注册要等通道扫描才回收。
        for addr in self.get_all_broker_addrs():
            try:
                self.unregister_client(addr, client_id, producer_group, consumer_group,
                                       timeout_millis)
            except Exception as e:  # noqa: BLE001
                logger.debug("unregister_client failed, addr=%s: %s", addr, e)


__all__ = ["MQClientInstance", "TopicPublishInfo"]