# -*- coding: utf-8 -*-
"""MQClientInstance：RocketMQ 客户端核心编排（对应 org.apache.rocketmq.client.impl.factory.MQClientInstance
与 MQClientAPIImpl 的核心调用面，Python 单机版）。

职责：NameServer 地址管理、Topic 路由获取与缓存、Broker 地址解析、
消息发送（SEND_MESSAGE / SEND_MESSAGE_V2）、拉取（PULL_MESSAGE）、
offset 查询/更新、心跳、管理类 API（创建/删除 Topic、集群信息等）。
"""
from __future__ import annotations

import threading
import time
from typing import Dict, List, Optional

from ..common.message import Message, MessageBatch, MessageExt, MessageQueue
from ..common.message_const import MessageConst
from ..common.message_decoder import (decode_messages, decompress_body,
                                      message_properties_2_string,
                                      string_2_message_properties)
from ..common.mix_all import MixAll
from ..common.sysflag import MessageSysFlag
from ..common.topic_config import TopicFilterType
from ..logging import get_logger
from ..remoting.client import RemotingClient
from ..remoting.protocol.body import (ClusterInfo, GetConsumerListByGroupResponseBody,
                                      TopicList)
from ..remoting.protocol.codes import RequestCode, ResponseCode, SerializeType
from ..remoting.protocol.headers import (CreateTopicRequestHeader,
                                         GetConsumerListByGroupRequestHeader,
                                         GetMaxOffsetRequestHeader,
                                         GetMaxOffsetResponseHeader, GetMinOffsetRequestHeader,
                                         GetMinOffsetResponseHeader, PullMessageRequestHeader,
                                         PullMessageResponseHeader, QueryConsumerOffsetRequestHeader,
                                         QueryConsumerOffsetResponseHeader, QueryMessageRequestHeader,
                                         QueryMessageResponseHeader, ReplyMessageRequestHeader,
                                         SearchOffsetRequestHeader,
                                         SearchOffsetResponseHeader, SendMessageRequestHeader,
                                         SendMessageRequestHeaderV2, SendMessageResponseHeader,
                                         UpdateConsumerOffsetRequestHeader)
from ..remoting.protocol.heartbeat import HeartbeatData
from ..remoting.protocol.remoting_command import RemotingCommand
from ..remoting.protocol.route import TopicRouteData
from .exception import MQBrokerException, MQClientException
from .request_reply import REQUEST_FUTURE_HOLDER, is_reply_message
from .send_result import SendResult, SendStatus

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
                 connect_timeout_millis: int = 3000, invoke_timeout_millis: int = 15000):
        self.client_id = client_id
        self.name_server_addrs: List[str] = list(name_server_addrs)
        self.remoting_client = RemotingClient(connect_timeout_millis, invoke_timeout_millis)
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
        MQClientInstance.INSTANCE_MAP[client_id] = self

    # ---------------- 生命周期 ----------------
    def start(self) -> None:
        self._started = True
        if self._route_refresh_thread is None:
            self._route_refresh_stop.clear()
            t = threading.Thread(target=self._route_refresh_loop, daemon=True,
                                 name="rmq-route-refresh-%s" % self.client_id)
            t.start()
            self._route_refresh_thread = t

    def shutdown(self) -> None:
        self._started = False
        self._route_refresh_stop.set()
        self.remoting_client.shutdown()

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
            raise MQClientException("name server address list is empty")

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

    # ---------------- 消息发送 ----------------
    def send_message(self, producer_group: str, msg: Message, mq: MessageQueue,
                     timeout_millis: int = 3000, sys_flag: int = 0) -> SendResult:
        addr = self.find_broker_addr_in_route(self.get_topic_route_data(mq.topic), mq.broker_name) if self.get_topic_route_data(mq.topic) else None
        if addr is None:
            route = self.get_topic_route_data(mq.topic)
            if route is None:
                raise MQClientException("No route info of this topic: %s" % mq.topic)
            addr = MQClientInstance.find_broker_addr_in_route(route, mq.broker_name)
            if addr is None:
                raise MQClientException("Broker %s not found in route of topic %s" % (mq.broker_name, mq.topic))
        request = self._build_send_request(producer_group, msg, mq, timeout_millis, sys_flag)
        response = self._invoke_sync(addr, request, timeout_millis)
        return self._parse_send_response(response, msg, mq)

    def send_message_to_addr(self, producer_group: str, msg: Message, mq: MessageQueue,
                             addr: str, timeout_millis: int = 3000,
                             sys_flag: int = 0) -> SendResult:
        request = self._build_send_request(producer_group, msg, mq, timeout_millis, sys_flag)
        response = self._invoke_sync(addr, request, timeout_millis)
        return self._parse_send_response(response, msg, mq)

    def send_message_oneway(self, producer_group: str, msg: Message, mq: MessageQueue,
                            addr: str, timeout_millis: int = 3000,
                            sys_flag: int = 0) -> None:
        request = self._build_send_request(producer_group, msg, mq, timeout_millis, sys_flag)
        request.mark_oneway_rpc()
        self.remoting_client.invoke_oneway(addr, request)

    def _build_send_request(self, producer_group: str, msg: Message, mq: MessageQueue,
                            timeout_millis: int = 3000, sys_flag: int = 0) -> RemotingCommand:
        """sys_flag 由 Producer 算好（压缩标志 + 压缩类型位），见
        DefaultMQProducer.try_to_compress_message。"""
        header = SendMessageRequestHeaderV2()
        header.producer_group = producer_group
        header.topic = msg.topic
        header.default_topic = MixAll.DEFAULT_TOPIC
        header.default_topic_queue_nums = MixAll.DEFAULT_TOPIC_QUEUE_NUMS
        header.queue_id = mq.queue_id
        header.sys_flag = sys_flag
        header.born_timestamp = int(time.time() * 1000)
        header.flag = msg.flag
        header.properties = message_properties_2_string(msg.properties)
        header.reconsume_times = 0
        header.unit_mode = False
        header.max_reconsume_times = 0
        header.batch = isinstance(msg, MessageBatch)
        # Request-Reply：MSG_TYPE == "reply" 的应答消息走 SEND_REPLY_MESSAGE_V2(325)，
        # 而不是普通的 SEND_MESSAGE_V2(314)。broker 只在 324/325 上注册了
        # ReplyMessageProcessor（它负责按 REPLY_TO_CLIENT 把应答推回请求方）。
        # 对齐 Java MQClientAPIImpl.sendMessage:550-558（sendSmartMsg 默认 true → V2）。
        code = (RequestCode.SEND_REPLY_MESSAGE_V2 if is_reply_message(msg)
                else RequestCode.SEND_MESSAGE_V2)
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
            return SendResult(
                send_status=status_map[response.code],
                msg_id=header.msg_id,
                message_queue=MessageQueue(mq.topic, mq.broker_name, header.queue_id if header.queue_id is not None else mq.queue_id),
                queue_offset=header.queue_offset or 0,
                transaction_id=header.transaction_id,
            )
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
    def query_consumer_offset(self, consumer_group: str, mq: MessageQueue,
                              timeout_millis: int = 5000, addr: Optional[str] = None,
                              set_zero_if_not_found: bool = True) -> Optional[int]:
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
                          consumer_group: str, timeout_millis: int = 5000) -> None:
        from ..remoting.protocol.headers import UnregisterClientRequestHeader
        header = UnregisterClientRequestHeader()
        header.client_id = client_id
        header.producer_group = producer_group
        header.consumer_group = consumer_group
        request = RemotingCommand.create_request_command(RequestCode.UNREGISTER_CLIENT, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        self._check_response(response)

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
                                      consumer_group: str, timeout_millis: int = 5000) -> None:
        """向所有已知 broker 注销本 clientId（对应 Java MQClientInstance.unregisterClient）。

        Java 在生产者/消费者 shutdown 时会逐台 broker 发 UNREGISTER_CLIENT(35)。
        不发的话 broker 端 Producer/ConsumerManager 只能等心跳超时（默认 ~120s）清理，
        期间事务回查、消费者变更通知仍可能发往已退出的实例。
        单台失败只记 debug —— shutdown 路径不应因网络抖动抛异常。
        """
        for addr in self.get_route_of_all_brokers():
            try:
                self.unregister_client(addr, client_id, producer_group, consumer_group,
                                       timeout_millis)
            except Exception as e:  # noqa: BLE001
                logger.debug("unregister_client failed, addr=%s: %s", addr, e)


__all__ = ["MQClientInstance", "TopicPublishInfo"]