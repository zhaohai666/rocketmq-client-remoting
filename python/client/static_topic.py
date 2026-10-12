
from __future__ import annotations

from typing import Dict, Optional

from common.message import MessageQueue
from remoting.protocol.route import TopicRouteData

# MixAll.METADATA_SCOPE_GLOBAL / LOGICAL_QUEUE_MOCK_BROKER_PREFIX /
# LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST。
METADATA_SCOPE_GLOBAL = "__global__"
LOGICAL_QUEUE_MOCK_BROKER_PREFIX = "__syslo__"
LOGICAL_QUEUE_MOCK_NOT_EXIST = "__syslo__none__"


def is_logical_queue_mock_broker_name(broker_name: str) -> bool:
    pass
    return broker_name.startswith(LOGICAL_QUEUE_MOCK_BROKER_PREFIX)


def get_mock_broker_name(scope: str) -> str:
    """``getMockBrokerName``：``__global__``（scope 本身）剥掉头两个字符，
    其余 scope 原样加前缀。"""
    if scope == METADATA_SCOPE_GLOBAL:
        return LOGICAL_QUEUE_MOCK_BROKER_PREFIX + scope[2:]
    return LOGICAL_QUEUE_MOCK_BROKER_PREFIX + scope


class TopicQueueMappingInfo:
    pass

    __slots__ = ("scope", "total_queues", "bname", "epoch", "curr_id_map")

    def __init__(self) -> None:
        self.scope: str = METADATA_SCOPE_GLOBAL
        self.total_queues: int = 0
        self.bname: str = ""
        self.epoch: int = 0
        # globalId -> 物理 queueId。物理 id 不参与端点推导，只保留给排查用。
        self.curr_id_map: Dict[int, int] = {}


def parse_topic_queue_mapping_info(raw) -> Optional[TopicQueueMappingInfo]:
    pass
    if not isinstance(raw, dict):
        return None
    info = TopicQueueMappingInfo()
    scope = raw.get("scope")
    if isinstance(scope, str):
        info.scope = scope
    total_queues = raw.get("totalQueues")
    if isinstance(total_queues, (int, float)):
        info.total_queues = int(total_queues)
    bname = raw.get("bname")
    if isinstance(bname, str):
        info.bname = bname
    epoch = raw.get("epoch")
    if isinstance(epoch, (int, float)):
        info.epoch = int(epoch)
    curr_id_map = raw.get("currIdMap")
    if isinstance(curr_id_map, dict):
        for k, v in curr_id_map.items():
            try:
                gid = int(k)
                phys = int(v)
            except (TypeError, ValueError):
                continue
            info.curr_id_map[gid] = phys
    return info


def topic_route_data_2_endpoints_for_static_topic(
    topic: str, route: Optional[TopicRouteData]
) -> Optional[Dict[MessageQueue, str]]:
    """``ClientMetadata.topicRouteData2EndpointsForStaticTopic``：构建逻辑队列集
    ``MessageQueue(topic, mockBrokerName(scope), globalId) -> 承载它的真实 broker 名``
    （total 范围内没有承载者的逻辑 id 指向 ``__syslo__none__``）。
    空映射（非静态主题）返回 ``None``。"""
    if route is None or not route.topic_queue_mapping_by_broker:
        return None

    infos_by_scope: Dict[str, Dict[str, TopicQueueMappingInfo]] = {}
    for broker, raw in route.topic_queue_mapping_by_broker.items():
        info = parse_topic_queue_mapping_info(raw)
        if info is None:
            continue
        infos_by_scope.setdefault(info.scope, {})[broker] = info

    endpoints: Dict[MessageQueue, str] = {}
    for scope, infos in infos_by_scope.items():
        ordered = sorted(infos.items(), key=lambda kv: kv[0])
        ordered.sort(key=lambda kv: kv[1].epoch, reverse=True)

        max_total_nums = 0
        max_total_num_of_epoch = -1
        mock_name = get_mock_broker_name(scope)
        mq_end_points: Dict[MessageQueue, TopicQueueMappingInfo] = {}
        for _, info in ordered:
            if info.epoch >= max_total_num_of_epoch and info.total_queues > max_total_nums:
                max_total_nums = info.total_queues
            # 只有 global id（键）形状决定端点集；物理 id 是 broker 内部的。
            for global_id in info.curr_id_map:
                mq = MessageQueue(topic, mock_name, global_id)
                old = mq_end_points.get(mq)
                if old is None or old.epoch <= info.epoch:
                    mq_end_points[mq] = info

        # 补齐静态逻辑队列：total 之下的每个 id 都存在；没有承载者的指向
        # NOT_EXIST 哨兵。
        for i in range(max_total_nums):
            mq = MessageQueue(topic, mock_name, i)
            holder = mq_end_points.get(mq)
            endpoints[mq] = holder.bname if holder is not None else LOGICAL_QUEUE_MOCK_NOT_EXIST

    if not endpoints:
        return None
    return endpoints
