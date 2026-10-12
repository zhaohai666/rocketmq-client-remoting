"""静态主题（单元化主题）消费者侧路由支持，移植自 Java 客户端：

- ``org.apache.rocketmq.remoting.rpc.ClientMetadata
  .topicRouteData2EndpointsForStaticTopic``
- ``org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingUtils
  .getMockBrokerName``
- ``org.apache.rocketmq.client.impl.factory.MQClientInstance
  .getBrokerNameFromMessageQueue``（见 ``client/mq_client.py`` 同名方法）

wire 事实（不得"简化"）：

- 路由解码侧把 ``topicQueueMappingByBroker`` 保持为不透明的
  ``Dict[brokerName, dict]``；**类型化视图在这里解析**，不动协议层透传。
- ``TopicQueueMappingInfo.scope`` 缺省为 ``MixAll.METADATA_SCOPE_GLOBAL``
  （``__global__``）；global scope 的 mock broker 名会剥掉开头两个下划线
  （``__syslo__global__``）。
- Java 的 ``maxTotalNumOfEpoch`` 读了但从不赋值（恒 -1），所以
  ``epoch >= maxTotalNumOfEpoch`` 守卫恒真，``maxTotalNums`` 就是该 scope 的
  ``totalQueues`` 最大值 —— 这个 quirk 照抄。
- 同 epoch 的后到条目**覆盖**先到条目（Java ``oldEpoch <= epoch``）。
"""

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
    """Java ``MixAll::isLogicalQueueMockBrokerName``（前缀判断）。"""
    return broker_name.startswith(LOGICAL_QUEUE_MOCK_BROKER_PREFIX)


def get_mock_broker_name(scope: str) -> str:
    """``getMockBrokerName``：``__global__``（scope 本身）剥掉头两个字符，
    其余 scope 原样加前缀。"""
    if scope == METADATA_SCOPE_GLOBAL:
        return LOGICAL_QUEUE_MOCK_BROKER_PREFIX + scope[2:]
    return LOGICAL_QUEUE_MOCK_BROKER_PREFIX + scope


class TopicQueueMappingInfo:
    """``topicQueueMappingByBroker[brokerName]`` 单条目的类型化视图
    （Java ``TopicQueueMappingInfo``）。"""

    __slots__ = ("scope", "total_queues", "bname", "epoch", "curr_id_map")

    def __init__(self) -> None:
        self.scope: str = METADATA_SCOPE_GLOBAL
        self.total_queues: int = 0
        self.bname: str = ""
        self.epoch: int = 0
        # globalId -> 物理 queueId。物理 id 不参与端点推导，只保留给排查用。
        self.curr_id_map: Dict[int, int] = {}


def parse_topic_queue_mapping_info(raw) -> Optional[TopicQueueMappingInfo]:
    """从路由解码侧保留下来的原始 json 对象里解析类型化视图。
    畸形条目丢弃（返回 ``None``）—— Java 遇到会 NPE；能抢救的部分映射
    好过整个崩溃。"""
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

    # 按 scope 分组（Java 里没有 scope 的条目不参与，同此）。
    infos_by_scope: Dict[str, Dict[str, TopicQueueMappingInfo]] = {}
    for broker, raw in route.topic_queue_mapping_by_broker.items():
        info = parse_topic_queue_mapping_info(raw)
        if info is None:
            continue
        infos_by_scope.setdefault(info.scope, {})[broker] = info

    endpoints: Dict[MessageQueue, str] = {}
    for scope, infos in infos_by_scope.items():
        # 先按 broker 名排序，再做稳定的 epoch 降序排序。Java 对
        # ConcurrentHashMap 的 entrySet 做 epoch 降序排序，同 epoch 的覆盖顺序本就
        # 未定义；钉死 broker 升序让结果可复现。
        ordered = sorted(infos.items(), key=lambda kv: kv[0])
        ordered.sort(key=lambda kv: kv[1].epoch, reverse=True)

        max_total_nums = 0
        # Java 从不更新 max_total_num_of_epoch —— 守卫恒真，照抄。
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
