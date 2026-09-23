# 拉取前流控判定（Java ProcessQueue 五个阈值）单测 —— 不需要集群。
#
# 为什么必须离线锁死：五条闸门里只有**条数**那条会在"消息小、拉得快"的场景先命中，
# 其余四条判错方向是**静默**的：
#   - 队列级字节闸门失效 ⇒ 大消息把已拉未消费的堆撑爆（真机表现为 OOM/RT 飙升，无异常）；
#   - 位点跨度闸门失效 ⇒ 队首一条一直消费失败、后面无限堆着，位点跨度失控；
#   - topic 级闸门失效 ⇒ 同 topic 多队列各自为政，单队列都"没超"但实例总量爆掉。
# 单位也只有一个坑：size 阈值是 **MiB** 不是字节；跨度是**严格大于**而条数/字节是 **>=**。
#
# 本文件锁的是**参考实现**本身（Python 是四语言的对齐基准），C++/Rust/.NET 的
# test_flow_control 与这里逐条同构。真机侧"闸门命中之后消息一条都不丢"见
# ../verify_flow_control_live.py。
from rocketmq.client.consumer import DefaultMQPushConsumer
from rocketmq.common.message import MessageExt, MessageQueue

TOPIC = "FlowControlPyUnitTopic"
OTHER_TOPIC = "FlowControlPyUnitOther"
BROKER = "broker-a"
MIB = 1024 * 1024


def _queue(topic=TOPIC, queue_id=0):
    return MessageQueue(topic=topic, broker_name=BROKER, queue_id=queue_id)


def _sized(topic, store_size, queue_offset):
    m = MessageExt(topic=topic, body=b"x")
    m.broker_name = BROKER
    m.store_size = store_size
    m.queue_offset = queue_offset
    return m


def _n_sized(topic, store_size, n):
    return [_sized(topic, store_size, i) for i in range(n)]


def _consumer(group):
    """未 Start 的消费者：_pending/_mq_map 由测试自己预置，不碰网络。"""
    return DefaultMQPushConsumer(group)


def _gates_off(c):
    """关掉除待测阈值以外的所有闸门：先命中的那条会掩盖被测分支。"""
    c.pull_threshold_for_queue = 2 ** 31 - 1
    c.pull_threshold_size_for_queue = 0
    c.consume_concurrently_max_span = 0
    c.pull_threshold_for_topic = -1
    c.pull_threshold_size_for_topic = -1


def _stage(c, mq, msgs):
    """预置某队列的缓冲 + 登记为已分配（topic 级阈值靠 _mq_map 聚合兄弟队列）。"""
    key = c._mq_key(mq)
    c._mq_map[key] = mq
    c._pending[key] = list(msgs)
    return key


def _hit(c, mq, key=None):
    return c._flow_control_hit(mq, key or c._mq_key(mq))


def test_defaults_only_the_count_gate_is_live():
    c = _consumer("G_fc_defaults")
    mq = _queue()
    # 默认 1000 条 / 100MiB / 跨度 2000，topic 级关闭：小缓冲一律不命中
    _stage(c, mq, _n_sized(TOPIC, 100, 1))
    assert not _hit(c, mq)
    assert c._flow_control_triggered == 0
    # 缓冲为空（还没拉过任何消息）同样不命中，且跨度不能被算成负数
    _stage(c, mq, [])
    assert not _hit(c, mq)


def test_count_gate_fires_at_threshold_and_treats_zero_as_one():
    c = _consumer("G_fc_count")
    mq = _queue()
    _gates_off(c)
    c.pull_threshold_for_queue = 3
    _stage(c, mq, _n_sized(TOPIC, 100, 2))
    assert not _hit(c, mq)
    _stage(c, mq, _n_sized(TOPIC, 100, 3))
    assert _hit(c, mq)
    assert c._flow_control_triggered == 1
    # Java 的 Math.max(1, n) 守卫：配 0 不是"全放行"，而是"1 条就停"
    c.pull_threshold_for_queue = 0
    _stage(c, mq, _n_sized(TOPIC, 100, 1))
    assert _hit(c, mq), "pull_threshold_for_queue=0 要按 1 条算（Java 的 max(1,n) 守卫）"


def test_size_gate_is_in_mib_and_disabled_by_zero():
    c = _consumer("G_fc_size")
    mq = _queue()
    _gates_off(c)
    c.pull_threshold_size_for_queue = 1          # 1 MiB
    _stage(c, mq, _n_sized(TOPIC, 300, 3))       # 900 B
    assert not _hit(c, mq)
    # 单位是 MiB 而不是字节：正好 1 MiB 就算命中（>=）
    _stage(c, mq, [_sized(TOPIC, MIB, 0)])
    assert _hit(c, mq), "正好 1MiB 要命中（>=，不是 >）"
    _stage(c, mq, _n_sized(TOPIC, 2 * MIB, 4))
    assert _hit(c, mq)
    # 0 = 关闭这条闸门，再大的缓冲也不管
    c.pull_threshold_size_for_queue = 0
    assert not _hit(c, mq)


def test_span_gate_is_strictly_greater_and_measures_real_span():
    c = _consumer("G_fc_span")
    mq = _queue()
    _gates_off(c)
    c.consume_concurrently_max_span = 10
    _stage(c, mq, [_sized(TOPIC, 1, 0), _sized(TOPIC, 1, 100)])
    assert _hit(c, mq)
    # **严格大于**：跨度正好等于阈值不算（Java 同）
    c.consume_concurrently_max_span = 100
    assert not _hit(c, mq), "跨度 100 不严格大于 100，不该命中"
    # 乱序缓冲也要量出真实跨度（min/max 而不是首尾差）
    c.consume_concurrently_max_span = 10
    _stage(c, mq, [_sized(TOPIC, 1, 50), _sized(TOPIC, 1, 5), _sized(TOPIC, 1, 7)])
    assert _hit(c, mq), "乱序缓冲要按 max-min 量跨度，不是首尾差"
    c.consume_concurrently_max_span = 0
    assert not _hit(c, mq)


def test_topic_count_gate_aggregates_siblings_but_not_other_topics():
    c = _consumer("G_fc_topic_count")
    q0, q1 = _queue(queue_id=0), _queue(queue_id=1)
    other = _queue(topic=OTHER_TOPIC)
    _gates_off(c)
    c.pull_threshold_for_topic = 2
    # 单队列 1 条：topic 级也只看到 1 条
    _stage(c, q0, _n_sized(TOPIC, 1, 1))
    assert not _hit(c, q0)
    # 同 topic 的兄弟队列各 1 条 ⇒ 累计 2 条，两条队列都必须停
    k1 = _stage(c, q1, _n_sized(TOPIC, 1, 1))
    assert _hit(c, q0)
    assert _hit(c, q1, k1)
    # 别的 topic 不许掺进来：把本 topic 降到 1 条，另一 topic 堆 5 条
    _stage(c, q1, [])
    _stage(c, other, _n_sized(OTHER_TOPIC, 1, 5))
    assert not _hit(c, q0), "别的 topic 的缓冲不能算进本 topic 的累计"


def test_topic_size_gate_has_its_own_switch_and_runs_last():
    c = _consumer("G_fc_topic_size")
    q0, q1 = _queue(queue_id=0), _queue(queue_id=1)
    _gates_off(c)
    # 队列级字节闸门**关掉**（0），只留 topic 级：误用队列级开关当闸门会让这条静默失效
    c.pull_threshold_size_for_topic = 1
    _stage(c, q0, [_sized(TOPIC, MIB // 2, 0)])
    assert not _hit(c, q0)
    _stage(c, q1, [_sized(TOPIC, 3 * MIB // 2, 0)])
    assert _hit(c, q0), "队列级 size 闸门关掉时主题级 size 仍要生效"
    assert _hit(c, q1)
    # 队列级那道还开着时先命中队列级（判定顺序：条数 → 字节 → 跨度 → topic 条数 → topic 字节）
    c.pull_threshold_size_for_queue = 1
    _stage(c, q0, [_sized(TOPIC, 2 * MIB, 0)])
    before = c._flow_control_triggered
    assert _hit(c, q0)
    # 一次判定只记一格，即使多条闸门同时命中
    assert c._flow_control_triggered == before + 1


def test_one_hit_counts_one_tick_even_when_every_gate_matches():
    """计数器是"因流控停了几次"的观测面：五条闸门同时命中也只能加一格。"""
    c = _consumer("G_fc_counter")
    q0, q1 = _queue(queue_id=0), _queue(queue_id=1)
    _gates_off(c)
    c.pull_threshold_for_queue = 1
    c.pull_threshold_size_for_queue = 1
    c.consume_concurrently_max_span = 1
    c.pull_threshold_for_topic = 1
    c.pull_threshold_size_for_topic = 1
    # 2 条 2MiB、位点跨到 100 的缓冲：五条闸门同时命中
    _stage(c, q0, [_sized(TOPIC, 2 * MIB, 0), _sized(TOPIC, 2 * MIB, 100)])
    _stage(c, q1, [_sized(TOPIC, 2 * MIB, 0)])
    assert _hit(c, q0)
    assert c._flow_control_triggered == 1
