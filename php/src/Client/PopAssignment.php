<?php

declare(strict_types=1);

namespace RocketMQ\Client;

/**
 * POP 的 broker 侧队列分配轮询方（对应 Java RebalanceImpl#getRebalanceResultFromBroker:345
 * 里"一个 topic 一次 QUERY_ASSIGNMENT(400)"的那条腿）。
 *
 * 为什么要显式注册：Java 的这条腿挂在 RebalanceService 的 20s 调度线程上，
 * PHP 端**没有线程**（单线程 tick() 驱动模型，见 PushConsumer 类头注释），所以
 * 由 {@see MQClientInstance::tick()} 的阶段⑦按到期判定推进 —— 注册方式照抄
 * {@see MQClientInstance::registerFaultStrategy()}（探活）：注册方给周期与回调，
 * 实例只在到点时同步跑一轮。
 *
 * 注册方 = ``popMode && !clientRebalance`` 的 push 消费者（Java RebalancePushImpl
 * #clientRebalance:138-141 取反）；两个 closure 都由消费者提供：
 *
 * - {@see $topicsFn} 每轮**现查**当前订阅的 topic 集合（订阅会运行中变化：subscribe /
 *   unsubscribe / 集群模式自动加的重试 topic）；
 * - {@see $applyFn} 拿到该 topic 的指配集后并入本地分配、驱动 POP 拉取面更新。
 *
 * "无效结果"（RPC 失败、broker 没给指配集、查不到 broker 地址）**不会**走到
 * {@see $applyFn} —— 实例直接跳过，上一次分配原样保留（Java 的 `return false`）。
 */
final class PopAssignmentPoller
{
    /**
     * @param \Closure(): list<string> $topicsFn
     * @param \Closure(string, list<array{mq: \RocketMQ\Common\MessageQueue, mode: \RocketMQ\Remoting\Protocol\MessageRequestMode}>): void $applyFn
     */
    public function __construct(
        /** 注册表键：一个实例可以挂多个消费者，也允许同一消费者重复注册（覆盖旧的）。 */
        public string $key,
        public string $consumerGroup,
        public string $clientId,
        /** 本端分配策略名（AVG / AVG_BY_CIRCLE ...）；broker 只认这两个，其余回无效结果。 */
        public string $strategyName,
        /** 'CLUSTERING' | 'BROADCASTING'，进 body。 */
        public string $messageModel,
        public \Closure $topicsFn,
        public \Closure $applyFn,
        /** 轮询周期；默认 20s = Java 的 rebalance 调度周期。 */
        public int $intervalMillis = 20000,
    ) {
    }

    /**
     * 下一轮到点时刻（毫秒）。0 = 立刻到期（新注册 / rebalanceImmediately 唤醒时置零）。
     * 由 {@see MQClientInstance::runPopAssignmentRound()} 在每轮开跑时重排。
     */
    public int $nextRunMillis = 0;
}
