<?php

declare(strict_types=1);

namespace RocketMQ\Client;

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
        public int $intervalMillis = 20000,
    ) {
    }

    /**
     * 下一轮到点时刻（毫秒）。0 = 立刻到期（新注册 / rebalanceImmediately 唤醒时置零）。
     * 由 {@see MQClientInstance::runPopAssignmentRound()} 在每轮开跑时重排。
     */
    public int $nextRunMillis = 0;
}
