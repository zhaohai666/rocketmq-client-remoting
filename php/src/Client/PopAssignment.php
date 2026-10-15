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
        /** Registry key: one instance can host multiple consumers, and the same consumer may register repeatedly (overwriting the old one). */
        public string $key,
        public string $consumerGroup,
        public string $clientId,
        /** Local allocation strategy name (AVG / AVG_BY_CIRCLE ...); the broker only recognizes these two, anything else returns an invalid result. */
        public string $strategyName,
        /** 'CLUSTERING' | 'BROADCASTING', goes into the body. */
        public string $messageModel,
        public \Closure $topicsFn,
        public \Closure $applyFn,
        public int $intervalMillis = 20000,
    ) {
    }

    /**
     * The next round's due time (milliseconds). 0 = due immediately (set to zero on new
     * registration / rebalanceImmediately wakeup). Rearranged by
     * {@see MQClientInstance::runPopAssignmentRound()} at the start of each round.
     */
    public int $nextRunMillis = 0;
}
