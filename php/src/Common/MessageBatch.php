<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * Batch message: a group of Message objects concatenated into one body and sent in a single request.
 *
 * It has no serialization fields of its own; the body is produced by ``encode()``:
 * the concatenation result of ``MessageDecoder::encodeMessages($messages)`` (each entry in a 6-segment
 * lightweight format).
 */
final class MessageBatch extends Message
{
    /** @var Message[] */
    public array $messages = [];

    /** @param list<Message> $messages */
    public function __construct(array $messages = [])
    {
        parent::__construct();
        $this->messages = array_values($messages);
    }

    /** Generates the batch body: 6-segment lightweight format per entry, concatenated. */
    public function encode(): string
    {
        return MessageDecoder::encodeMessages($this->messages);
    }

    /** @return list<Message> */
    public function getMessages(): array
    {
        return $this->messages;
    }

    /**
     * Builds a batch message from a list and validates the constraints.
     *
     * Constraints: not empty; the same topic; the same waitStoreMsgOK; delayed messages are not allowed;
     * retry topics are not allowed. If any entry violates them the whole batch throws
     * InvalidArgumentException — it is not acceptable to just drop a few and send the rest.
     *
     * @param list<Message> $messages
     */
    public static function generateFromList(array $messages): self
    {
        if ($messages === []) {
            throw new \InvalidArgumentException('messages must not be null or empty');
        }

        $messageList = [];
        $first = null;
        foreach ($messages as $message) {
            $delayLevel = $message->getDelayTimeLevel();
            if ($delayLevel !== null && (int) $delayLevel > 0) {
                throw new \InvalidArgumentException('Delayed messages are not supported for batching');
            }
            if (str_starts_with($message->getTopic(), MixAll::RETRY_GROUP_TOPIC_PREFIX)) {
                throw new \InvalidArgumentException('Retry Group is not supported for batching');
            }
            if ($first === null) {
                $first = $message;
            } else {
                if ($first->getTopic() !== $message->getTopic()) {
                    throw new \InvalidArgumentException('The topic of the messages in one batch should be the same');
                }
                if ($first->getWaitStoreMsgOk() !== $message->getWaitStoreMsgOk()) {
                    throw new \InvalidArgumentException('The waitStoreMsgOK of the messages in one batch should be the same');
                }
            }
            $messageList[] = $message;
        }

        $batch = new self($messageList);
        $batch->setTopic($first->getTopic());
        // The batch-level WAIT follows the verdict for the first message, and **an absent property means
        // true**. It used to be written as ``first.getWaitStoreMsgOk() == "true"`` — an absent value
        // (null) was then judged as false, so a batch made of ordinary messages (whose constructor does
        // not set WAIT) would be sent with ``WAIT=false``: the broker would answer SEND_OK without waiting
        // for the flush to disk, silently downgrading durability.
        $batch->setWaitStoreMsgOk(self::isWaitStoreMsgOk($first));
        $batch->setBody($batch->encode());
        return $batch;
    }
}
