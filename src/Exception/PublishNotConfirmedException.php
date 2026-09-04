<?php

declare(strict_types=1);

namespace Kinetis\QueueRabbitMq\Exception;

use RuntimeException;
use Thesis\Amqp\PublishResult;

/**
 * A publish RabbitMQ did not acknowledge — see `RabbitMqQueue`'s own
 * docblock for why every publish waits for that acknowledgement, and
 * what release() does with this.
 *
 * Nothing is settled when this is thrown, so a job whose replacement
 * never landed is still queued.
 */
final class PublishNotConfirmedException extends RuntimeException
{
    /**
     * The channel answered, and the answer was not an acknowledgement —
     * `Nacked` (the broker refused the message), `Unrouted` (mandatory
     * publishing found no queue to route it to), `Canceled`, or
     * `Waiting`.
     */
    public static function answered(string $target, PublishResult $result): self
    {
        return new self(
            "RabbitMQ did not confirm the message published to \"{$target}\": {$result->name}.",
        );
    }

    /**
     * No confirmation to wait on at all, which `Channel::publish()`
     * returns only for a channel outside confirm mode.
     */
    public static function unconfirmable(string $target): self
    {
        return new self(
            "RabbitMQ did not confirm the message published to \"{$target}\": "
            . 'the publishing channel is not in confirm mode.',
        );
    }
}
