<?php

declare(strict_types=1);

namespace Kinetis\QueueRabbitMq;

use Kinetis\Config\Config;
use Kinetis\Queue\QueueInterface;

/**
 * Builds the RabbitMQ queue backend `QUEUE_CONNECTION=rabbitmq` selects
 * — called by `kinetis/queue`'s own `QueueFactory::fromConfig()`, gated
 * behind a `class_exists()` check so core never depends on this package
 * directly.
 */
final class RabbitMqQueueFactory
{
    public static function fromConfig(Config $config, string $connectionName = 'default'): QueueInterface
    {
        $queuePrefix = $config->string(Config::scopedKey('QUEUE_RABBITMQ_QUEUE_PREFIX', $connectionName), '');

        return new RabbitMqQueue(RabbitMqClientFactory::fromConfig($config, $connectionName), $queuePrefix);
    }
}
