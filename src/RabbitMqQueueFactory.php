<?php

declare(strict_types=1);

namespace Kinetis\QueueRabbitMq;

use Kinetis\Config\Config;

/**
 * Builds the RabbitMQ queue backend `QUEUE_CONNECTION=rabbitmq` selects
 * — called by `kinetis/queue`'s own `QueueFactory::fromConfig()`, gated
 * behind a `class_exists()` check so core never depends on this package
 * directly.
 *
 * Returns the concrete `RabbitMqQueue`, which declares both the
 * clearing and the disposal capability; see `QueueFactory` for why the
 * connection-driven factory stays on `QueueInterface`.
 *
 * The client is built here and belongs to the queue that gets it, so
 * its disconnect() travels with it as the queue's disposer — see
 * `Kinetis\Queue\DisposableQueueInterface`. A caller binding this
 * result itself registers `$queue->dispose(...)` on the scope it binds
 * into; `Kinetis\Queue\PackageBootstrap` does that for the queue
 * `QUEUE_CONNECTION=rabbitmq` builds.
 */
final class RabbitMqQueueFactory
{
    public static function fromConfig(Config $config, string $connectionName = 'default'): RabbitMqQueue
    {
        $queuePrefix = $config->string(Config::scopedKey('QUEUE_RABBITMQ_QUEUE_PREFIX', $connectionName), '');
        $client = RabbitMqClientFactory::fromConfig($config, $connectionName);

        return new RabbitMqQueue($client, $queuePrefix, $client->disconnect(...));
    }
}
