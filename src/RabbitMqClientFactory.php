<?php

declare(strict_types=1);

namespace Kinetis\QueueRabbitMq;

use InvalidArgumentException;
use Kinetis\Config\Config;
use Thesis\Amqp\Client;
use Thesis\Amqp\Config as AmqpConfig;

/**
 * $connection selects a named connection via Config::scopedKey() — plugged
 * into kinetis/queue's QueueFactory dispatch when QUEUE_CONNECTION=rabbitmq,
 * never resolved automatically by type.
 */
final class RabbitMqClientFactory
{
    public static function fromConfig(Config $config, string $connection = 'default'): Client
    {
        $key = Config::scopedKey('QUEUE_RABBITMQ_URL', $connection);
        $url = $config->required($key);

        if ($url === '') {
            throw new InvalidArgumentException("{$key} must not be empty.");
        }

        return new Client(AmqpConfig::fromURI($url));
    }
}
