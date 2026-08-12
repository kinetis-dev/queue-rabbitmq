<?php

declare(strict_types=1);

namespace Kinetis\QueueRabbitMq\Tests;

use Kinetis\Config\Config;
use Kinetis\Config\Exception\MissingConfigException;
use Kinetis\QueueRabbitMq\RabbitMqClientFactory;
use PHPUnit\Framework\TestCase;
use Thesis\Amqp\Client;

final class RabbitMqClientFactoryTest extends TestCase
{
    public function test_builds_a_client_for_the_default_connection(): void
    {
        $config = new Config(['QUEUE_RABBITMQ_URL' => 'amqp://guest:guest@localhost:5672/']);

        $client = RabbitMqClientFactory::fromConfig($config);

        self::assertInstanceOf(Client::class, $client);
        self::assertSame(['tcp://localhost:5672'], iterator_to_array($client->config->connectionUrls()));
        self::assertSame('guest', $client->config->user);
    }

    public function test_a_named_connection_reads_its_own_url_not_the_default(): void
    {
        $config = new Config([
            'QUEUE_RABBITMQ_URL' => 'amqp://guest:guest@localhost:5672/',
            'QUEUE_REPORTS_RABBITMQ_URL' => 'amqp://reports:secret@rabbitmq-reports:5672/reports',
        ]);

        $default = RabbitMqClientFactory::fromConfig($config);
        $reports = RabbitMqClientFactory::fromConfig($config, 'reports');

        self::assertSame(['tcp://localhost:5672'], iterator_to_array($default->config->connectionUrls()));
        self::assertSame(['tcp://rabbitmq-reports:5672'], iterator_to_array($reports->config->connectionUrls()));
        self::assertSame('reports', $reports->config->user);
        self::assertSame('reports', $reports->config->vhost);
    }

    public function test_a_missing_url_throws_a_clear_error(): void
    {
        $config = new Config([]);

        $this->expectException(MissingConfigException::class);
        $this->expectExceptionMessage('QUEUE_RABBITMQ_URL');
        RabbitMqClientFactory::fromConfig($config);
    }

    public function test_a_named_connections_missing_url_names_its_own_scoped_key(): void
    {
        $config = new Config([]);

        $this->expectException(MissingConfigException::class);
        $this->expectExceptionMessage('QUEUE_REPORTS_RABBITMQ_URL');
        RabbitMqClientFactory::fromConfig($config, 'reports');
    }
}
