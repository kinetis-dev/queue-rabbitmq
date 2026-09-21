<?php

declare(strict_types=1);

namespace Kinetis\QueueRabbitMq\Tests;

use Kinetis\Config\Config;
use Kinetis\Config\Exception\MissingConfigException;
use Kinetis\QueueRabbitMq\RabbitMqClientFactory;
use PHPUnit\Framework\TestCase;
use Thesis\Amqp\Client;
use Thesis\Amqp\Internal\Protocol\Auth\Plain;
use Thesis\Amqp\Scheme;

final class RabbitMqClientFactoryTest extends TestCase
{
    public function test_builds_a_client_for_the_default_connection(): void
    {
        $config = new Config(['QUEUE_RABBITMQ_URL' => 'amqp://guest:guest@localhost:5672/']);

        $client = RabbitMqClientFactory::fromConfig($config);

        self::assertInstanceOf(Client::class, $client);
        self::assertSame(['tcp://localhost:5672'], iterator_to_array($client->config->connectionUrls()));
        self::assertSame('guest', $client->config->user);
        self::assertSame('guest', $client->config->password);
        self::assertSame('/', $client->config->vhost);
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

    public function test_percent_encoded_user_password_and_vhost_are_decoded(): void
    {
        $config = new Config(['QUEUE_RABBITMQ_URL' => 'amqp://us%65r:p%40ss%2Fword@rabbitmq:5672/%2f']);

        $client = RabbitMqClientFactory::fromConfig($config);

        self::assertSame('user', $client->config->user);
        self::assertSame('p@ss/word', $client->config->password);
        self::assertSame('/', $client->config->vhost);
    }

    public function test_the_credentials_the_handshake_sends_are_the_decoded_ones(): void
    {
        // Config derives its SASL mechanisms in the constructor, so decoding
        // has to reach the object the handshake authenticates with, not only
        // the properties a caller reads back.
        $config = new Config(['QUEUE_RABBITMQ_URL' => 'amqp://us%65r:p%40ss%2Fword@rabbitmq:5672/%2f']);

        $mechanism = RabbitMqClientFactory::fromConfig($config)->config->sasl()[0];

        self::assertInstanceOf(Plain::class, $mechanism);
        self::assertSame('user', $mechanism->username);
        self::assertSame('p@ss/word', $mechanism->password);
    }

    public function test_an_encoded_component_is_decoded_exactly_once(): void
    {
        $config = new Config(['QUEUE_RABBITMQ_URL' => 'amqp://a%2561:b%2562@rabbitmq:5672/v%252fhost']);

        $client = RabbitMqClientFactory::fromConfig($config);

        self::assertSame('a%61', $client->config->user);
        self::assertSame('b%62', $client->config->password);
        self::assertSame('v%2fhost', $client->config->vhost);
    }

    public function test_every_other_parsed_component_survives_unchanged(): void
    {
        $config = new Config(['QUEUE_RABBITMQ_URL' => 'amqps://reports:secret@rabbit-a:5671,rabbit-b:5672/production'
            . '?certfile=/tls/client.pem&keyfile=/tls/client.key&cacertfile=/tls/ca.pem'
            . '&server_name_indication=broker.example&auth_mechanism=AMQPLAIN'
            . '&heartbeat=15&connection_timeout=3&channel_max=64&frame_max=4096'
            . '&tcp_nodelay=0&verify_peer=0&verify_peer_name=0']);

        $amqp = RabbitMqClientFactory::fromConfig($config)->config;

        self::assertSame(Scheme::amqps, $amqp->scheme);
        self::assertSame(['tcp://rabbit-a:5671', 'tcp://rabbit-b:5672'], iterator_to_array($amqp->connectionUrls()));
        self::assertSame('reports', $amqp->user);
        self::assertSame('secret', $amqp->password);
        self::assertSame('production', $amqp->vhost);
        self::assertSame('/tls/client.pem', $amqp->certFile);
        self::assertSame('/tls/client.key', $amqp->keyFile);
        self::assertSame('/tls/ca.pem', $amqp->cacertFile);
        self::assertSame('broker.example', $amqp->serverName);
        self::assertSame(['AMQPLAIN'], $amqp->authMechanisms);
        self::assertSame(15, $amqp->heartbeat);
        self::assertSame(3.0, $amqp->connectionTimeout);
        self::assertSame(64, $amqp->channelMax);
        self::assertSame(4096, $amqp->frameMax);
        self::assertFalse($amqp->tcpNoDelay);
        self::assertFalse($amqp->verifyPeer);
        self::assertFalse($amqp->verifyPeerName);
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
