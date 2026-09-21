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

        return new Client(self::decodeUriComponents(AmqpConfig::fromURI($url)));
    }

    /**
     * thesis/amqp parses the URI with parse_url() and leaves the userinfo and
     * path components percent-encoded, so `/%2f` names a vhost literally
     * called `%2f` and an encoded username authenticates as its encoded form.
     * A URI carries those three components encoded; AMQP carries them decoded.
     *
     * Every other parsed value, the query options included, is copied
     * unchanged, and the constructor re-derives the SASL mechanisms from the
     * decoded credentials.
     */
    private static function decodeUriComponents(AmqpConfig $parsed): AmqpConfig
    {
        return new AmqpConfig(
            scheme: $parsed->scheme,
            urls: $parsed->urls,
            user: rawurldecode($parsed->user),
            password: rawurldecode($parsed->password),
            vhost: rawurldecode($parsed->vhost),
            certFile: $parsed->certFile,
            keyFile: $parsed->keyFile,
            cacertFile: $parsed->cacertFile,
            serverName: $parsed->serverName,
            authMechanisms: $parsed->authMechanisms,
            heartbeat: $parsed->heartbeat,
            connectionTimeout: $parsed->connectionTimeout,
            channelMax: $parsed->channelMax,
            frameMax: $parsed->frameMax,
            tcpNoDelay: $parsed->tcpNoDelay,
            verifyPeer: $parsed->verifyPeer,
            verifyPeerName: $parsed->verifyPeerName,
        );
    }
}
