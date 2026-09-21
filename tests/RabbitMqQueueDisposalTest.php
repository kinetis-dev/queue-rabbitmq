<?php

declare(strict_types=1);

namespace Kinetis\QueueRabbitMq\Tests;

use Closure;
use Kinetis\Config\Config;
use Kinetis\Container\AppScope;
use Kinetis\Queue\DisposableQueueInterface;
use Kinetis\Queue\PackageBootstrap;
use Kinetis\Queue\QueueInterface;
use Kinetis\QueueRabbitMq\RabbitMqClientFactory;
use Kinetis\QueueRabbitMq\RabbitMqQueue;
use Kinetis\QueueRabbitMq\RabbitMqQueueFactory;
use PHPUnit\Framework\TestCase;
use ReflectionFunction;
use ReflectionProperty;
use Thesis\Amqp\Client;

/**
 * Who disconnects the client, and when. `Client::disconnect()` closes
 * the channel factory — this queue's own channel with it — and then the
 * connection, and returns at once when nothing has connected yet, which
 * is what lets the whole file run with no broker reachable and makes it
 * the check that disposal before the queue's first frame is safe.
 */
final class RabbitMqQueueDisposalTest extends TestCase
{
    private const string URL = 'amqp://guest:guest@localhost:5672/';

    public function test_the_factory_hands_the_queue_the_client_it_built(): void
    {
        $queue = RabbitMqQueueFactory::fromConfig(new Config(['QUEUE_RABBITMQ_URL' => self::URL]));

        self::assertSame(self::client($queue), self::ownedClient($queue));
    }

    public function test_disposing_the_factory_queue_before_any_frame_is_safe_and_idempotent(): void
    {
        $queue = RabbitMqQueueFactory::fromConfig(new Config(['QUEUE_RABBITMQ_URL' => self::URL]));
        self::assertSame(self::client($queue), self::ownedClient($queue));

        $queue->dispose();
        $queue->dispose();

        self::assertNull(
            new ReflectionProperty(RabbitMqQueue::class, 'disposer')->getValue($queue),
            'the disposer is dropped as it runs, so the second call disconnects nothing a second time',
        );
    }

    /**
     * A client the caller built stays the caller's to disconnect: the
     * queue was lent a connection, not given one, so a second consumer
     * of that client keeps working after the queue is finished with it.
     */
    public function test_a_directly_constructed_queue_owns_no_client(): void
    {
        $queue = new RabbitMqQueue(RabbitMqClientFactory::fromConfig(new Config(['QUEUE_RABBITMQ_URL' => self::URL])));

        self::assertNull(new ReflectionProperty(RabbitMqQueue::class, 'disposer')->getValue($queue));

        $queue->dispose();
    }

    public function test_a_caller_can_hand_the_queue_a_client_to_own(): void
    {
        $disconnects = 0;
        $queue = new RabbitMqQueue(
            RabbitMqClientFactory::fromConfig(new Config(['QUEUE_RABBITMQ_URL' => self::URL])),
            '',
            static function () use (&$disconnects): void {
                ++$disconnects;
            },
        );

        $queue->dispose();
        $queue->dispose();

        self::assertSame(1, $disconnects);
    }

    /**
     * End to end through the binding an application actually gets:
     * nothing is registered until something injects the queue, and the
     * worker's own teardown is what disconnects the client the binding
     * built.
     */
    public function test_the_package_bootstrap_disconnects_the_queue_it_built_when_the_worker_ends(): void
    {
        $app = new AppScope();
        new PackageBootstrap()->register($app, new Config([
            'QUEUE_CONNECTION' => 'rabbitmq',
            'QUEUE_RABBITMQ_URL' => self::URL,
        ]));
        $app->boot();

        self::assertSame([], self::disposeCallbacks($app));

        $queue = $app->get(QueueInterface::class);
        self::assertInstanceOf(RabbitMqQueue::class, $queue);
        self::assertInstanceOf(DisposableQueueInterface::class, $queue);
        self::assertCount(1, self::disposeCallbacks($app), 'one disconnect for the one backend built');
        self::assertSame(self::client($queue), self::ownedClient($queue));

        $app->dispose();

        self::assertNull(
            new ReflectionProperty(RabbitMqQueue::class, 'disposer')->getValue($queue),
            "the scope's disposal ran this queue's own dispose",
        );
    }

    /**
     * The client the queue was handed to disconnect, read back off the
     * disposer itself: a factory wiring some other object's disconnect
     * would leave this queue's own connection open, and that is the
     * shape this reads.
     */
    private static function ownedClient(RabbitMqQueue $queue): Client
    {
        $disposer = new ReflectionProperty(RabbitMqQueue::class, 'disposer')->getValue($queue);
        self::assertInstanceOf(Closure::class, $disposer);

        $client = new ReflectionFunction($disposer)->getClosureThis();
        self::assertInstanceOf(Client::class, $client);

        return $client;
    }

    private static function client(RabbitMqQueue $queue): Client
    {
        /** @var Client */
        return new ReflectionProperty(RabbitMqQueue::class, 'client')->getValue($queue);
    }

    /**
     * @return list<callable(): void>
     */
    private static function disposeCallbacks(AppScope $app): array
    {
        /** @var list<callable(): void> */
        return new ReflectionProperty(AppScope::class, 'disposeCallbacks')->getValue($app);
    }
}
