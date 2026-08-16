<?php

declare(strict_types=1);

namespace Kinetis\QueueRabbitMq;

use Kinetis\Queue\Job;
use Kinetis\Queue\JobSerializer;
use Kinetis\Queue\QueueInterface;
use Kinetis\Queue\QueuedJob;
use Thesis\Amqp\Channel;
use Thesis\Amqp\Client;
use Thesis\Amqp\DeliveryMessage;
use Thesis\Amqp\DeliveryMode;
use Thesis\Amqp\Message;
use Thesis\Time\TimeSpan;
use function Amp\delay;

/**
 * A hard, permanent limitation, not a someday: once any method on this
 * class has opened its underlying `Thesis\Amqp\Client` connection,
 * `Kinetis\Async\concurrently()` can never be called again anywhere in
 * that same OS process, for any reason, for as long as the connection
 * stays open — and nothing here ever closes it on its own. `Thesis\Amqp\Channel`
 * keeps a permanent background reader running for its whole lifetime
 * (AMQP is a push-capable protocol — heartbeats and deliveries can arrive
 * at any time, not just in response to a request), and `concurrently()`
 * waits for `Revolt\EventLoop::run()` to return on its own once nothing
 * is left to wait on — a condition that never becomes true while that
 * reader is still registered, even for a `concurrently()` call whose own
 * tasks never touch this queue at all.
 *
 * `QueueWorker::run()` itself never calls `concurrently()` around
 * pop()/ack()/release()/fail(), so this never bites the worker loop
 * itself directly — but a job's own handle() must not call concurrently()
 * either, for its own unrelated work, once any RabbitMqQueue instance in
 * that same process has opened a connection.
 *
 * The sharper risk is a persistent HTTP worker (FrankenPHP), not just a
 * queue worker: a single controller calling push() to enqueue a job opens
 * the connection in that worker process too, and a persistent worker
 * keeps running the same process across many unrelated requests. Every
 * later request that process happens to serve — regardless of whether it
 * touches this queue, or RabbitMQ, or a job at all — loses the ability to
 * call concurrently() from that point until the worker restarts. Calling
 * push()/pop() itself is never the problem; only a later concurrently()
 * call anywhere in that same process is.
 *
 * A queue is declared durable on first use — by push(), pop(), or
 * release() on either side, whichever touches it first — and never
 * auto-created ahead of that, the same "real infrastructure resource,
 * provisioned as a side effect of normal operation, not implicitly ahead
 * of it" stance every other backend in this package takes.
 *
 * AMQP 0-9-1 has no native per-message delay, so a delayed push() goes to
 * a second, dedicated "{queue}.delay" queue instead, configured with
 * `x-dead-letter-exchange`/`x-dead-letter-routing-key` pointing back at
 * the real queue and a per-message `expiration` equal to the requested
 * delay — RabbitMQ moves the message to the real queue itself once that
 * expiration elapses, no polling involved. A queue named literally
 * "{something}.delay" would collide with this convention; queue names
 * ending in `.delay` are reserved for it.
 *
 * AMQP 0-9-1 also has no native attempt count — only a boolean
 * `redelivered` flag — so `attempts`/`maxAttempts` travel as message
 * headers instead, carried forward by release() republishing a fresh
 * message with an incremented `attempts` header before discarding the
 * original delivery (`nack(requeue: false)`), since nack's own `requeue`
 * flag redelivers the message unchanged and can't update its headers.
 * `QueuedJob::$handle` is the `Thesis\Amqp\DeliveryMessage` itself, opaque
 * to `QueueWorker` and passed straight back to ack()/release()/fail().
 *
 * One channel per instance, opened lazily on first use and reused for
 * every publish/get/ack/nack afterward — the same one-client-per-worker
 * lifecycle RedisQueue/SqlQueue/SqsQueue already have.
 *
 * pop() checks $queues in priority order via basic.get (a single,
 * immediate, non-blocking request per queue — AMQP has no native
 * blocking-wait-with-timeout primitive), sleeping between full sweeps
 * when nothing is found. $queueNamePrefix lets "high"/"default" map to
 * e.g. "myapp-high"/"myapp-default" so multiple environments sharing one
 * broker don't collide on plain queue names.
 */
final class RabbitMqQueue implements QueueInterface
{
    private const string ATTEMPTS_HEADER = 'attempts';

    private const string MAX_ATTEMPTS_HEADER = 'maxAttempts';

    private const float POLL_INTERVAL_SECONDS = 1.0;

    private ?Channel $channel = null;

    /** @var array<string, true> */
    private array $declaredQueues = [];

    public function __construct(
        private readonly Client $client,
        private readonly string $queueNamePrefix = '',
    ) {}

    #[\Override]
    public function push(Job $job, int $delaySeconds = 0, string $queue = 'default', ?int $maxAttempts = null): void
    {
        $realQueue = $this->queueNamePrefix . $queue;
        $this->ensureDeclared($realQueue);

        $serialized = JobSerializer::serialize($job);
        $headers = $maxAttempts !== null ? [self::MAX_ATTEMPTS_HEADER => $maxAttempts] : [];

        if ($delaySeconds > 0) {
            $delayQueue = $this->ensureDelayQueueDeclared($realQueue);

            $this->channel()->publish(new Message(
                body: json_encode($serialized, JSON_THROW_ON_ERROR),
                headers: $headers,
                deliveryMode: DeliveryMode::Persistent,
                expiration: TimeSpan::fromSeconds($delaySeconds),
            ), routingKey: $delayQueue);

            return;
        }

        $this->channel()->publish(new Message(
            body: json_encode($serialized, JSON_THROW_ON_ERROR),
            headers: $headers,
            deliveryMode: DeliveryMode::Persistent,
        ), routingKey: $realQueue);
    }

    #[\Override]
    public function pop(int $timeoutSeconds = 0, array $queues = ['default']): ?QueuedJob
    {
        if ($queues === []) {
            return null;
        }

        $deadline = $timeoutSeconds > 0 ? microtime(true) + $timeoutSeconds : null;

        while (true) {
            foreach ($queues as $queue) {
                $job = $this->getFrom($queue);

                if ($job !== null) {
                    return $job;
                }
            }

            if ($deadline !== null && microtime(true) >= $deadline) {
                return null;
            }

            delay(self::POLL_INTERVAL_SECONDS);
        }
    }

    #[\Override]
    public function ack(QueuedJob $job): void
    {
        $this->deliveryOf($job)->ack();
    }

    #[\Override]
    public function release(QueuedJob $job): void
    {
        $realQueue = $this->queueNamePrefix . $job->queue;
        $this->ensureDeclared($realQueue);

        $headers = [self::ATTEMPTS_HEADER => $job->attempts];

        if ($job->maxAttempts !== null) {
            $headers[self::MAX_ATTEMPTS_HEADER] = $job->maxAttempts;
        }

        $this->channel()->publish(new Message(
            body: json_encode(['class' => $job->class, 'args' => $job->args], JSON_THROW_ON_ERROR),
            headers: $headers,
            deliveryMode: DeliveryMode::Persistent,
        ), routingKey: $realQueue);

        $this->deliveryOf($job)->nack(requeue: false);
    }

    #[\Override]
    public function fail(QueuedJob $job): void
    {
        $this->deliveryOf($job)->nack(requeue: false);
    }

    private function getFrom(string $queue): ?QueuedJob
    {
        $realQueue = $this->queueNamePrefix . $queue;
        $this->ensureDeclared($realQueue);

        $delivery = $this->channel()->get($realQueue);

        if ($delivery === null) {
            return null;
        }

        /** @var array{class: class-string<Job>, args: array<string, mixed>} $decoded */
        $decoded = json_decode($delivery->message->body, true, flags: JSON_THROW_ON_ERROR);

        $completedAttempts = (int) ($delivery->message->headers[self::ATTEMPTS_HEADER] ?? 0);
        $maxAttempts = isset($delivery->message->headers[self::MAX_ATTEMPTS_HEADER])
            ? (int) $delivery->message->headers[self::MAX_ATTEMPTS_HEADER]
            : null;

        return new QueuedJob(
            $decoded['class'],
            $decoded['args'],
            handle: $delivery,
            queue: $queue,
            attempts: $completedAttempts + 1,
            maxAttempts: $maxAttempts,
        );
    }

    private function deliveryOf(QueuedJob $job): DeliveryMessage
    {
        /** @var DeliveryMessage */
        return $job->handle;
    }

    /**
     * queueDeclare() returns the queue's current message count, so the
     * declare this backend already performs on first touch doubles as
     * the count — no separate management-API call. Delayed jobs live in
     * the separate `{queue}.delay` queue and are counted with it, so a
     * job waiting out its delay is outstanding here the same as on every
     * other backend. The delay queue is declared (idempotently, with the
     * exact arguments push() uses) rather than passively probed: a
     * passive declare of a queue another process's delayed push created
     * — the normal state for a stats command's own fresh process — would
     * otherwise be the only way to see it, and a passive declare of a
     * *missing* queue closes the channel as an AMQP error.
     *
     * Messages already delivered to a consumer and not yet acked are
     * excluded by the broker's own count, matching the reserved/
     * processing exclusion elsewhere.
     */
    #[\Override]
    public function size(string $queue = 'default'): int
    {
        $name = $this->queueNamePrefix . $queue;
        $this->ensureDeclared($name);
        $delayQueue = $this->ensureDelayQueueDeclared($name);

        return $this->channel()->queueDeclare($name, passive: true)->messages
            + $this->channel()->queueDeclare($delayQueue, passive: true)->messages;
    }

    #[\Override]
    public function clear(string $queue = 'default'): int
    {
        $name = $this->queueNamePrefix . $queue;

        if ($name === '') {
            return 0;
        }

        $size = $this->size($queue);

        // size() above already declared both queues, so neither purge
        // can hit AMQP's missing-queue channel error; the explicit
        // ensure keeps that safety local instead of an ordering detail.
        $delayQueue = $this->ensureDelayQueueDeclared($name);
        $this->channel()->queuePurge($name);
        $this->channel()->queuePurge($delayQueue);

        return $size;
    }

    private function channel(): Channel
    {
        return $this->channel ??= $this->client->channel();
    }

    private function ensureDeclared(string $queue): void
    {
        if (isset($this->declaredQueues[$queue])) {
            return;
        }

        $this->channel()->queueDeclare($queue, durable: true);
        $this->declaredQueues[$queue] = true;
    }

    /** @return non-empty-string */
    private function ensureDelayQueueDeclared(string $realQueue): string
    {
        $delayQueue = $realQueue . '.delay';

        if (isset($this->declaredQueues[$delayQueue])) {
            return $delayQueue;
        }

        $this->channel()->queueDeclare($delayQueue, durable: true, arguments: [
            'x-dead-letter-exchange' => '',
            'x-dead-letter-routing-key' => $realQueue,
        ]);
        $this->declaredQueues[$delayQueue] = true;

        return $delayQueue;
    }
}
