<?php

declare(strict_types=1);

namespace Kinetis\QueueRabbitMq;

use Kinetis\Instrumentation\Telemetry;
use Kinetis\Queue\Job;
use Kinetis\Queue\JobSerializer;
use Kinetis\Queue\QueueContract;
use Kinetis\Queue\QueueInterface;
use Kinetis\Queue\QueuedJob;
use Kinetis\Queue\Support\PopSweep;
use Thesis\Amqp\Channel;
use Thesis\Amqp\Client;
use Thesis\Amqp\DeliveryMessage;
use Thesis\Amqp\DeliveryMode;
use Thesis\Amqp\Message;
use Thesis\Time\TimeSpan;
use function Amp\delay;
use Throwable;

/**
 * `Kinetis\Async\concurrently()` composes correctly with this class —
 * confirmed directly against a real broker, not assumed: once its
 * underlying `Thesis\Amqp\Client` connection opens, running two 50ms
 * timer tasks through `concurrently()` still returns promptly rather
 * than hanging. `Kinetis\Async\ConcurrentBatch` parks on a targeted
 * Revolt suspension resumed once its own tasks finish, unaffected by any
 * other still-registered watcher — so this queue's connection, whose
 * `Thesis\Amqp\Channel` keeps a permanent background reader registered
 * (AMQP is a push-capable protocol; heartbeats and deliveries can arrive
 * at any time), never interferes with `concurrently()` calls anywhere
 * else in the process.
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
 * release() is two separate AMQP operations, not one — publish, then
 * nack — because AMQP 0-9-1 has no cross-message transaction primitive
 * to make them atomic. Publishing before nacking means a crash between
 * the two never loses the job: the original delivery stays unacked and
 * the broker redelivers it once the connection drops. It does mean a
 * crash in that same window can leave both the redelivered original and
 * the freshly published replacement in the queue at once — a real,
 * open duplication window this ordering cannot close, unlike
 * RedisQueue/SqlQueue's own release(), which is one atomic operation.
 * A job handler that runs through this backend must tolerate being
 * invoked more than once for the same logical job.
 *
 * One channel per instance, opened lazily on first use and reused for
 * every publish/get/ack/nack afterward — the same one-client-per-worker
 * lifecycle RedisQueue/SqlQueue/SqsQueue already have.
 *
 * pop()'s whole priority/timeout algorithm is Kinetis\Queue\Support\PopSweep
 * — see that class and QueueInterface's own docblock for the full
 * cross-backend contract. This backend has no native blocking-wait-with-
 * timeout primitive at all (AMQP 0-9-1's basic.get is always a single,
 * immediate, non-blocking request per queue), so it runs PopSweep with
 * probeCanBlock: false — every probe is instant regardless of the wait
 * budget it's offered, and pacing between full sweeps is entirely
 * PopSweep's own bounded sleep() between them. $queueNamePrefix
 * lets "high"/"default" map to e.g. "myapp-high"/"myapp-default" so
 * multiple environments sharing one broker don't collide on plain queue
 * names.
 */
final class RabbitMqQueue implements QueueInterface
{
    private const string ATTEMPTS_HEADER = 'attempts';

    private const string MAX_ATTEMPTS_HEADER = 'maxAttempts';

    private const string METADATA_HEADER = 'metadata';

    private const float POLL_INTERVAL_SECONDS = 1.0;

    private ?Channel $channel = null;

    /** @var array<string, true> */
    private array $declaredQueues = [];

    public function __construct(
        private readonly Client $client,
        private readonly string $queueNamePrefix = '',
    ) {
        QueueContract::assertValidQueueNamePrefix($queueNamePrefix);
    }

    #[\Override]
    public function push(Job $job, int $delaySeconds = 0, string $queue = 'default', ?int $maxAttempts = null): void
    {
        QueueContract::assertValidPushArguments($delaySeconds, $queue, $maxAttempts);

        $telemetry = Telemetry::global();
        $telemetryToken = $telemetry->jobPushStarted($job::class, $queue);

        try {
            $realQueue = $this->queueNamePrefix . $queue;
            $this->ensureDeclared($realQueue);

            $serialized = JobSerializer::serialize($job);
            $headers = $maxAttempts !== null ? [self::MAX_ATTEMPTS_HEADER => $maxAttempts] : [];
            $metadata = $telemetry->jobPushMetadata($telemetryToken);

            if ($metadata !== []) {
                $headers[self::METADATA_HEADER] = json_encode($metadata, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION);
            }

            // PRESERVE_ZERO_FRACTION, both branches: without it, an
            // integral-valued float argument (4.0) encodes as "4" and
            // decodes back as an int — a silent type change
            // JobSerializer's own portable-value contract promises never
            // happens.
            if ($delaySeconds > 0) {
                $delayQueue = $this->ensureDelayQueueDeclared($realQueue);

                $this->channel()->publish(new Message(
                    body: json_encode($serialized, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION),
                    headers: $headers,
                    deliveryMode: DeliveryMode::Persistent,
                    expiration: TimeSpan::fromSeconds($delaySeconds),
                ), routingKey: $delayQueue);
            } else {
                $this->channel()->publish(new Message(
                    body: json_encode($serialized, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION),
                    headers: $headers,
                    deliveryMode: DeliveryMode::Persistent,
                ), routingKey: $realQueue);
            }

            $telemetry->jobPushEnded($telemetryToken, null);
        } catch (Throwable $e) {
            $telemetry->jobPushEnded($telemetryToken, $e);

            throw $e;
        }
    }

    #[\Override]
    public function pop(int $timeoutSeconds = 0, array $queues = ['default']): ?QueuedJob
    {
        // PopSweep::run() itself validates $timeoutSeconds/$queues via
        // QueueContract before touching either — see that class's own
        // docblock for why it doesn't trust a caller to have already
        // done so.
        return PopSweep::run(
            timeoutSeconds: $timeoutSeconds,
            queues: $queues,
            probe: fn (string $queue): ?QueuedJob => $this->getFrom($queue),
            probeCanBlock: false,
            waitCapSeconds: self::POLL_INTERVAL_SECONDS,
            sleep: static function (float $seconds): void {
                delay($seconds);
            },
        );
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

        if ($job->metadata !== []) {
            $headers[self::METADATA_HEADER] = json_encode($job->metadata, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION);
        }

        // PRESERVE_ZERO_FRACTION: $job->args already went through this
        // exact encoding once at push() time — re-encoding it the same
        // way here keeps a released job's own float values from
        // silently narrowing on a second pass through this codepath.
        $this->channel()->publish(new Message(
            body: json_encode(['class' => $job->class, 'args' => $job->args], JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION),
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

        return QueueContract::settleIfMalformed(
            $queue,
            fn (): QueuedJob => self::buildQueuedJob(
                queue: $queue,
                handle: $delivery,
                body: $delivery->message->body,
                headers: $delivery->message->headers,
            ),
            fn () => $delivery->nack(requeue: false),
        );
    }

    /**
     * Extracted out of getFrom() and taking the raw body/headers directly
     * rather than the whole DeliveryMessage — independently testable with
     * a hand-built headers array, no real broker round trip needed. Every
     * field is read through one of QueueContract's own coercion helpers
     * rather than trusted at a PHPStan-asserted @var shape — $body might
     * not even be valid JSON, or might decode to something other than a
     * {class, args} object, and the attempts/maxAttempts headers are read
     * through QueueContract::coerceStoredCompletedAttempts()/
     * coerceStoredInteger() rather than a lossy `(int)` cast: AMQP field
     * tables can carry a typed integer (the normal case, for a header
     * this class itself wrote) but a non-Kinetis publisher, or a
     * hand-edited one, could set either header to anything. The attempts
     * header specifically goes through coerceStoredCompletedAttempts(),
     * not coerceStoredMaxAttempts() (used for maxAttempts just below) —
     * this stored value is the completed-attempts count (0-indexed) that
     * gets a real `+ 1` below, and that method is what keeps a stored
     * PHP_INT_MAX from silently overflowing that addition into a float,
     * and also rejects a negative stored count outright. Its own absence
     * (no header at all) is deliberately not treated as malformed —
     * push() never sets this header at all, only release() does, so a
     * message on its genuine first attempt has none, and the `?? 0`
     * default below reads that correctly as "zero completed attempts so
     * far." maxAttempts, unlike RedisQueue's own field, is only ever
     * conditionally written by push() too (never for a null $maxAttempts
     * argument — see that method), so its absence is equally never a
     * sign of corruption, and coerceStoredMaxAttempts() already treats a
     * null $raw as "no override" directly, with no presence check
     * needed on top. Every failure here is caught by getFrom() — see
     * QueueContract::settleIfMalformed() — so a malformed delivery
     * settles the already-reserved delivery rather than crashing the
     * worker.
     *
     * @param array<string, mixed> $headers
     */
    private static function buildQueuedJob(string $queue, mixed $handle, string $body, array $headers): QueuedJob
    {
        $decoded = QueueContract::coerceStoredJsonArray($body, 'body');

        $class = QueueContract::coerceStoredClass($decoded['class'] ?? null);
        $args = QueueContract::coerceStoredArgs($decoded['args'] ?? null);

        $rawAttempts = $headers[self::ATTEMPTS_HEADER] ?? 0;
        $completedAttempts = QueueContract::coerceStoredCompletedAttempts($rawAttempts, self::ATTEMPTS_HEADER);

        $maxAttempts = QueueContract::coerceStoredMaxAttempts($headers[self::MAX_ATTEMPTS_HEADER] ?? null, self::MAX_ATTEMPTS_HEADER);

        $metadata = QueueContract::coerceStoredMetadata($headers[self::METADATA_HEADER] ?? null);

        return new QueuedJob(
            $class,
            $args,
            handle: $handle,
            queue: $queue,
            attempts: $completedAttempts + 1,
            maxAttempts: $maxAttempts,
            metadata: $metadata,
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
        QueueContract::assertValidQueueName($queue);

        $name = $this->queueNamePrefix . $queue;
        $this->ensureDeclared($name);
        $delayQueue = $this->ensureDelayQueueDeclared($name);

        return $this->channel()->queueDeclare($name, passive: true)->messages
            + $this->channel()->queueDeclare($delayQueue, passive: true)->messages;
    }

    #[\Override]
    public function clear(string $queue = 'default'): int
    {
        // size() below also validates $queue internally, but PHPStan
        // can't see across that call to know $name (built from $queue
        // right here) is provably non-empty for queuePurge()'s own
        // non-empty-string parameter — asserting it directly in this
        // scope too is what gives it that, not just runtime safety.
        QueueContract::assertValidQueueName($queue);

        $size = $this->size($queue);
        $name = $this->queueNamePrefix . $queue;

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
