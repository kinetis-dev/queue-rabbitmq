<?php

declare(strict_types=1);

namespace Kinetis\QueueRabbitMq;

use Kinetis\Instrumentation\Telemetry;
use Kinetis\Queue\ClearableQueueInterface;
use Kinetis\Queue\Job;
use Kinetis\Queue\JobSerializer;
use Kinetis\Queue\QueueContract;
use Kinetis\Queue\QueuedJob;
use Kinetis\QueueRabbitMq\Exception\PublishNotConfirmedException;
use Thesis\Amqp\Channel;
use Thesis\Amqp\Client;
use Thesis\Amqp\DeliveryMessage;
use Thesis\Amqp\DeliveryMode;
use Thesis\Amqp\Message;
use Thesis\Amqp\PublishConfirmation;
use Thesis\Amqp\PublishResult;
use Thesis\Amqp\Queue as AmqpQueue;
use function Amp\delay;
use Throwable;

/**
 * `Kinetis\Async\concurrently()` composes correctly with this class, and
 * a real-broker check in `tests-integration/` holds it there: once the
 * `Thesis\Amqp\Client` connection opens, two 50ms timer tasks through
 * `concurrently()` still return promptly. `Kinetis\Async\ConcurrentBatch`
 * parks on a targeted Revolt suspension resumed by its own tasks,
 * unaffected by any other registered watcher — and this connection's
 * channel keeps a permanent background reader registered, since AMQP is
 * push-capable and heartbeats or deliveries can arrive at any time.
 *
 * A queue is declared durable on first use — by push(), pop() or
 * release(), whichever touches it first — and never provisioned ahead of
 * that, the same stance every other backend here takes. A delayed push()
 * declares the tiers its own delay uses; size()/clear() declare every
 * tier, since a message parked by another process can be in any of them.
 * DelayLadder owns that topology and its delay properties.
 *
 * **Every publish is confirmed before it counts as having happened.**
 * The channel runs in confirm mode, publishing is `mandatory`, and the
 * broker's answer is awaited: `Channel::publish()` returning means the
 * frames reached the socket, not that RabbitMQ accepted, routed or
 * durably recorded anything. release() is where that decides whether a
 * job can be lost — it publishes the replacement, waits for the
 * acknowledgement, and only then discards the original with
 * `nack(requeue: false)`, so an unconfirmed publish raises
 * Exception\PublishNotConfirmedException with the original still unacked
 * and the broker free to redeliver it. Mandatory publishing turns an
 * unroutable message into that same exception rather than a silent drop,
 * at the cost of the `X-Thesis-Mandatory-Id` header `Thesis\Amqp`
 * correlates a returned message by, which travels with the job.
 *
 * The reverse window cannot be closed here: AMQP 0-9-1 has no
 * cross-message transaction, so a crash between a confirmed publish and
 * the nack leaves both the redelivered original and the replacement in
 * the queue, unlike RedisQueue's and SqlQueue's single-operation
 * release(). A handler running through this backend must tolerate being
 * invoked more than once for the same logical job.
 *
 * AMQP 0-9-1 has no native attempt count, only a boolean `redelivered`
 * flag, so `attempts`/`maxAttempts` travel as message headers — carried
 * forward by release() republishing with an incremented `attempts`
 * header, since nack's `requeue` flag redelivers a message unchanged and
 * cannot update its headers.
 *
 * **Settlements here are unfenced.** QueuedJob::$handle is the
 * `Thesis\Amqp\DeliveryMessage` itself, opaque to QueueWorker. A
 * delivery tag is scoped to the channel that produced it and the broker
 * answers a second settlement with a channel-level error, so this
 * backend raises no Exception\StaleJobHandleException of its own.
 *
 * One channel per instance, opened lazily and reused for every
 * publish/get/ack/nack — the same one-client-per-worker lifecycle the
 * other backends have.
 *
 * pop() sweeps every queue in priority order with basic.get, which is
 * always an immediate, non-blocking request, then suspends through
 * Revolt for up to a second before sweeping again, less when less of
 * the deadline remains. AMQP 0-9-1 offers no blocking-wait-with-timeout
 * primitive, so that paced sweep is what bounds an idle pop() — see
 * QueueInterface for the contract it meets.
 *
 * $queueNamePrefix maps "high"/"default" onto "myapp-high"/"myapp-default"
 * so environments sharing one broker do not collide.
 */
final class RabbitMqQueue implements ClearableQueueInterface
{
    private const string ATTEMPTS_HEADER = 'attempts';

    private const string MAX_ATTEMPTS_HEADER = 'maxAttempts';

    private const string METADATA_HEADER = 'metadata';

    private const float POLL_INTERVAL_SECONDS = 1.0;

    private ?Channel $channel = null;

    /** @var array<string, true> */
    private array $declaredQueues = [];

    /**
     * The highest delay tier whose exchange and bindings this instance
     * has already declared, per real queue name.
     *
     * @var array<string, int<0, DelayLadder::TOP_TIER>>
     */
    private array $declaredLadders = [];

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
        DelayLadder::assertSupportedDelay($delaySeconds);

        $telemetry = Telemetry::global();
        $telemetryToken = $telemetry->jobPushStarted($job::class, $queue);

        try {
            $realQueue = $this->realQueue($queue);

            $serialized = JobSerializer::serialize($job);
            $headers = $maxAttempts !== null ? [self::MAX_ATTEMPTS_HEADER => $maxAttempts] : [];
            $metadata = $telemetry->jobPushMetadata($telemetryToken);

            if ($metadata !== []) {
                $headers[self::METADATA_HEADER] = json_encode($metadata, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION);
            }

            // PRESERVE_ZERO_FRACTION: without it, an integral-valued
            // float argument (4.0) encodes as "4" and decodes back as an
            // int — a silent type change JobSerializer's own
            // portable-value contract promises never happens.
            $message = new Message(
                body: json_encode($serialized, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION),
                headers: $headers,
                deliveryMode: DeliveryMode::Persistent,
            );

            if ($delaySeconds > 0) {
                $tier = DelayLadder::entryTier($delaySeconds);
                $this->ensureLadderDeclared($realQueue, $tier);

                $this->publishConfirmed(
                    $message,
                    exchange: DelayLadder::exchange($realQueue, $tier),
                    routingKey: DelayLadder::routingKey($delaySeconds),
                );
            } else {
                $this->ensureDeclared($realQueue);

                $this->publishConfirmed($message, exchange: '', routingKey: $realQueue);
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
        QueueContract::assertValidPopArguments($timeoutSeconds, $queues);

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

            // basic.get never blocks, so pacing between sweeps is what
            // keeps an idle pop() off the broker. delay() suspends the
            // calling Fiber through Revolt rather than blocking the loop,
            // and the pause is cut short by whatever is left of the
            // deadline so pop() does not overshoot it.
            $pause = self::POLL_INTERVAL_SECONDS;

            if ($deadline !== null) {
                $remaining = $deadline - microtime(true);

                if ($remaining <= 0.0) {
                    return null;
                }

                $pause = min($pause, $remaining);
            }

            delay($pause);
        }
    }

    #[\Override]
    public function ack(QueuedJob $job): void
    {
        $this->deliveryOf($job)->ack();
    }

    /**
     * The replacement is published, confirmed by the broker, and only
     * then is the original delivery discarded — see this class's own
     * docblock for what each of those two steps protects against.
     */
    #[\Override]
    public function release(QueuedJob $job): void
    {
        $realQueue = $this->realQueue($job->queue);
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
        $this->publishConfirmed(
            new Message(
                body: json_encode(['class' => $job->class, 'args' => $job->args], JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION),
                headers: $headers,
                deliveryMode: DeliveryMode::Persistent,
            ),
            exchange: '',
            routingKey: $realQueue,
        );

        $this->deliveryOf($job)->nack(requeue: false);
    }

    #[\Override]
    public function fail(QueuedJob $job): void
    {
        $this->deliveryOf($job)->nack(requeue: false);
    }

    private function getFrom(string $queue): ?QueuedJob
    {
        $realQueue = $this->realQueue($queue);
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
     * Takes the raw body and headers rather than a DeliveryMessage, so it
     * is testable against a hand-built header array with no broker.
     *
     * Every field goes through a QueueContract decode helper: a
     * non-Kinetis publisher can put anything in an AMQP field table, and
     * a `(int)` cast would read a non-numeric header as 0. A missing
     * attempts header is not corruption — push() never writes one, only
     * release() does, so a first delivery has none and `?? 0` reads that
     * as zero completed attempts. maxAttempts is likewise only written
     * when push() got a non-null value. The PHP_INT_MAX - 1 ceiling keeps
     * the `+ 1` below from overflowing into a float.
     *
     * @param array<string, mixed> $headers
     */
    private static function buildQueuedJob(string $queue, mixed $handle, string $body, array $headers): QueuedJob
    {
        $decoded = QueueContract::storedJsonArray($body, 'body');

        $class = QueueContract::storedClass($decoded['class'] ?? null);
        $args = QueueContract::storedArgs($decoded['args'] ?? null);

        $rawAttempts = $headers[self::ATTEMPTS_HEADER] ?? 0;
        $completedAttempts = QueueContract::storedInt($rawAttempts, self::ATTEMPTS_HEADER, 0, PHP_INT_MAX - 1);

        $maxAttempts = QueueContract::storedNullableInt(
            $headers[self::MAX_ATTEMPTS_HEADER] ?? null,
            self::MAX_ATTEMPTS_HEADER,
            0,
        );

        $metadata = QueueContract::storedMetadata($headers[self::METADATA_HEADER] ?? null);

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
     * queueDeclare() answers with the queue's current message count, so
     * the declare this backend performs on first touch doubles as the
     * count — no separate management-API call, and no passive declare,
     * which would close the channel for a delay tier no process has
     * created yet. A job waiting out its delay sits in one of those
     * tiers and is outstanding here the same as on every other backend,
     * so every tier is counted, whichever process parked the job in it.
     *
     * Each queue is its own read, with no snapshot across them, so a
     * message dead-lettering while the reads run lands on either side of
     * one: leaving tier 0 for the real queue, read last, it is counted
     * twice, and dropping into a tier already read it is counted in
     * neither. QueueInterface's own docblock says what the result is for
     * — a monitoring signal, not a value to branch on.
     *
     * Messages already delivered to a consumer and not yet acked are
     * excluded by the broker's own count, matching the reserved/
     * processing exclusion elsewhere.
     */
    #[\Override]
    public function size(string $queue = 'default'): int
    {
        $name = $this->realQueue($queue);
        $waiting = 0;

        foreach (DelayLadder::tiers() as $tier) {
            $waiting += $this->declareTierQueue($name, $tier)->messages;
        }

        return $waiting + $this->declareRealQueue($name)->messages;
    }

    /**
     * Purging runs from the top tier down and finishes with the real
     * queue, the same direction a delayed message travels, so a message
     * moving between tiers during the sweep lands in one not yet purged
     * rather than behind it. The purges are still separate operations
     * against a live broker: a message crossing between two of them, or
     * a job pushed while the sweep runs, can outlive it. The returned
     * count is what the broker reported removing.
     */
    #[\Override]
    public function clear(string $queue = 'default'): int
    {
        $name = $this->realQueue($queue);
        $removed = 0;

        foreach (array_reverse(DelayLadder::tiers()) as $tier) {
            $removed += $this->channel()->queuePurge($this->ensureTierQueueDeclared($name, $tier));
        }

        $this->ensureDeclared($name);

        return $removed + $this->channel()->queuePurge($name);
    }

    /**
     * Confirm mode is armed once, with the channel itself, so every
     * publish through it has an acknowledgement to wait for.
     */
    private function channel(): Channel
    {
        if ($this->channel !== null) {
            return $this->channel;
        }

        $channel = $this->client->channel();
        $channel->confirmSelect();

        return $this->channel = $channel;
    }

    /**
     * @throws PublishNotConfirmedException
     */
    private function publishConfirmed(Message $message, string $exchange, string $routingKey): void
    {
        $confirmation = $this->channel()->publish(
            $message,
            exchange: $exchange,
            routingKey: $routingKey,
            mandatory: true,
        );

        self::assertConfirmed($confirmation, $exchange !== '' ? $exchange : $routingKey);
    }

    /**
     * The one place a publish becomes a fact rather than a written
     * frame. Every outcome other than an acknowledgement throws, so a
     * caller's next statement — release()'s nack, in the case that
     * matters — only runs against a message RabbitMQ has taken
     * responsibility for.
     *
     * @throws PublishNotConfirmedException
     */
    private static function assertConfirmed(?PublishConfirmation $confirmation, string $target): void
    {
        if ($confirmation === null) {
            throw PublishNotConfirmedException::unconfirmable($target);
        }

        $result = $confirmation->await();

        if ($result !== PublishResult::Acked) {
            throw PublishNotConfirmedException::answered($target, $result);
        }
    }

    /**
     * The broker-side name a logical queue maps to: a $queueNamePrefix
     * validated at construction, and the name itself validated right
     * here. Every broker call this result reaches wants a
     * non-empty-string, and asserting in the scope that builds the name
     * is what proves it one — PHPStan carries no caller's own validation
     * across the concatenation. QueueContract's assertions are pure, so
     * re-checking a name a public entry point has already validated
     * costs nothing and keeps the proof local to the name it applies to.
     *
     * @return non-empty-string
     */
    private function realQueue(string $queue): string
    {
        QueueContract::assertValidQueueName($queue);

        return $this->queueNamePrefix . $queue;
    }

    /**
     * @param non-empty-string $queue
     */
    private function ensureDeclared(string $queue): void
    {
        if (isset($this->declaredQueues[$queue])) {
            return;
        }

        $this->declareRealQueue($queue);
    }

    /**
     * @param non-empty-string $queue
     */
    private function declareRealQueue(string $queue): AmqpQueue
    {
        $declared = $this->channel()->queueDeclare($queue, durable: true);
        $this->declaredQueues[$queue] = true;

        return $declared;
    }

    /**
     * Declares the delay tiers up to $upToTier, and the exchanges and
     * bindings routing a message down through them — see DelayLadder for
     * what that topology is and why. Tiers already declared by this
     * instance are skipped, and a delay needing a higher tier than any
     * before it extends the ladder upward from there.
     *
     * Declaring runs upward so that each tier's exchange is bound to one
     * that already exists.
     *
     * @param non-empty-string $realQueue
     * @param int<0, DelayLadder::TOP_TIER> $upToTier
     */
    private function ensureLadderDeclared(string $realQueue, int $upToTier): void
    {
        $this->ensureDeclared($realQueue);

        $declaredUpTo = $this->declaredLadders[$realQueue] ?? -1;

        if ($declaredUpTo >= $upToTier) {
            return;
        }

        for ($tier = $declaredUpTo + 1; $tier <= $upToTier; ++$tier) {
            $exchange = DelayLadder::exchange($realQueue, $tier);
            $this->channel()->exchangeDeclare($exchange, 'topic', durable: true);

            $this->channel()->queueBind(
                $this->ensureTierQueueDeclared($realQueue, $tier),
                $exchange,
                DelayLadder::bindingKey($tier, set: true),
            );

            $unsetBinding = DelayLadder::bindingKey($tier, set: false);

            if ($tier === 0) {
                $this->channel()->queueBind($realQueue, $exchange, $unsetBinding);
            } else {
                $this->channel()->exchangeBind(DelayLadder::exchange($realQueue, $tier - 1), $exchange, $unsetBinding);
            }
        }

        $this->declaredLadders[$realQueue] = $upToTier;
    }

    /**
     * @param non-empty-string $realQueue
     * @param int<0, DelayLadder::TOP_TIER> $tier
     * @return non-empty-string
     */
    private function ensureTierQueueDeclared(string $realQueue, int $tier): string
    {
        $name = DelayLadder::queue($realQueue, $tier);

        if (isset($this->declaredQueues[$name])) {
            return $name;
        }

        $this->declareTierQueue($realQueue, $tier);

        return $name;
    }

    /**
     * A tier holds a message for its own whole TTL and then hands it to
     * the tier below — or, from tier 0, straight back to the real queue
     * over the default exchange, which is the one hop no exchange of this
     * ladder can route, since the bit it would ask about is the one the
     * message has just finished paying.
     *
     * @param non-empty-string $realQueue
     * @param int<0, DelayLadder::TOP_TIER> $tier
     */
    private function declareTierQueue(string $realQueue, int $tier): AmqpQueue
    {
        $name = DelayLadder::queue($realQueue, $tier);

        $arguments = ['x-message-ttl' => DelayLadder::ttlMilliseconds($tier)];

        if ($tier === 0) {
            $arguments['x-dead-letter-exchange'] = '';
            $arguments['x-dead-letter-routing-key'] = $realQueue;
        } else {
            $arguments['x-dead-letter-exchange'] = DelayLadder::exchange($realQueue, $tier - 1);
        }

        $declared = $this->channel()->queueDeclare($name, durable: true, arguments: $arguments);
        $this->declaredQueues[$name] = true;

        return $declared;
    }
}
