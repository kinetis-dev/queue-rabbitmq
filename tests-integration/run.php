<?php

declare(strict_types=1);

/**
 * Real-backend regression coverage for RabbitMqQueue — push/pop/ack/
 * release/fail, maxAttempts round-tripping through message headers
 * (including the no-maxAttempts case reading back null), priority-queue
 * fallthrough across two real queues, real delays through the delay
 * ladder (including a short delay pushed behind a ten-minute one, and
 * size()/clear() reaching those from a connection that never pushed
 * them), release() refusing to settle a job the broker has not confirmed
 * the replacement for, and that Kinetis\Async\concurrently() still
 * composes correctly once this connection is open — against a real
 * RabbitMQ broker.
 */

require __DIR__ . '/../vendor/autoload.php';

use function Kinetis\Async\concurrently;
use Kinetis\Async\Timer;
use Kinetis\Config\Config;
use Kinetis\Queue\Exception\InvalidPopTimeoutException;
use Kinetis\Queue\Exception\InvalidQueueNameException;
use Kinetis\Queue\Job;
use Kinetis\Queue\QueuedJob;
use Kinetis\QueueRabbitMq\DelayLadder;
use Kinetis\QueueRabbitMq\Exception\PublishNotConfirmedException;
use Kinetis\QueueRabbitMq\RabbitMqClientFactory;
use Kinetis\QueueRabbitMq\RabbitMqQueue;

function check(string $label, bool $condition): void
{
    echo ($condition ? "OK   " : "FAIL ") . $label . "\n";

    if (!$condition) {
        exit(1);
    }
}

final readonly class RabbitMqIntegrationTestJob implements Job
{
    public function __construct(
        public string $message,
    ) {}

    public function handle(): void
    {
    }
}

$config = new Config([
    'QUEUE_RABBITMQ_URL' => getenv('RABBITMQ_URL') ?: 'amqp://guest:guest@127.0.0.1:5672/',
]);

$client = RabbitMqClientFactory::fromConfig($config);
$prefix = 'kinetis-integration-' . bin2hex(random_bytes(4)) . '-';
$queue = new RabbitMqQueue($client, queueNamePrefix: $prefix);

// --- push/pop/ack round trip ---

$queue->push(new RabbitMqIntegrationTestJob('hello'));
$popped = $queue->pop(timeoutSeconds: 5);
check('pop() returns the pushed job', $popped !== null);
check('job data round-trips correctly', $popped?->args['message'] === 'hello');
check('attempts is 1 on first pop', $popped?->attempts === 1);
check('maxAttempts reads back null when never given', $popped?->maxAttempts === null);

$queue->ack($popped);
check('nothing left after ack()', $queue->pop(timeoutSeconds: 1) === null);

// --- maxAttempts round-trips through a message header ---

$queue->push(new RabbitMqIntegrationTestJob('capped'), maxAttempts: 3);
$popped = $queue->pop(timeoutSeconds: 5);
check('maxAttempts round-trips through the header', $popped?->maxAttempts === 3);
$queue->fail($popped);
check('fail() removes the job permanently', $queue->pop(timeoutSeconds: 1) === null);

// --- release() increments attempts, carries maxAttempts and data forward ---

$queue->push(new RabbitMqIntegrationTestJob('retry-me'), maxAttempts: 5);
$popped = $queue->pop(timeoutSeconds: 5);
check('first attempt is 1', $popped?->attempts === 1);
$queue->release($popped);

$popped = $queue->pop(timeoutSeconds: 5);
check('release() increments the attempt count', $popped?->attempts === 2);
check('release() carries maxAttempts forward', $popped?->maxAttempts === 5);
check('release() carries the job data forward', $popped?->args['message'] === 'retry-me');
$queue->ack($popped);

// --- priority cycling across two real queues ---

$queue->push(new RabbitMqIntegrationTestJob('low-priority'), queue: 'default');
$queue->push(new RabbitMqIntegrationTestJob('high-priority'), queue: 'high');

$popped = $queue->pop(timeoutSeconds: 5, queues: ['high', 'default']);
check('the higher-priority queue is checked first', $popped?->args['message'] === 'high-priority');
$queue->ack($popped);

$popped = $queue->pop(timeoutSeconds: 5, queues: ['high', 'default']);
check('falls through to the next queue once the first is empty', $popped?->args['message'] === 'low-priority');
$queue->ack($popped);

// --- a real delay, not just a configured one ---
//
// Four seconds is one set bit with two clear ones under it, so the
// message leaves its holding queue and falls through both lower tiers'
// exchanges to reach the real queue — the routing path a delay ending in
// a set bit (the three-second one below) never takes.

$start = microtime(true);
$queue->push(new RabbitMqIntegrationTestJob('delayed'), delaySeconds: 4);
check('a delayed job is not visible before its delay elapses', $queue->pop(timeoutSeconds: 1) === null);

$popped = $queue->pop(timeoutSeconds: 10);
$elapsed = microtime(true) - $start;
check('the delayed job becomes visible after its delay elapses', $popped?->args['message'] === 'delayed');
check("it came due no sooner than its delay — {$elapsed}s elapsed", $elapsed >= 4.0);
$queue->ack($popped);

// --- a short delay is not held up by a longer one pushed before it ---
//
// The regression a single holding queue with per-message expiration
// cannot avoid: RabbitMQ expires a queue's messages from the head, so a
// ten-minute message sitting in front of a three-second one holds it for
// ten minutes. The ladder puts each of them in the tiers its own delay
// sets, every tier owing every message in it the same wait, so the short
// delay comes due while the long one is still parked. The upper bound
// below is what fails without that; the lower bound is the delay itself.
// The long job is left for clear() below.

$start = microtime(true);
$queue->push(new RabbitMqIntegrationTestJob('ten-minutes'), delaySeconds: 600, queue: 'blocked');
$queue->push(new RabbitMqIntegrationTestJob('three-seconds'), delaySeconds: 3, queue: 'blocked');

$popped = $queue->pop(timeoutSeconds: 30, queues: ['blocked']);
$elapsed = microtime(true) - $start;
check('the short delay comes due while the longer one is still waiting', $popped?->args['message'] === 'three-seconds');
check("it waited its own delay, not the longer one — {$elapsed}s elapsed", $elapsed >= 3.0 && $elapsed < 20.0);
$queue->ack($popped);

// --- size() and clear() see delayed jobs from a process that never pushed them ---
//
// A fresh RabbitMqQueue on its own connection knows nothing but the queue
// name — the same position `kinetis queue:stats` and `kinetis queue:clear`
// start from — so counting and purging a job still waiting out its delay
// has to work from that name alone.

$queue->push(new RabbitMqIntegrationTestJob('waiting'), delaySeconds: 600, queue: 'blocked');

$fresh = new RabbitMqQueue(RabbitMqClientFactory::fromConfig($config), queueNamePrefix: $prefix);
check('a fresh connection counts both jobs still waiting out their delays', $fresh->size('blocked') === 2);
check('clear() reports both of them removed', $fresh->clear('blocked') === 2);
check('nothing is left waiting afterward', $fresh->size('blocked') === 0);

// --- a delay longer than the ladder can hold is rejected, not weakened ---

try {
    $queue->push(new RabbitMqIntegrationTestJob('too-far-out'), delaySeconds: DelayLadder::MAX_DELAY_SECONDS + 1);
    check('a delay beyond the ladder ceiling is rejected', false);
} catch (InvalidArgumentException) {
    check('a delay beyond the ladder ceiling is rejected', true);
}

$queue->push(new RabbitMqIntegrationTestJob('as-far-out-as-it-goes'), delaySeconds: DelayLadder::MAX_DELAY_SECONDS, queue: 'ceiling');
check('the ceiling delay itself is accepted and held', $queue->size('ceiling') === 1);
check('and clears again', $queue->clear('ceiling') === 1);

// --- release() settles nothing the broker has not confirmed ---
//
// The replacement is published to a queue deleted out from under this
// connection, so the broker returns it as unroutable instead of
// acknowledging it. release() has to fail there with the original
// delivery still unacked: dropping the connection then hands the job
// back to the queue, on the same attempt it was popped on, rather than
// losing it to a publish that never landed.

$strandedClient = RabbitMqClientFactory::fromConfig($config);
$stranded = new RabbitMqQueue($strandedClient, queueNamePrefix: $prefix);

$stranded->push(new RabbitMqIntegrationTestJob('stranded'), queue: 'strays');
$reserved = $stranded->pop(timeoutSeconds: 5, queues: ['strays']);
check('the job to release is reserved', $reserved?->args['message'] === 'stranded');

// size() declares the queue and records it as declared on this instance,
// so release() below publishes to it without redeclaring it first.
$stranded->size('vanished');

$admin = RabbitMqClientFactory::fromConfig($config);
$admin->channel()->queueDelete($prefix . 'vanished');

$unpublishable = new QueuedJob(
    $reserved->class,
    $reserved->args,
    handle: $reserved->handle,
    queue: 'vanished',
    attempts: $reserved->attempts,
);

try {
    $stranded->release($unpublishable);
    check('release() fails when the broker does not confirm the replacement', false);
} catch (PublishNotConfirmedException) {
    check('release() fails when the broker does not confirm the replacement', true);
}

$strandedClient->disconnect();

$recovered = new RabbitMqQueue(RabbitMqClientFactory::fromConfig($config), queueNamePrefix: $prefix);
$redelivered = $recovered->pop(timeoutSeconds: 10, queues: ['strays']);
check('the original job survived the failed release()', $redelivered?->args['message'] === 'stranded');
check('and is still on the attempt it was popped on', $redelivered?->attempts === 1);
$recovered->ack($redelivered);

// --- immediate, non-blocking priority sweep ---
//
// basic.get is always immediate — there's no native blocking primitive
// to accidentally over-commit to — and pop() delegates the whole
// priority/timeout algorithm to the shared, tested PopSweep. This
// confirms a job in the last of several empty higher-priority queues is
// still found quickly, and every named queue gets declared (a real AMQP
// operation) along the way.
$start = microtime(true);
$queue->push(new RabbitMqIntegrationTestJob('found-immediately'), queue: 'lowest');
$found = $queue->pop(timeoutSeconds: 10, queues: ['empty-one', 'empty-two', 'empty-three', 'lowest']);
$elapsed = microtime(true) - $start;
check(
    'a job in the last of four queues, the first three empty, is still found',
    $found?->args['message'] === 'found-immediately',
);
check("found within a couple of real seconds, not the full 10s timeout — took {$elapsed}s", $elapsed < 3.0);
$queue->ack($found);

try {
    $queue->pop(timeoutSeconds: -1);
    check('a negative timeout is rejected', false);
} catch (InvalidPopTimeoutException) {
    check('a negative timeout is rejected', true);
}

try {
    $queue->pop(queues: ['default', '']);
    check('an empty queue name is rejected', false);
} catch (InvalidQueueNameException) {
    check('an empty queue name is rejected', true);
}

try {
    $queue->pop(queues: ['default', 'high', 'default']);
    check('a duplicate queue name is rejected', false);
} catch (InvalidQueueNameException) {
    check('a duplicate queue name is rejected', true);
}

check('an empty queue list returns null, not an error', $queue->pop(timeoutSeconds: 1, queues: []) === null);

// --- concurrently() still works after this connection has opened ---
//
// ConcurrentBatch parks on a targeted Revolt suspension resumed once its
// own tasks finish, so a still-open RabbitMQ channel's permanent
// background reader has nothing to keep concurrently() from returning.
// Two 50ms timer tasks through concurrently(), with $queue's connection
// already open from every push()/pop() call above.
$start = microtime(true);
$results = concurrently([
    static function (): int {
        Timer::delay(0.05);

        return 1;
    },
    static function (): int {
        Timer::delay(0.05);

        return 2;
    },
]);
$elapsed = microtime(true) - $start;
check('concurrently() still returns after this connection is open', $results === [1, 2]);
check('it returned quickly, not hung on the still-open connection', $elapsed < 1.0);

echo "ALL CHECKS PASSED\n";
