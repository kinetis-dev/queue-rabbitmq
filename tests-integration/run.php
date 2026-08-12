<?php

declare(strict_types=1);

/**
 * Real-backend regression coverage for RabbitMqQueue — push/pop/ack/
 * release/fail, maxAttempts round-tripping through message headers
 * (including the no-maxAttempts case reading back null), priority-queue
 * fallthrough across two real queues, and a real (not just configured)
 * delay via the dead-letter-exchange mechanism — against a real RabbitMQ
 * broker.
 *
 * Deliberately its own standalone script, never wired to run in the same
 * process as anything else: RabbitMqQueue's own docblock discloses that
 * once its connection opens, Kinetis\Async\concurrently() can never be
 * called again anywhere in that process. A dedicated CI job (its own,
 * separate runner process) doesn't hit this at all — the limitation is
 * about sharing one process, not about RabbitMQ being reachable from CI.
 */

require __DIR__ . '/../vendor/autoload.php';

use Kinetis\Config\Config;
use Kinetis\Queue\Job;
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

$start = microtime(true);
$queue->push(new RabbitMqIntegrationTestJob('delayed'), delaySeconds: 3);
check('a delayed job is not visible before its delay elapses', $queue->pop(timeoutSeconds: 1) === null);

$popped = $queue->pop(timeoutSeconds: 10);
$elapsed = microtime(true) - $start;
check('the delayed job becomes visible after its delay elapses', $popped?->args['message'] === 'delayed');
check('the delay was genuinely honored (>= 3s elapsed)', $elapsed >= 2.5);
$queue->ack($popped);

echo "ALL CHECKS PASSED\n";
