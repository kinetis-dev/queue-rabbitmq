<?php

declare(strict_types=1);

namespace Kinetis\QueueRabbitMq\Tests;

use Kinetis\Queue\Exception\InvalidDelaySecondsException;
use Kinetis\Queue\Exception\InvalidMaxAttemptsException;
use Kinetis\Queue\Exception\InvalidQueueNameException;
use Kinetis\Queue\Exception\MalformedQueuedJobDataException;
use Kinetis\Queue\Job;
use Kinetis\QueueRabbitMq\RabbitMqQueue;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use Thesis\Amqp\Client;
use Thesis\Amqp\Config as AmqpConfig;

/**
 * Queue-name validation only — RabbitMqQueue's own backend-specific
 * correctness (a real dead-letter-exchange delay, priority cycling, a
 * real ack/nack round trip) is deliberately never unit-tested against a
 * fake, matching this package's established "swap the storage, not the
 * whole system, and don't fake what a real backend has to prove"
 * discipline — real-backend verification lives in tests-integration/
 * against a real broker instead. This one check is pure PHP validation
 * that throws before the channel is ever touched, so a real broker has
 * nothing to prove that a fast unit test can't already prove faster.
 *
 * Thesis\Amqp\Client's own constructor only builds its internal
 * connection/channel factories — confirmed by reading its source — and
 * never opens a real socket; the connection is established lazily,
 * inside Sync\Once, on the first call that actually needs one
 * (channel()/connect()). A Client pointed at an unreachable broker is
 * therefore safe to construct and pass to RabbitMqQueue here, since
 * size()/clear() both validate the queue name before ever calling
 * channel().
 */
final class RabbitMqQueueTest extends TestCase
{
    private function neverConnectedQueue(): RabbitMqQueue
    {
        return new RabbitMqQueue(new Client(AmqpConfig::fromURI('amqp://guest:guest@localhost:1/')));
    }

    public function test_size_rejects_an_empty_queue_name_before_ever_touching_the_channel(): void
    {
        $queue = $this->neverConnectedQueue();

        $this->expectException(InvalidQueueNameException::class);
        $queue->size('');
    }

    public function test_size_rejects_a_malformed_queue_name_before_ever_touching_the_channel(): void
    {
        $queue = $this->neverConnectedQueue();

        $this->expectException(InvalidQueueNameException::class);
        $queue->size('has spaces');
    }

    public function test_clear_rejects_an_empty_queue_name_before_ever_touching_the_channel(): void
    {
        $queue = $this->neverConnectedQueue();

        $this->expectException(InvalidQueueNameException::class);
        $queue->clear('');
    }

    public function test_push_rejects_a_negative_delay_before_ever_touching_the_channel(): void
    {
        $queue = $this->neverConnectedQueue();

        $this->expectException(InvalidDelaySecondsException::class);
        $queue->push(new class implements Job {}, delaySeconds: -1);
    }

    public function test_push_rejects_a_negative_max_attempts_before_ever_touching_the_channel(): void
    {
        $queue = $this->neverConnectedQueue();

        $this->expectException(InvalidMaxAttemptsException::class);
        $queue->push(new class implements Job {}, maxAttempts: -1);
    }

    /**
     * buildQueuedJob() is where a corrupted AMQP header's malformed value
     * is actually caught — proven directly with a hand-built headers
     * array (no real broker round trip needed, since this method was
     * extracted specifically to make that possible), so the wiring
     * between it and QueueContract::coerceStoredInteger() is exercised
     * too, not just coerceStoredInteger()'s own unit-level behavior.
     */
    public function test_build_queued_job_rejects_a_non_numeric_stored_attempts_header(): void
    {
        $buildQueuedJob = new ReflectionMethod(RabbitMqQueue::class, 'buildQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"attempts"');
        $buildQueuedJob->invoke(
            null,
            'default',
            'opaque-handle',
            '{"class":"Fixture\\\\Job","args":[]}',
            ['attempts' => 'garbage'],
        );
    }

    public function test_build_queued_job_rejects_a_non_numeric_stored_max_attempts_header(): void
    {
        $buildQueuedJob = new ReflectionMethod(RabbitMqQueue::class, 'buildQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"maxAttempts"');
        $buildQueuedJob->invoke(
            null,
            'default',
            'opaque-handle',
            '{"class":"Fixture\\\\Job","args":[]}',
            ['maxAttempts' => 'garbage'],
        );
    }

    /**
     * The reviewer's own reported overflow gap, at the real decode level:
     * a stored completed-attempts count of exactly PHP_INT_MAX is
     * syntactically a perfectly valid integer — coerceStoredInteger()
     * alone would accept it — but buildQueuedJob()'s own `+ 1` would
     * silently overflow it to a float, which would then fail QueuedJob's
     * strictly-typed constructor with a confusing TypeError. This proves
     * the real, wired decode path rejects it cleanly instead, via
     * QueueContract::coerceStoredCompletedAttempts() — as the native
     * typed int form a real AMQP field table (the header shape this
     * class itself writes) actually carries.
     */
    public function test_build_queued_job_rejects_a_stored_attempts_header_of_php_int_max(): void
    {
        $buildQueuedJob = new ReflectionMethod(RabbitMqQueue::class, 'buildQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('PHP_INT_MAX');
        $buildQueuedJob->invoke(
            null,
            'default',
            'opaque-handle',
            '{"class":"Fixture\\\\Job","args":[]}',
            ['attempts' => PHP_INT_MAX],
        );
    }

    /**
     * @return list<array{mixed}>
     */
    public static function malformedAttemptsHeaders(): array
    {
        return [
            'a float' => [5.0],
            'a bool' => [true],
            'an array' => [[5]],
        ];
    }

    #[DataProvider('malformedAttemptsHeaders')]
    public function test_build_queued_job_rejects_a_non_integer_attempts_header_of_any_type(mixed $raw): void
    {
        $buildQueuedJob = new ReflectionMethod(RabbitMqQueue::class, 'buildQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $buildQueuedJob->invoke(
            null,
            'default',
            'opaque-handle',
            '{"class":"Fixture\\\\Job","args":[]}',
            ['attempts' => $raw],
        );
    }

    public function test_build_queued_job_rejects_a_body_that_is_not_valid_json(): void
    {
        $buildQueuedJob = new ReflectionMethod(RabbitMqQueue::class, 'buildQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"body"');
        $buildQueuedJob->invoke(null, 'default', 'opaque-handle', '{not valid json', []);
    }

    public function test_build_queued_job_rejects_a_body_missing_the_class_field(): void
    {
        $buildQueuedJob = new ReflectionMethod(RabbitMqQueue::class, 'buildQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"class"');
        $buildQueuedJob->invoke(null, 'default', 'opaque-handle', '{"args":[]}', []);
    }

    public function test_build_queued_job_rejects_a_body_whose_args_field_is_not_an_array(): void
    {
        $buildQueuedJob = new ReflectionMethod(RabbitMqQueue::class, 'buildQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"args"');
        $buildQueuedJob->invoke(null, 'default', 'opaque-handle', '{"class":"Fixture\\\\Job","args":"not an array"}', []);
    }

    /**
     * A JSON *list* args value ("args": ["value"], no object keys) is a
     * real, distinct malformed shape from "not an array at all" above —
     * is_array() alone would have accepted it. Confirming it throws
     * MalformedQueuedJobDataException here, from buildQueuedJob() itself
     * (the exact function getFrom() wraps in
     * QueueContract::settleIfMalformed()), is what proves this reaches
     * the settle-and-remove path rather than QueueWorker's ordinary
     * job-execution failure handling.
     */
    public function test_build_queued_job_rejects_a_body_whose_args_field_is_a_json_list(): void
    {
        $buildQueuedJob = new ReflectionMethod(RabbitMqQueue::class, 'buildQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"args"');
        $buildQueuedJob->invoke(null, 'default', 'opaque-handle', '{"class":"Fixture\\\\Job","args":["positional value"]}', []);
    }

    public function test_build_queued_job_rejects_a_metadata_header_that_is_not_a_string_to_string_map(): void
    {
        $buildQueuedJob = new ReflectionMethod(RabbitMqQueue::class, 'buildQueuedJob');

        $this->expectException(MalformedQueuedJobDataException::class);
        $this->expectExceptionMessage('"metadata"');
        $buildQueuedJob->invoke(
            null,
            'default',
            'opaque-handle',
            '{"class":"Fixture\\\\Job","args":[]}',
            ['metadata' => '["not","a","map"]'],
        );
    }
}
