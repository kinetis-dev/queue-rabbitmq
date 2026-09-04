<?php

declare(strict_types=1);

namespace Kinetis\QueueRabbitMq;

use InvalidArgumentException;

/**
 * The topology a delayed push() travels through, and the pure arithmetic
 * deciding which part of it a given delay uses.
 *
 * AMQP 0-9-1 has no per-message delay. RabbitMQ can hold a message for a
 * while and then move it somewhere else — a queue's TTL plus
 * `x-dead-letter-exchange` do exactly that — but one TTL applies to a
 * whole queue, and a queue expires its messages from the head. A single
 * holding queue carrying a per-message `expiration` therefore holds a
 * message long past its own delay: one asking for an hour, sitting at
 * the head, keeps a message behind it asking for three seconds waiting
 * the full hour. Uniform per-queue TTL removes that, because FIFO order
 * and expiry order are then the same order — whatever entered first also
 * comes due first, and nothing behind it can be due sooner.
 *
 * So a delay is spent across a ladder of holding queues, tier $i holding
 * a message for 2^$i seconds. A delay is the binary sum of the tiers it
 * sets: 3600 seconds is tiers 11, 10, 9 and 4 (2048 + 1024 + 512 + 16),
 * visiting four queues whose TTLs add up to the requested delay. Every
 * message in a tier owes the same wait, so none of them can be held up
 * by one owing longer.
 *
 * A TTL is a floor, not a schedule. It says when the broker may move a
 * message on, so a job is available no sooner than its delay; expiry
 * work, each hop's own routing, and whatever else the broker has to do
 * can put it later. What the ladder buys is the independence — a short
 * delay pushed behind a long one waits its own wait rather than the
 * long one's — not delivery at a wall-clock instant.
 *
 * Routing between tiers is the broker's own work, with nothing polling
 * and no process holding state between hops. The delay's bit pattern
 * travels as the routing key — TIER_COUNT words of `0`/`1`, most
 * significant first — and each tier owns one topic exchange asking a
 * single question about it: is bit $i set? A set bit binds to that
 * tier's holding queue, which dead-letters into the next exchange down
 * (tier 0 dead-letters to the real queue itself); a clear bit binds
 * straight to the next exchange down, an exchange-to-exchange binding the
 * message passes through without being queued. Dead-lettering preserves
 * the routing key, so the same bit pattern answers every tier's question
 * on the way down. A message enters at the exchange of its highest set
 * bit and leaves from tier 0's dead-letter route, so it only ever moves
 * toward lower tiers and can never re-enter one it has left.
 *
 * MAX_DELAY_SECONDS is the whole ladder spent at once, and TOP_TIER sets
 * it: `Thesis\Amqp` encodes every integer in an AMQP field table as a
 * signed 32-bit value, and a tier's `x-message-ttl` is one of those
 * integers — RabbitMQ holds a message for whatever millisecond figure
 * reaches it. So the largest TTL a tier can carry is 2^31 - 1
 * milliseconds, and 2^21 seconds is the largest whole power of two
 * fitting under it. A tier of 2^22 seconds would go out as a negative
 * TTL, so push() rejects a longer delay itself, naming the ceiling,
 * rather than publishing a message the ladder cannot hold as long as it
 * was asked to. The ceiling is what this client's encoding can express,
 * not a delay limit RabbitMQ itself sets — the broker's own `x-message-ttl`
 * takes an unsigned 32-bit millisecond value, one tier further up.
 *
 * A ladder name can never collide with a real queue: `QueueContract`'s
 * queue-name grammar allows no `.` at all, in either a queue name or a
 * `$queueNamePrefix`, and every name here carries one.
 *
 * @internal RabbitMqQueue's own topology. Nothing outside this package
 *     should name these queues, exchanges or tiers; the delay contract a
 *     caller can rely on is QueueInterface::push()'s own.
 */
final class DelayLadder
{
    /**
     * Tier $i holds a message for 2^$i seconds; 2^21 seconds is
     * 2_097_152_000 milliseconds, the largest whole power of two under
     * the signed 32-bit ceiling `Thesis\Amqp` encodes a field-table
     * integer within.
     */
    public const int TOP_TIER = 21;

    /**
     * Every tier set at once — the longest delay this ladder can spend.
     */
    public const int MAX_DELAY_SECONDS = (1 << (self::TOP_TIER + 1)) - 1;

    private const int TIER_COUNT = self::TOP_TIER + 1;

    // Never instantiated — every method here is static.
    private function __construct() {}

    /**
     * @throws InvalidArgumentException
     */
    public static function assertSupportedDelay(int $delaySeconds): void
    {
        if ($delaySeconds > self::MAX_DELAY_SECONDS) {
            throw new InvalidArgumentException(
                'This backend cannot delay a message by more than ' . self::MAX_DELAY_SECONDS
                . " seconds (requested {$delaySeconds}): the AMQP client encodes a queue's x-message-ttl "
                . 'as a signed 32-bit millisecond value, which caps the longest tier it can declare.',
            );
        }
    }

    /**
     * The tier a delay enters the ladder at — its highest set bit, and so
     * the only tier that can be its first hop.
     *
     * @param positive-int $delaySeconds
     * @return int<0, self::TOP_TIER>
     */
    public static function entryTier(int $delaySeconds): int
    {
        self::assertSupportedDelay($delaySeconds);

        /** @var int<0, self::TOP_TIER> */
        return \strlen(decbin($delaySeconds)) - 1;
    }

    /**
     * @param int<0, self::TOP_TIER> $tier
     * @return positive-int
     */
    public static function tierSeconds(int $tier): int
    {
        /** @var positive-int */
        return 1 << $tier;
    }

    /**
     * @param int<0, self::TOP_TIER> $tier
     * @return positive-int
     */
    public static function ttlMilliseconds(int $tier): int
    {
        return self::tierSeconds($tier) * 1000;
    }

    /**
     * Every tier, lowest first.
     *
     * @return list<int<0, self::TOP_TIER>>
     */
    public static function tiers(): array
    {
        /** @var list<int<0, self::TOP_TIER>> */
        return range(0, self::TOP_TIER);
    }

    /**
     * The delay's bit pattern as a routing key: TIER_COUNT single-digit
     * words, most significant bit first, so every tier's binding key can
     * name its own bit by a fixed word position.
     *
     * @param positive-int $delaySeconds
     * @return non-empty-string
     */
    public static function routingKey(int $delaySeconds): string
    {
        self::assertSupportedDelay($delaySeconds);

        return implode('.', str_split(str_pad(decbin($delaySeconds), self::TIER_COUNT, '0', STR_PAD_LEFT)));
    }

    /**
     * The binding key matching every delay whose bit $tier is (or is not)
     * set — `*` for each higher bit, the bit itself, then `#` for the
     * lower bits the tier below asks about instead.
     *
     * @param int<0, self::TOP_TIER> $tier
     * @return non-empty-string
     */
    public static function bindingKey(int $tier, bool $set): string
    {
        $bit = $set ? '1' : '0';

        return str_repeat('*.', self::TOP_TIER - $tier) . $bit . ($tier > 0 ? '.#' : '');
    }

    /**
     * @param non-empty-string $realQueue
     * @param int<0, self::TOP_TIER> $tier
     * @return non-empty-string
     */
    public static function queue(string $realQueue, int $tier): string
    {
        return $realQueue . '.delay.' . self::tierSeconds($tier) . 's';
    }

    /**
     * The topic exchange asking whether bit $tier is set — where a delay
     * entering at $tier is published, and where the tier above it
     * dead-letters into.
     *
     * @param non-empty-string $realQueue
     * @param int<0, self::TOP_TIER> $tier
     * @return non-empty-string
     */
    public static function exchange(string $realQueue, int $tier): string
    {
        return self::queue($realQueue, $tier) . '.in';
    }
}
