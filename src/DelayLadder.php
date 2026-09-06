<?php

declare(strict_types=1);

namespace Kinetis\QueueRabbitMq;

use InvalidArgumentException;

/**
 * The topology a delayed push() travels through, and the arithmetic
 * deciding which part of it a given delay uses.
 *
 * AMQP 0-9-1 has no per-message delay. A queue's TTL plus
 * `x-dead-letter-exchange` hold a message and then move it on, but one
 * TTL applies to the whole queue and a queue expires from the head, so a
 * single holding queue carrying per-message `expiration` values holds a
 * message long past its own delay: one asking for an hour, sitting at the
 * head, keeps a three-second message behind it waiting the full hour.
 *
 * So a delay is spent across a ladder of holding queues, tier $i holding
 * a message for 2^$i seconds, and a delay is the binary sum of the tiers
 * it sets — 3600 seconds visits tiers 11, 10, 9 and 4. Every message in a
 * tier owes the same wait, so FIFO order and expiry order are the same
 * order and nothing can be held up by a message owing longer.
 *
 * A TTL is a floor, not a schedule: a job is available no sooner than its
 * delay, and expiry work and routing can put it later. What the ladder
 * buys is independence between delays, not delivery at a wall-clock
 * instant.
 *
 * Routing between tiers is the broker's work — nothing polls, and no
 * process holds state between hops. The delay's bit pattern travels as
 * the routing key (TIER_COUNT words of `0`/`1`, most significant first),
 * and each tier owns one topic exchange asking whether bit $i is set. A
 * set bit binds to that tier's holding queue, which dead-letters into the
 * next exchange down (tier 0 into the real queue); a clear bit binds
 * straight to the next exchange down. Dead-lettering preserves the
 * routing key, so the same pattern answers every tier's question, and a
 * message only ever moves toward lower tiers.
 *
 * TOP_TIER sets MAX_DELAY_SECONDS: `Thesis\Amqp` encodes a field-table
 * integer as signed 32-bit, and a tier's `x-message-ttl` is one of those,
 * so the largest TTL a tier can carry is 2^31 - 1 milliseconds and 2^21
 * seconds is the largest whole power of two under it. push() rejects a
 * longer delay, naming the ceiling, rather than publishing a message the
 * ladder cannot hold as long as asked. The ceiling is this client's
 * encoding, not a RabbitMQ limit.
 *
 * A ladder name can never collide with a real queue: QueueContract's
 * name grammar allows no `.`, and every name here carries one.
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
