<?php

declare(strict_types=1);

namespace Kinetis\QueueRabbitMq\Tests;

use InvalidArgumentException;
use Kinetis\QueueRabbitMq\DelayLadder;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * The delay ladder is pure arithmetic plus a set of AMQP names and
 * binding keys, so what a broker would do with them is reproducible here
 * without one: this file walks a delay down the ladder using a topic
 * matcher written to AMQP 0-9-1's own rules (`*` matches one word, `#`
 * matches zero or more), and checks the properties the delay contract
 * rests on. A real broker then confirms the same walk end to end, with
 * real timing, in tests-integration/.
 *
 * Two of those properties are what a later short delay not waiting
 * behind an earlier long one actually reduces to: every tier a delay
 * visits holds it for that tier's own fixed wait, and two delays sharing
 * a tier therefore wait the identical time in it. A queue whose messages
 * all owe the same wait expires them in arrival order, so nothing in it
 * can be due before the message ahead of it.
 */
final class DelayLadderTest extends TestCase
{
    /**
     * @return list<array{int}>
     */
    public static function delays(): array
    {
        return [
            [1],
            [2],
            [3],
            [4],
            [5],
            [7],
            [30],
            [59],
            [60],
            [300],
            [3600],
            [86400],
            [604800],
            [DelayLadder::MAX_DELAY_SECONDS - 1],
            [DelayLadder::MAX_DELAY_SECONDS],
        ];
    }

    /**
     * The tiers a delay is routed through add up to that delay, so a job
     * comes due no sooner than it asked to wait. When the broker then
     * moves it on is the broker's own business — see DelayLadder.
     */
    #[DataProvider('delays')]
    public function test_the_tiers_a_delay_visits_add_up_to_that_delay(int $delaySeconds): void
    {
        $held = 0;

        foreach (self::walk($delaySeconds) as $tier) {
            $held += DelayLadder::tierSeconds($tier);
        }

        self::assertSame($delaySeconds, $held);
    }

    /**
     * A topic exchange delivers one copy per matching binding, so a delay
     * matching both of a tier's bindings would be duplicated — one copy
     * held, one falling straight through — and a delay matching neither
     * would be dropped.
     */
    #[DataProvider('delays')]
    public function test_every_tier_routes_a_delay_exactly_one_way(int $delaySeconds): void
    {
        $routingKey = DelayLadder::routingKey($delaySeconds);

        foreach (DelayLadder::tiers() as $tier) {
            $matches = (int) self::routes(DelayLadder::bindingKey($tier, set: true), $routingKey)
                + (int) self::routes(DelayLadder::bindingKey($tier, set: false), $routingKey);

            self::assertSame(1, $matches, "tier {$tier} matched {$matches} bindings for a {$delaySeconds}s delay");
        }
    }

    /**
     * A short delay pushed after a long one shares whatever tiers the two
     * have in common, and waits the same fixed time in each of them — the
     * property a single holding queue with per-message expiration cannot
     * offer, and the reason the long delay cannot hold the short one up.
     */
    public function test_two_delays_sharing_a_tier_wait_the_same_time_in_it(): void
    {
        $long = self::walk(600);
        $short = self::walk(24);
        $shared = array_intersect($long, $short);

        self::assertNotSame([], $shared, 'the two delays share no tier at all');

        foreach ($shared as $tier) {
            self::assertSame(DelayLadder::tierSeconds($tier) * 1000, DelayLadder::ttlMilliseconds($tier));
        }

        self::assertSame(24, array_sum(array_map(DelayLadder::tierSeconds(...), $short)));
    }

    /**
     * Each tier of the ladder is entered at most once, and always from a
     * higher one — the message can only move downward, so no tier can
     * hold it twice and no dead-letter cycle can form.
     */
    #[DataProvider('delays')]
    public function test_a_delay_only_ever_moves_toward_lower_tiers(int $delaySeconds): void
    {
        $visited = self::walk($delaySeconds);
        $descending = $visited;
        rsort($descending);

        self::assertSame($descending, $visited);
        self::assertSame(array_values(array_unique($visited)), $visited);
    }

    #[DataProvider('delays')]
    public function test_a_delay_enters_at_its_own_highest_tier(int $delaySeconds): void
    {
        $visited = self::walk($delaySeconds);

        self::assertSame(DelayLadder::entryTier($delaySeconds), $visited[0]);
    }

    public function test_the_routing_key_is_one_word_per_tier_most_significant_first(): void
    {
        $words = explode('.', DelayLadder::routingKey(5));

        self::assertCount(DelayLadder::TOP_TIER + 1, $words);
        self::assertSame(['1', '0', '1'], \array_slice($words, -3));
        self::assertSame(['0', '0', '0'], \array_slice($words, 0, 3));
    }

    /**
     * The ceiling exists because the client encodes a tier's TTL as a
     * signed 32-bit millisecond value — one more tier would go out
     * negative.
     */
    public function test_the_top_tier_ttl_fits_a_signed_32_bit_millisecond_field(): void
    {
        self::assertLessThanOrEqual(2 ** 31 - 1, DelayLadder::ttlMilliseconds(DelayLadder::TOP_TIER));
        self::assertGreaterThan(2 ** 31 - 1, DelayLadder::ttlMilliseconds(DelayLadder::TOP_TIER) * 2);
        self::assertSame(2 ** (DelayLadder::TOP_TIER + 1) - 1, DelayLadder::MAX_DELAY_SECONDS);
    }

    public function test_a_delay_beyond_the_ceiling_is_rejected_by_name(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage((string) DelayLadder::MAX_DELAY_SECONDS);
        DelayLadder::assertSupportedDelay(DelayLadder::MAX_DELAY_SECONDS + 1);
    }

    public function test_the_ceiling_itself_is_accepted(): void
    {
        DelayLadder::assertSupportedDelay(DelayLadder::MAX_DELAY_SECONDS);
        DelayLadder::assertSupportedDelay(0);

        self::assertSame(DelayLadder::TOP_TIER, DelayLadder::entryTier(DelayLadder::MAX_DELAY_SECONDS));
    }

    /**
     * Every ladder name carries a `.`, which QueueContract's own
     * queue-name grammar rejects — so no logical queue name, prefixed or
     * not, can ever resolve to one of these.
     */
    public function test_ladder_names_cannot_collide_with_a_logical_queue_name(): void
    {
        self::assertSame('myapp-default.delay.8s', DelayLadder::queue('myapp-default', 3));
        self::assertSame('myapp-default.delay.8s.in', DelayLadder::exchange('myapp-default', 3));
        self::assertSame('myapp-default.delay.1s', DelayLadder::queue('myapp-default', 0));
        self::assertSame('myapp-default.delay.2097152s', DelayLadder::queue('myapp-default', DelayLadder::TOP_TIER));
    }

    /**
     * The tiers a delay is held in, in the order the broker would move it
     * through them: start at the exchange of its highest set bit, ask
     * each tier's two bindings which way this routing key goes, and step
     * down until tier 0 hands it to the real queue.
     *
     * @return list<int>
     */
    private static function walk(int $delaySeconds): array
    {
        $routingKey = DelayLadder::routingKey($delaySeconds);
        $visited = [];

        for ($tier = DelayLadder::entryTier($delaySeconds); $tier >= 0; --$tier) {
            if (self::routes(DelayLadder::bindingKey($tier, set: true), $routingKey)) {
                $visited[] = $tier;
            }
        }

        return $visited;
    }

    /**
     * AMQP 0-9-1 topic matching: `*` matches exactly one word, `#`
     * matches zero or more, and every other word matches itself.
     */
    private static function routes(string $bindingKey, string $routingKey): bool
    {
        return self::matchWords(explode('.', $bindingKey), explode('.', $routingKey));
    }

    /**
     * @param list<string> $pattern
     * @param list<string> $words
     */
    private static function matchWords(array $pattern, array $words): bool
    {
        if ($pattern === []) {
            return $words === [];
        }

        $head = array_shift($pattern);

        if ($head === '#') {
            for ($skipped = 0; $skipped <= \count($words); ++$skipped) {
                if (self::matchWords($pattern, \array_slice($words, $skipped))) {
                    return true;
                }
            }

            return false;
        }

        if ($words === []) {
            return false;
        }

        $word = array_shift($words);

        return ($head === '*' || $head === $word) && self::matchWords($pattern, $words);
    }
}
