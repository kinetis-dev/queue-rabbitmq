<p align="center">
  <img src="logo.svg" alt="Kinetis" width="420">
</p>

<p align="center">
  <strong>kinetis/queue-rabbitmq</strong>
  <br>
  <strong>A Fiber-native, non-blocking RabbitMQ backend for kinetis/queue's <code>QueueInterface</code></strong>
</p>

<p align="center">
  <a href="https://packagist.org/packages/kinetis/queue-rabbitmq"><img src="https://img.shields.io/packagist/v/kinetis/queue-rabbitmq?label=version" alt="Packagist Version"></a>
  <a href="https://packagist.org/packages/kinetis/queue-rabbitmq"><img src="https://img.shields.io/packagist/dt/kinetis/queue-rabbitmq" alt="Packagist Downloads"></a>
  <a href="https://packagist.org/packages/kinetis/queue-rabbitmq"><img src="https://img.shields.io/packagist/php-v/kinetis/queue-rabbitmq" alt="PHP Version"></a>
  <a href="https://packagist.org/packages/kinetis/queue-rabbitmq"><img src="https://img.shields.io/packagist/l/kinetis/queue-rabbitmq" alt="License"></a>
  <a href="https://github.com/kinetis-dev/kinetis/actions/workflows/ci.yml"><img src="https://github.com/kinetis-dev/kinetis/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
</p>

---

Part of [Kinetis](https://kinetis.dev/), a non-blocking PHP framework for
API-first applications, developed in the
[kinetis-dev/kinetis](https://github.com/kinetis-dev/kinetis) monorepo.

Adds RabbitMQ as a queue backend. `push()`/`pop()`/`ack()`/`fail()` work
exactly like any other backend — only your configuration changes.
`release()` does too, with one difference worth knowing: it's two
separate AMQP operations rather than one atomic step, so a crash between
them can redeliver a job twice. See
[kinetis.dev/docs/queue-rabbitmq.html](https://kinetis.dev/docs/queue-rabbitmq.html#a-released-job-can-be-delivered-twice)
for why, and what other backends don't share this.

Delays are broker-driven and independent of each other: a job delayed by
three seconds waits three seconds, not the hour an earlier delayed job on
the same queue still has to go. A delay is a floor — the job is available
no sooner than that, and the broker delivers it when it gets to it.
Nothing beyond a stock RabbitMQ is needed — no plugin. Delays cap at
4,194,303 seconds (about 48 days), the longest queue TTL the AMQP client
can encode, and a longer one is rejected at `push()`.

```php
use Kinetis\Config\Config;
use Kinetis\QueueRabbitMq\RabbitMqClientFactory;
use Kinetis\QueueRabbitMq\RabbitMqQueue;

$queue = new RabbitMqQueue(RabbitMqClientFactory::fromConfig($config));

$queue->push(new SendWelcomeEmail($email, $name), queue: 'default');
```

## Configuration

```
QUEUE_CONNECTION=rabbitmq
QUEUE_RABBITMQ_URL=amqp://guest:guest@localhost:5672/
```

| Key | Default | Purpose |
|---|---|---|
| `QUEUE_RABBITMQ_URL` | *(required)* | `amqp://` URI. |
| `QUEUE_RABBITMQ_QUEUE_PREFIX` | — | Prepended to every queue name. |

Both are scoped — `QUEUE_RABBITMQ_URL` + `events` →
`QUEUE_EVENTS_RABBITMQ_URL`. [`kinetis/queue`](https://github.com/kinetis-dev/queue)'s own keys
(`QUEUE_CONNECTION`, `QUEUE_MAX_ATTEMPTS`, ...) are documented in that
package; full reference:
[kinetis.dev/docs/config.html](https://kinetis.dev/docs/config.html).

A queue name resolves directly to a RabbitMQ queue of that name, declared
durable the first time anything touches it — nothing to create ahead of
time. Delayed jobs additionally use queues and exchanges named
`{queue}.delay.{seconds}s`, declared the same way; a queue name can't
contain a `.`, so none of them can collide with a queue of your own.

## Installation

```sh
composer require kinetis/queue-rabbitmq
```

Requires PHP 8.4+, [`kinetis/framework`](https://github.com/kinetis-dev/framework), and [`kinetis/queue`](https://github.com/kinetis-dev/queue). Full
documentation:
[kinetis.dev/docs/queue-rabbitmq.html](https://kinetis.dev/docs/queue-rabbitmq.html).

## License

MIT — see [LICENSE](LICENSE).
