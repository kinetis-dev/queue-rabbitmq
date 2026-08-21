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

Adds RabbitMQ as a queue backend. `push()`/`pop()`/`ack()`/`release()`/`fail()`
work exactly like any other backend — only your configuration changes.

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
`QUEUE_EVENTS_RABBITMQ_URL`. `kinetis/queue`'s own keys
(`QUEUE_CONNECTION`, `QUEUE_MAX_ATTEMPTS`, ...) are documented in that
package; full reference:
[kinetis.dev/docs/config.html](https://kinetis.dev/docs/config.html).

A queue name resolves directly to a RabbitMQ queue of that name, declared
durable the first time anything touches it — nothing to create ahead of
time. Don't name a queue ending in `.delay`; that suffix is reserved for
the internal queue delayed jobs route through.

## Installation

```sh
composer require kinetis/queue-rabbitmq
```

Requires PHP 8.4+, `kinetis/framework`, and `kinetis/queue`. Full
documentation:
[kinetis.dev/docs/queue-rabbitmq.html](https://kinetis.dev/docs/queue-rabbitmq.html).

## License

MIT — see [LICENSE](../../LICENSE).
