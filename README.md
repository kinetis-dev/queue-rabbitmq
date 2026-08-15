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

## Configuring

```
QUEUE_CONNECTION=rabbitmq
QUEUE_RABBITMQ_URL=amqp://guest:guest@localhost:5672/
```

A queue name resolves directly to a RabbitMQ queue of that name, declared
durable the first time anything touches it — nothing to create ahead of
time. Don't name a queue ending in `.delay`; that suffix is reserved for
the internal queue delayed jobs route through.

## Important: opening this connection disables `concurrently()` in that process

Once anything calls `push()`/`pop()` for the first time, `Kinetis\Async\concurrently()`
can't be called again anywhere in that same OS process, for any reason,
for as long as the connection stays open — which is indefinitely, since
nothing here closes it on its own. RabbitMQ keeps a connection open and
listening at all times, and `concurrently()` waits for everything pending
in the process to settle before it returns, which never happens while
that connection stays open.

This never affects the `vendor/bin/queue work` loop itself. It does
affect two other things:

- **A job's own `handle()`** reaching for `concurrently()` for its own
  unrelated work, once any `RabbitMqQueue` in that process has opened a
  connection.
- **A persistent HTTP worker (FrankenPHP), not just a queue worker.** If a
  controller calls `push()` to enqueue a job, that opens the connection in
  the *request-handling* worker process too — and a persistent worker
  keeps running that same process across many unrelated requests
  afterward. Every later request that process happens to serve loses the
  ability to call `concurrently()`, even if that request never touches
  this queue at all, until the worker restarts.

## Installation

```sh
composer require kinetis/queue-rabbitmq
```

Requires PHP 8.4+, `kinetis/framework`, and `kinetis/queue`. Full
documentation:
[docs.kinetis.dev/queue-rabbitmq.html](https://docs.kinetis.dev/queue-rabbitmq.html).

## License

MIT — see [LICENSE](../../LICENSE).
