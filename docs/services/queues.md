# Queues

[Queues](https://developers.cloudflare.com/queues/) is a message broker. A
producer Worker sends a message, and a consumer Worker receives the message
later in a batch. An application uses a queue to move work out of a request,
such as an email, a webhook call, a thumbnail, or a search index update. Each
queue is one celld cell, so a message lives in that cell's SQLite database
until a consumer acknowledges it. Read the
[Cloudflare Queues documentation](https://developers.cloudflare.com/queues/configuration/javascript-apis/)
for the standard API behavior.

## Example

The [Queues example](../../examples/queues) sends one job from a request and
processes the job in a queue handler. A job for a path that ends in `/fail`
retries twice, and the example then reads it from a dead-letter queue.

<!-- celld-example: queues -->

## Sending a message

A project declares a producer with a `queues.producers` entry that gives a
`binding` name and a `queue` name. `env.JOBS.send(body)` sends one message, and
`env.JOBS.sendBatch(messages)` sends up to 100 messages in one call. celld
resolves the queue name to one cell, therefore every producer in the fleet that
names `example-jobs` writes to the same queue.

A message carries a body and a content type. The `contentType` option selects
`"v8"`, `"json"`, `"text"`, or `"bytes"`, and the `queues_json_messages`
compatibility flag chooses the default, as on Cloudflare. celld enforces the
[Cloudflare limits](https://developers.cloudflare.com/queues/platform/limits/):
a message of at most 128,000 bytes, a batch of at most 100 messages, and at most
256,000 bytes in one `sendBatch()` call. A `delaySeconds` option holds a message
invisible for up to 86,400 seconds, and a producer entry can set
`delivery_delay` as the default for every message of that binding.

`send()` resolves after the queue cell commits the message, so a producer learns
that the message is durable. celld joins the concurrent sends of one queue into
one transaction, because a separate commit for each call makes durability the
cost of the queue. The cell accepts at most 256 producer calls at a time, and it
refuses a further call with a cell overload error that the producer can retry.
One queue has one writer, so an application that needs more write capacity
spreads its messages across more queues.

## The consumer and the batch

A project declares a consumer with a `queues.consumers` entry that names the
`queue`. One script consumes a queue, and a deployment in which two scripts
consume one queue fails. The consumer entry sets `max_batch_size` (10 by
default, 100 at most), `max_batch_timeout` in seconds (5 by default, 60 at
most), `max_retries` (3 by default), `max_concurrency` (250 at most),
`retry_delay`, and `dead_letter_queue`. celld validates each bound at deploy
time, so a bad value fails the deployment instead of the first delivery. Read
[Batching, retries, and delays](https://developers.cloudflare.com/queues/configuration/batching-retries/)
for the upstream meaning of each key.

The queue cell forms the batch. It closes a batch when `max_batch_size` messages
are ready, or when `max_batch_timeout` passes after the oldest ready message,
whichever arrives first. A batch can therefore hold fewer messages than
`max_batch_size`. The cell leases the batch and calls the
`queue(batch, env, ctx)` handler that the consumer script exports. That handler
runs in a stateless isolate and not inside the queue cell, so a slow handler
does not block a producer. `max_concurrency` bounds how many batches of one
queue can run at the same time.

`batch.queue` names the queue, and `batch.messages` holds the messages. Each
message has an `id`, a `timestamp`, the decoded `body`, and an `attempts` count
that is 1 on the first delivery. celld does not guarantee a delivery order, and
[Cloudflare gives the same rule](https://developers.cloudflare.com/queues/reference/how-queues-works/).
A retry, a per-message delay, and a concurrent batch each move a message away
from its send order, so an application that needs an order must carry that order
in the message body.

## Acknowledgement and retry

celld leases a batch instead of deleting it, so a message survives a consumer
that fails. Each message settles once. `message.ack()` marks one message as
done, and `message.retry()` returns one message to the queue. `batch.ackAll()`
and `batch.retryAll()` apply the same two outcomes to the whole batch. The first
call for a message wins, therefore a later call on that message does nothing.

A handler that returns without a call acknowledges every message of its batch. A
handler that throws returns every message to the queue, except a message that
the handler acknowledged before the throw. A handler that never settles its
batch holds the lease until the lease expires, and celld then counts the batch
as one failed delivery.

Every failed delivery raises `attempts` by one and makes the message visible
again. A `delaySeconds` option on `retry()` or `retryAll()` sets the delay, and
the consumer's `retry_delay` is the default. celld adds no exponential backoff,
so an application that wants one computes the delay from `message.attempts`.

A message leaves the retry loop when `attempts` passes `max_retries`. celld
moves the message to the queue that `dead_letter_queue` names, which is an
ordinary queue and can have its own consumer. celld deletes a message that
reaches the bound when the consumer configures no dead-letter queue. Read
[Dead letter queues](https://developers.cloudflare.com/queues/configuration/dead-letter-queues/)
for the upstream behavior.

![A busy queue closes a batch on max_batch_size and a sparse queue closes it on max_batch_timeout, and a message that no consumer acknowledges returns to the queue with attempts raised by one until attempts pass max_retries and the message moves to the dead-letter queue](queues-flow.svg)

celld delivers a message at least once, as Cloudflare does. A consumer can crash
after its side effect and before its acknowledgement, and the message then
returns to the queue and runs again. A consumer must therefore tolerate a
duplicate. `message.id` stays the same across a redelivery, so an application
can use it as an idempotency key.

celld keeps a message for four days after the send. A sweeper removes an older
message, even when no consumer read it, so a queue with no working consumer
loses its backlog instead of growing without a bound.

## Differences from Cloudflare

- A queue has one writer. Use more queues to increase write capacity.
- A queue owner accepts at most 256 concurrent producer calls. It refuses an
  additional call, which the producer can retry.
- celld retains a message for four days. This period is not configurable.
- Pull consumers, the Queues HTTP API, dashboard controls, manual consumer
  attachment, R2 event notifications, and Queue event subscriptions are
  unavailable.

The [Cloudflare compatibility](../cloudflare-compat.md#services) page lists the
runtime APIs and the unsupported services.
