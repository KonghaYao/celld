# Cron Triggers

A Cron Trigger runs a Worker on a schedule instead of on a request. An
application uses one for periodic work, such as a cleanup, a report, or a poll
of an upstream service. celld keeps the schedule of a script in one reserved
cell, so a fleet of many nodes runs each occurrence once. Read the
[Cloudflare Cron Triggers documentation](https://developers.cloudflare.com/workers/configuration/cron-triggers/)
for the standard API behavior.

## Example

The [Cron Trigger example](../../examples/cron) logs the scheduled time once a
minute.

<!-- celld-example: cron -->

## Configuring a schedule

A project declares its schedules in a `triggers.crons` array, and each entry is
one cron expression. `celld deploy` parses every entry, therefore a malformed
expression stops the deployment instead of failing at the first occurrence. A
configuration that sets `triggers.crons` without `main` also fails, because a
Cron Trigger needs a `scheduled` handler to call.

celld reads the Cloudflare cron dialect and not the POSIX one. An expression has
five fields: minute, hour, day of month, month, and day of week. The resolution
is one minute and the zone is UTC. Cloudflare
[numbers the weekdays 1 to 7 from Sunday](https://developers.cloudflare.com/workers/configuration/cron-triggers/),
so `1-5` means Sunday to Thursday here and not Monday to Friday.

Each field takes `*`, a single value, an `a-b` range, an `a,b` list, and a `/n`
step on any of those. The month field and the day-of-week field also take the
three-letter names in any case, such as `JAN` and `MON`. celld supports the
non-standard day forms as well: the day-of-month field takes `L`, `L-<n>`, `LW`,
`L-<n>W`, and `<d>W`, and the day-of-week field takes `<dow>L` and `<dow>#<n>`.

The two day fields form a union and not an intersection when both of them carry
a restriction, which is the ordinary cron rule. `0 0 1 * MON` therefore fires on
the first day of each month and on every Monday. Only a literal `*` leaves a day
field unrestricted, so `*/2` in the day-of-month field joins that union.

celld refuses a small set of expressions that Cloudflare accepts, because the
upstream parser answers them in a way that nobody writes on purpose. A
descending range such as `SAT-SUN` wraps around the end of the field there and
takes one value too many at the low end, so celld stops the deployment instead
of shipping a schedule that fires on an unasked day. A `*` inside a list such as
`1,*` means the minimum of the field there and not every value, so celld refuses
that form and asks for the values. celld also bounds a step by the width of its
field, therefore `*/60` fails for the minute field.

A script can declare as many expressions as it needs, because celld sets no
count limit. A node fires only the schedule of the deployment that it serves, so
a `triggers.crons` entry in a service-binding target never runs, and celld logs
a warning when it loads such a target.

## The node that runs a scheduled event

celld gives each deployment that declares `triggers.crons` one reserved cell,
named from the class `.cron` and the script name. That cell carries the whole
schedule of the script. Exactly one node owns a cell at a time, which is the
rule that every [Durable Object](durable-objects.md) already follows, so the
reserved cell is what makes an occurrence run once for the fleet and not once
for each node.

A cron cell has no client that can wake it, so every node calls the cell once
after its deployment loads. The ownership record in the fleet bucket accepts one
of those callers, and the other nodes route their call to that owner. celld adds
no election and reserves no cron node. The call is idempotent, because the same
schedule and the same clock give the same deadline, so which node wins does not
matter.

The cell does nothing between two occurrences, so it hibernates like any other
cell and costs no isolate while it waits. A deployment that declares no
expression leaves nothing to arm, and the cell then deletes its alarm and
retires instead of waking forever.

The cell holds one alarm row, therefore one deadline covers every expression of
the script. celld arms that row at the earliest next occurrence across the whole
list. When the alarm fires, celld finds every expression that matches that
minute and calls `scheduled(controller, env, ctx)` once for each one. The
invocations run one after another inside the cell, because a cell runs one event
at a time.

`controller.cron` holds the expression text as the developer wrote it, and
`controller.scheduledTime` holds the occurrence in milliseconds. celld reports
the occurrence and never the instant the attempt started, so a late run and a
retry both name the minute that they were scheduled for. `controller.noRetry()`
and `ctx.waitUntil()` behave as the
[scheduled handler documentation](https://developers.cloudflare.com/workers/runtime-apis/handlers/scheduled/)
describes, and celld drains the `waitUntil()` work of one invocation before it
starts the next one.

![Every node arms the same reserved cron cell and ownership CAS keeps one owner, the cell holds one alarm row armed at the earliest occurrence across all expressions, a shared deadline runs one scheduled invocation for each matching expression in turn, and the re-arm takes whichever is earlier of the next occurrence and the failure backoff](cron-triggers-flow.svg)

Cloudflare runs a scheduled event on its own global network and selects the
location for you. celld makes no such placement promise. The occurrence runs on
the node that owns the reserved cell at the deadline, and that owner can change
when a node stops, when a node drains, or when idle rebalancing moves the cell.

## Failure, retry, and a missed occurrence

A `scheduled` handler that throws leaves its expression owed. celld logs the
error and arms a backoff of 4 seconds, and each further failure of the same
occurrence doubles that delay. celld gives up on the occurrence after six
failures. A retry repeats only the expressions that failed, because the
expressions that succeeded already ran for that occurrence.
`controller.noRetry()` removes an expression from that set, so a handler that
knows the work is pointless ends the loop itself.

The next occurrence and the backoff compete for the one alarm row, and the
earlier deadline wins. A retry therefore never delays a scheduled run and never
replaces one. A schedule that fires faster than the backoff never retries at
all, which costs little, because the next attempt is already seconds away. celld
holds the retry state in memory, so an eviction or an ownership move loses a
pending retry and never the next occurrence.

celld runs one missed occurrence after downtime. A deadline that is already due
when the fleet returns is a late tick and not a wrong one, so celld leaves the
armed alarm in place and runs it. The re-arm then walks forward from the current
time, therefore the occurrences between the late one and now do not run. An
application that must process every interval reads the interval from
`controller.scheduledTime` and catches up in its own code.

Two occurrences of one script never overlap. The reserved cell runs one event at
a time, so a handler that takes longer than the interval holds the cell and
delays the next occurrence. Move long work into a [queue](queues.md) or a
[Workflow](workflows.md) when the schedule must stay on time.

## Differences from Cloudflare

- celld rejects a descending range such as `SAT-SUN` and a list that contains
  `*`.
- celld runs one handler for each occurrence across the fleet. After downtime,
  it runs one missed occurrence and skips the rest.
- celld serializes the handlers for one script. It retries a failure until the
  next occurrence unless the handler calls `noRetry()`.
- A service-binding target cannot run its own Cron Triggers.

The [Cloudflare compatibility](../cloudflare-compat.md#services) page
lists the runtime APIs and the unsupported services.
