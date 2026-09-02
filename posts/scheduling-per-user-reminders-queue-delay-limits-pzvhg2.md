# Scheduling per-user reminders: queue delay limits, cron fallback, and webhook retries

Use the queue for timing and a database row for truth. A delay queue gives you cheap, low-latency wakeups for per-user reminders, and a cron sweep over the same table is what saves you when the queue's delay limit, a bad deploy or a lost message eats the wakeup. The pairing only holds if every scheduled notification carries an identity that survives both paths, because the first time a cron fallback and a delayed message fire for the same reminder, your public HTTPS webhook receives it twice — and in a payments backend, twice is not a rounding error.

## The duplicate webhook page, and why it starts in the scheduler

I run cron and queue infrastructure in production, and I've been paged from both ends of this: reminders that never fired, and reminders that fired again after someone replayed a backlog.

The second page is worse.

A reminder that never goes out is a support ticket and an apology. A duplicate outbound webhook is a duplicate side effect in somebody else's system — a second "payment due" push to a customer who already paid, or a partner ledger that books the redelivery as a fresh instruction. The blast radius isn't yours to control once the request leaves your egress. That asymmetry is the whole reason I treat delivery deduplication as a scheduling concern rather than a networking one: by the time you are choosing between at-least-once and at-most-once at the HTTP layer, the damage has already been designed in upstream, in how the reminder was scheduled and how its identity was assigned.

The incident shape repeats. A scheduler enqueues a delayed message. Something delays the consumer past the visibility timeout — a slow downstream, a deploy that drains workers, a lock held too long. The broker decides the message was never processed and hands it to a second worker. Now two workers own the same reminder, and both of them POST to the customer's endpoint. Amazon SQS documents this directly: the visibility timeout defaults to 30 seconds and can be raised to 12 hours, and if processing outlives it, the message becomes visible to other consumers again. That is not a defect. It's the contract you signed when you chose an at-least-once broker.

The queue behaved correctly. Our identity model didn't.

## Should a reminder backend use a delay queue or a cron fallback for per-user notifications?

Both, with strictly different jobs. The queue is a timer, the table is the ledger, and cron is the sweeper that reconciles the two.

Start with the horizon, because that's where most designs quietly break. Message-level delay is capped much lower than people expect — SQS allows a per-message delay of at most 15 minutes, and default message retention is 4 days with a 14-day ceiling. A reminder due in 7 days cannot live as a parked message under those defaults; it will be silently discarded before it ever becomes visible. Managed durable-execution platforms do expose multi-day sleeps as a first-class primitive, so this is a real difference in capability, not a universal law. But if your queue is a plain broker, the horizon belongs in a `reminders` table with a `due_at` column, and the queue only ever carries the last mile.

| Mechanism | Natural horizon | How it fails | What recovers it |
| --- | --- | --- | --- |
| Per-message delay | Minutes | Delay cap and retention window silently drop the message | Cron sweep over due rows |
| Durable workflow sleep | Days to months | Vendor lock-in, harder local testing, per-step cost | Vendor replay plus your own audit query |
| Database row plus cron sweep | Unbounded | Sweep interval sets your worst-case lateness | Idempotent re-dispatch on the next tick |
| Per-user in-process timer | Process lifetime | Vanishes on restart, does not survive a rolling deploy | Anything durable, which is the point |

My default for user reminders is the third row with the first row bolted on for latency: rows carry the schedule, a sweep every 30 seconds claims what is due, and short-horizon work gets an optional enqueue so the common case doesn't wait for the next tick. The sweep interval becomes your published lateness budget. Write it in the runbook, alert on the oldest unclaimed `due_at`, and you have converted an invisible failure into a boring gauge.

## One scheduled occurrence, many attempts, one delivery

Here is the invariant that survived every postmortem I've written on this: a scheduled occurrence has exactly one identity, and every attempt to deliver it — from the queue path, from the cron path, from a manual replay during an incident — must present that same identity.

Not the attempt. The occurrence.

Derive the key from the reminder id and the scheduled instant, never from a UUID minted at send time and never from the attempt counter. `reminder:8f21:1786291200` is stable whether it's produced by a worker at 09:00:00, by the sweep at 09:00:30, or by an operator draining a dead-letter queue three hours later. Send it as an `Idempotency-Key` header — the IETF HTTP API working group has a draft specification for exactly this, so it's a convention receivers increasingly recognise rather than something you invented locally. RFC 9110 is blunt about why you can't skip this: POST is not idempotent, so retry safety is something the two endpoints agree on, not something the protocol gives you.

Then close the loop on your own side with a unique constraint on `(reminder_id, occurrence)` in a deliveries table, written in the same transaction that marks the reminder done. A constraint violation on insert is the correct, quiet outcome of a duplicate — it means the invariant held. If you only dedupe at the receiver, you're trusting someone else's storage to protect your customers; if you only dedupe locally, a network timeout after a successful POST still produces a double delivery. You want both, and you want the receiver's dedupe window documented, because a 24-hour window and a 7-day retry policy don't compose.

## Dispatcher code: claim, lease, deliver

The claim step is the only part that needs to be clever, and it's about fifteen lines of SQL. Lease the row, don't just read it.

```go
const claimDue = `
UPDATE reminders
   SET state = 'claimed',
       attempt = attempt + 1,
       lease_until = now() + interval '2 minutes'
 WHERE id IN (
   SELECT id FROM reminders
    WHERE state IN ('pending', 'claimed')
      AND due_at <= now()
      AND (lease_until IS NULL OR lease_until < now())
    ORDER BY due_at
    FOR UPDATE SKIP LOCKED
    LIMIT $1)
RETURNING id, user_id, endpoint, due_at, attempt`

// claim returns work that is due and unleased. SKIP LOCKED lets N sweepers
// run concurrently without blocking each other, so a rolling deploy that
// starts a new pod before draining the old one stays correct.
func (d *Dispatcher) claim(ctx context.Context, batch int) ([]Reminder, error) {
	rows, err := d.db.QueryContext(ctx, claimDue, batch)
	if err != nil {
		return nil, fmt.Errorf("claim due reminders: %w", err)
	}
	defer rows.Close()

	var out []Reminder
	for rows.Next() {
		var r Reminder
		if err := rows.Scan(&r.ID, &r.UserID, &r.Endpoint, &r.DueAt, &r.Attempt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
```

The lease is the part people skip, and it's what makes the cron fallback safe to run next to the queue consumer. A row claimed by a worker that dies is picked up again after the lease expires, and a row claimed by a live worker is invisible to everyone else. Set the lease longer than your HTTP timeout plus your retry budget, or you'll reintroduce the exact overlap you were trying to prevent.

Delivery is deliberately dull:

```go
func (d *Dispatcher) deliver(ctx context.Context, r Reminder) error {
	// Stable across the queue path, the cron sweep and any manual replay.
	key := fmt.Sprintf("reminder:%s:%d", r.ID, r.DueAt.UTC().Unix())

	body, _ := json.Marshal(map[string]any{
		"type": "reminder.due", "reminder_id": r.ID, "user_id": r.UserID,
		"due_at": r.DueAt.UTC().Format(time.RFC3339),
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.Endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	req.Header.Set("X-Signature", d.sign(body))

	resp, err := d.client.Do(req) // client.Timeout well under the lease
	if err != nil {
		return retryable{err} // ambiguous: the POST may have landed
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode < 300:
		return d.markDelivered(ctx, r, key, resp.StatusCode)
	case resp.StatusCode == 408, resp.StatusCode == 429, resp.StatusCode >= 500:
		return retryable{fmt.Errorf("endpoint %d", resp.StatusCode)}
	default:
		return d.deadLetter(ctx, r, resp.StatusCode) // 4xx: our payload, not their outage
	}
}
```

Two operational details make this testable. Inject the clock — a `now()` you control lets you assert that a reminder due in 7 days is not claimed on day 6 and is claimed on day 7, without sleeping in tests. And keep a replay test that runs `deliver` twice against a fake receiver, asserting one recorded delivery and two identical keys. That test has caught more regressions for me than any amount of staging traffic, because duplicate delivery is a race you will not reproduce by clicking around.

For observability, three signals cover almost everything: the age of the oldest due-but-unclaimed row, the count of leases that expired without a terminal state, and delivery outcomes split by endpoint. The first tells you the sweeper is alive, the second tells you workers are dying mid-flight, the third tells you whose endpoint is having a bad day. I page on the first one only.

## When this design is the wrong call

The catch is that all of it assumes your reminder volume fits comfortably in one relational database. A sweep that scans due rows every 30 seconds is fine at millions of rows and starts to hurt when the due index no longer stays in memory; at that point you're partitioning by time bucket or moving to a purpose-built timer service, and the operational simplicity that made this design attractive is gone.

Stick with a managed durable workflow platform when your reminders are really multi-step sequences — send, wait 3 days, check state, escalate — because rebuilding step-level replay and versioned workflow state on top of a table is a project, not a weekend. The trade-off you're accepting there is portability and local testability in exchange for not maintaining a scheduler.

This design also doesn't support sub-second precision, and it isn't suitable when the receiver refuses to dedupe and treats every POST as a new command. If you can't get an idempotency contract in writing, no amount of scheduler hygiene fixes it; you're reduced to at-most-once delivery and accepting silent misses, which for a payment reminder is usually the wrong trade. I'm also not sure the 30-second sweep interval generalises — for a fintech reminder, lateness measured in seconds is invisible to users, and if yours is closer to a trading alert your mileage may vary and you'll want the queue path to be the primary one.

The durable part of this is small. One identity per occurrence, one lease per attempt, one constraint that makes the duplicate boring. Everything else is preference.

## Sources

- Amazon SQS visibility timeout — https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
- Amazon SQS message quotas (delay and retention limits) — https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html
- RFC 9110, HTTP Semantics, idempotent methods — https://www.rfc-editor.org/rfc/rfc9110.html#section-9.2.2
- The Idempotency-Key HTTP Header Field (IETF draft) — https://datatracker.ietf.org/doc/html/draft-ietf-httpapi-idempotency-key-header
- PostgreSQL SELECT, FOR UPDATE SKIP LOCKED — https://www.postgresql.org/docs/current/sql-select.html
- Inngest documentation, durable steps and sleeps — https://www.inngest.com/docs
