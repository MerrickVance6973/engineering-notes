# Node.js Media Reminder Delivery Under Provider Limits: Idempotent Batch Draining

When a media service has a large reminder backlog, the provider limit is the scheduling constraint, not the number of CPU cores. A safe design scans due reminders, publishes small batches, and lets each channel drain through its own concurrency and retry policy.

Short answer: use a durable queue as the handoff, cap both in-flight work and send-start rate per email or SMS provider, and make the reminder ID the idempotency key wherever the provider supports one. A queue can deliver a job again; the application must make that repeat harmless.

## What should a Node.js reminder queue do before a batch send?

Keep the timer and the delivery attempt separate. A cron-like scheduler should find reminders whose due time has passed and publish compact jobs. It should not hold a scheduler run open while an email or SMS request waits on a remote provider. The worker owns pacing; the database remains the record of what was due and what reached a terminal delivery state.

For a media workload, a job might contain a stable reminder ID, channel, destination reference, template version, and rendering data. The publisher should reserve or mark the batch transactionally with the application database, then make retries safe. A message is a work claim, not proof that a user saw anything.

Batch size deserves a measured limit. Large batches reduce queue calls, but they make a failed publish harder to reconcile and can push message size beyond the queue contract. Start with a small batch, record the last successfully published ID, and make a rerun publish by ID rather than by an opaque cursor. This is less elegant than pretending a scan is one atomic operation. It is much easier to explain during an incident.

Separate email and SMS queues even when they share a scheduler. Provider limits, response latency, retry guidance, and usefulness windows differ by channel. A single pool can let slow SMS requests starve email reminders, while one shared rate limiter can make the less constrained channel unnecessarily slow.

## How should a Node.js worker combine concurrency, batching, and provider limits?

Concurrency limits requests that are currently in flight. A start-rate limit controls how quickly new requests begin. They are different controls: a fast provider can free every worker quickly enough to violate a per-second allowance even though the concurrency number never changes.

The worker below shows the shape of the control loop without pretending to implement a provider SDK. It takes jobs from a channel, spaces starts, claims the reminder, and leaves a failed job available for the queue's retry policy. In production, the claim store must survive process restarts and the send operation should pass the same stable key to the provider when supported.

```go
package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Reminder struct {
	ID      string
	Channel string
}

type Claims struct {
	mu   sync.Mutex
	done map[string]bool
}

func (c *Claims) Once(id string, send func() error) error {
	c.mu.Lock()
	if c.done[id] {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	if err := send(); err != nil {
		return err
	}

	c.mu.Lock()
	c.done[id] = true
	c.mu.Unlock()
	return nil
}

func drain(ctx context.Context, jobs <-chan Reminder, workers int, startEvery time.Duration, claims *Claims) {
	starts := time.NewTicker(startEvery)
	defer starts.Stop()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobs:
					if !ok {
						return
					}
					select {
					case <-ctx.Done():
						return
					case <-starts.C:
					}
					err := claims.Once(job.ID, func() error {
						fmt.Printf("send %s via %s\\n", job.ID, job.Channel)
						return nil
					})
					if err != nil {
						fmt.Printf("retry %s: %v\\n", job.ID, err)
					}
				}
			}
		}()
	}
	wg.Wait()
}

func main() {
	jobs := make(chan Reminder, 3)
	jobs <- Reminder{ID: "episode-42-email", Channel: "email"}
	jobs <- Reminder{ID: "episode-42-sms", Channel: "sms"}
	jobs <- Reminder{ID: "episode-42-email", Channel: "email"}
	close(jobs)

	claims := &Claims{done: make(map[string]bool)}
	drain(context.Background(), jobs, 2, 100*time.Millisecond, claims)
}
```

This example has an important boundary: the in-memory claim is illustrative, not a production durability mechanism. A database row with a unique reminder ID, or an equivalent durable idempotency store, must define what happens when the process dies after the provider accepts the request but before the outcome is recorded. That boundary needs a runbook entry, not a hopeful comment in the worker. On restart, the operator should be able to list the affected IDs, see whether the provider has an idempotency-key result, and mark an unresolved attempt for reconciliation without publishing a new ID. If the provider can return a stable result for the same key, retry the request with that key; if it cannot, hold the decision in an explicit unknown state and use the delivery record, provider logs, or an application-specific suppression rule before sending again. I'm not sure any generic queue setting can promise exactly-once user-visible delivery in that gap. Your mileage may vary by provider; resolve it with an idempotency-key contract or an explicit reconciliation path.

On a `429`, do not spin. Honor `Retry-After` when present; otherwise use exponential backoff with jitter, keep the job retryable, and reduce the channel's start rate if throttling persists. A dead-letter queue is for work that needs inspection after a bounded retry policy. It is not permission to configure the worker above the provider contract.

## Which failure signals show that the drain is unsafe?

Queue depth is a weak signal by itself. Measure it.

Track the age of the oldest actionable reminder, due-to-published latency, published-to-attempt latency, provider response categories, retry count, and duplicate-claim decisions. A successful scheduler run only says that a scan ran; it says nothing about delivery.

Test one failure mode at a time. Publish the same reminder ID twice and confirm one user-visible effect. Hold provider responses open and confirm in-flight requests stay below the channel cap. Return `429` with `Retry-After` from a test endpoint and verify that retries wait instead of forming a tight loop. Pause the scheduler and reconcile the database interval after resuming, because missed timer triggers should not be assumed to replay themselves.

I've been paged by missed jobs and duplicate deliveries, and the useful alert was rarely “worker count is low.” Page when the oldest reminder crosses its usefulness window, when retryable work grows while provider acceptance falls, or when the idempotency store and delivery records disagree. Those signals point at user impact and reconciliation, not merely process health.

The trade-off is deliberate. This design is not suitable when a team needs a workflow engine for long-running, branching media campaigns; use a workflow system with durable state and built-in activity semantics in that case. It is also a poor fit when the provider offers no useful idempotency contract and duplicate messages are unacceptable without human reconciliation. Stick with a queue plus application-owned state when the work is short, channel-specific, and easy to replay by stable ID.

## How can a team roll back a reminder drain without a duplicate wave?

Stop new publication first. Record a cutoff time, pause the schedule, let in-flight requests finish or reach a recorded unknown outcome, and scale workers down in a controlled sequence. Do not purge pending jobs as a routine rollback; deleting them removes the evidence needed to compare the queue with the application database.

After correcting the change, query reminders due since the cutoff and compare their IDs with durable delivery records. Republish only IDs without a terminal outcome. Start one channel at a conservative pace, watch oldest-message age and provider throttling, then expand capacity. A rollback is complete when the database, queue, and provider outcome records agree, not when the deployment command succeeds.

No heroics.

## References

- https://en.wikipedia.org/wiki/Cron
- https://en.wikipedia.org/wiki/Exponential_backoff
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/429
- https://www.rfc-editor.org/rfc/rfc6585
