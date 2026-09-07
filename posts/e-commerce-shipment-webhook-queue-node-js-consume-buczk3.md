# E-commerce Shipment Webhook Queue: Node.js Consumers, Push Delivery, and Rate Limits

Short answer: for a shipment update that must reach many subscribers, persist one event, give each subscriber an independent delivery record, and let a rate-limited HTTPS worker acknowledge only after the subscriber's side effect is durably idempotent. A Node.js consumer is fine, but the language does not change the delivery contract: expect duplicates, bound concurrency, and send poison messages to a dead-letter path instead of retrying forever.

The operational constraint is the subscriber quota. A sale can produce a burst of shipment events while a partner accepts only a small number of requests per second. If the consumer answers every push immediately and lets the downstream call run unbounded, the queue has moved the overload rather than absorbed it. The result is usually a 429 storm, rising delivery attempts, and an operator trying to decide which shipment updates were actually applied.

This is a fan-out problem first and a webhook problem second. The shipment service should retain an event identity such as `shipment-8472-status-3`, then create delivery state for each subscriber. One slow subscriber must not hold up the fraud, notification, or fulfillment path for the others.

## How should an e-commerce webhook queue handle HTTPS push, rate limiting, and ack/nack?

Treat the HTTPS endpoint as an adapter with a narrow job. It authenticates the request, validates the envelope, checks the delivery identity, waits for admission to the rate-limited worker, performs the subscriber call, records the result, and returns the status that the queue maps to ack or nack. The business handler can sit behind a load balancer; the externally reachable edge still needs valid TLS and an authentication scheme that lets the service reject forged shipment updates.

Ack is the final commit step.

An ack before the durable mutation is a lost shipment notification if the process exits between those operations. A nack after a successful mutation is less dangerous only when the mutation is idempotent, because the queue may deliver the same event again. Store the delivery key with the business result in durable storage. A process-local map is not a ledger: it disappears on restart and says nothing to a second replica.

Use separate policies for failures:

- A timeout, connection refusal, 429, or temporary dependency refusal should return a retryable result. Apply backoff and jitter at the queue or worker boundary, not in a tight application loop.
- A malformed, unauthenticated, or permanently invalid payload should go through the dead-letter policy after the agreed attempt limit. Keep the event identity, subscriber identity, failure class, and last response metadata so an operator can make a redrive decision.
- A duplicate delivery should verify the recorded outcome and acknowledge without applying the shipment change twice.

The catch is that HTTPS push is not suitable when the organization cannot expose a reachable endpoint or cannot operate request authentication at that boundary. Use a pull consumer or a private queue arrangement in that case. That choice is about network and ownership constraints, not which runtime is fashionable.

## The delivery ledger is the safety boundary

For each `(event_id, subscriber_id)` pair, keep a state transition that is safe to repeat. A useful minimal record contains the event payload or a durable reference to it, the subscriber endpoint identity, attempt count, next-attempt time, delivery status, and an idempotency key. The key should remain stable across retries. Do not derive it from the current attempt number.

The side effect and the completion record need a transaction boundary. For a subscriber call that cannot participate in the same database transaction, use an outbox or intent record: commit the intent first, perform the external call with the stable key, and reconcile an ambiguous timeout by querying the subscriber when that protocol allows it. If no query exists, the receiver must be idempotent. There is no honest way to infer success from a missing HTTP response. This is where a superficially healthy implementation tends to fail: the handler makes the remote request, the partner commits the update, the connection drops before the response reaches the worker, and the queue quite reasonably redelivers the event. Without a stable key and a receiver-side duplicate check, the second delivery sends the same shipment update again. The logs show two attempts; only the ledger can show whether there were two effects.

I've been paged for missed jobs and duplicate deliveries, and the useful question in both incidents was the same: can the ledger distinguish "committed" from "attempted"? `attempted` is not success. A timeout after a remote commit is an ambiguous result, so the next delivery must be allowed to run without creating a second business effect.

Measure twice.

For a shipment fan-out, do not publish one mutable message and let several workers race over it. Create an independent delivery task for each subscriber, or use a fan-out mechanism that gives each branch its own acknowledgement and dead-letter policy. A notification partner may be healthy while a returns partner is throttling; their retry clocks should not be coupled.

## A bounded Go consumer for the rate-limited edge

The following adapter shows the control points without assuming a provider-specific message envelope. It caps in-flight work, refills a token bucket, preserves a delivery key, and returns a retryable HTTP status when admission cannot happen before the request deadline. In production, replace the in-memory ledger with a database transaction or an outbox. The example is Go because the service contract matters more than a framework-specific Node.js wrapper.

```go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

type ledger struct {
	mu   sync.Mutex
	done map[string]struct{}
}

type worker struct {
	limit chan struct{}
	token chan struct{}
	seen  ledger
}

func newWorker(concurrency, perSecond int) *worker {
	if concurrency < 1 {
		concurrency = 1
	}
	if perSecond < 1 {
		perSecond = 1
	}

	w := &worker{
		limit: make(chan struct{}, concurrency),
		token: make(chan struct{}, perSecond),
		seen:  ledger{done: make(map[string]struct{})},
	}

	interval := time.Second / time.Duration(perSecond)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			select {
			case w.token <- struct{}{}:
			default:
			}
		}
	}()
	return w
}

func (w *worker) handle(rw http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(rw, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(rw, r.Body, 256<<10))
	if err != nil {
		http.Error(rw, "invalid payload", http.StatusBadRequest)
		return
	}

	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		digest := sha256.Sum256(body)
		key = hex.EncodeToString(digest[:])
	}

	w.seen.mu.Lock()
	_, complete := w.seen.done[key]
	w.seen.mu.Unlock()
	if complete {
		rw.WriteHeader(http.StatusNoContent)
		return
	}

	select {
	case w.limit <- struct{}{}:
		defer func() { <-w.limit }()
	case <-r.Context().Done():
		http.Error(rw, "retry admission", http.StatusServiceUnavailable)
		return
	}

	select {
	case <-w.token:
	case <-r.Context().Done():
		http.Error(rw, "retry rate limit", http.StatusServiceUnavailable)
		return
	}

	// Commit the subscriber effect and key in one durable transaction.
	w.seen.mu.Lock()
	w.seen.done[key] = struct{}{}
	w.seen.mu.Unlock()
	rw.WriteHeader(http.StatusNoContent)
}

func main() {
	w := newWorker(32, 10)
	http.HandleFunc("/shipment-events", w.handle)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

The sample's fallback hash is only safe when identical payloads always represent the same event. Prefer an event ID from the producer and include the subscriber ID in the durable key; two distinct shipment events can legitimately have identical JSON. The code also uses a simple token bucket, not a claim that every partner's quota has the same burst semantics. Your mileage may vary, so set the refill rate from the documented subscriber limit and leave headroom for other traffic.

The handler should authenticate before consuming expensive worker capacity. It should also enforce a request deadline shorter than the queue's delivery deadline, otherwise the queue may retry while the first handler is still holding a slot. Measure queue age, in-flight handlers, token wait time, downstream status classes, duplicate-hit count, and dead-letter volume. A green endpoint alone proves very little.

## Verification, dead letters, and rollback

Start with one uniquely identified shipment event and one test subscriber. Verify that the request is authenticated, the downstream effect exists once, and the acknowledgement follows the durable commit. Deliver the same identity again. The expected result is one business mutation and one duplicate acknowledgement, not two notifications.

Then test the awkward cases deliberately: force a 429, cut the connection after the subscriber has committed, submit malformed input, and exhaust the retry policy. Check that retryable failures preserve the stable key, that a poison message reaches the dead-letter queue with enough context, and that a redrive does not bypass the ledger. An HTTP 204 is useful only after those checks establish what it means in this system.

Keep the first rollout narrow. Limit the number of enabled subscribers, watch oldest-message age and attempt count, and compare downstream throttling with actual shipment completion. If the queue grows, reduce admission or pause the affected branch; raising concurrency against a quota usually makes recovery slower.

Rollback needs a data decision. Stop new fan-out tasks for the affected subscriber, let in-flight work finish or expire under the documented deadline, and preserve delivery records while operators compare committed effects with outstanding attempts. Switch to a pull-capable path before removing the public endpoint, or retries will accumulate against an unreachable target. Redrive only a reviewed sample first.

## Choosing the operating model

An HTTPS push queue is a good fit when the team wants the queue to absorb bursts and can own a public edge, a durable idempotency ledger, and subscriber-specific retry policy. A PostgreSQL work queue using `FOR UPDATE SKIP LOCKED` keeps task state beside application data, but the team owns polling, retention, backoff, and worker health. A streaming system fits when retained history and independent consumer groups matter more than a short-lived delivery task. A workflow engine fits when shipment handling has joins, timers, or long-running dependencies.

There is no universal winner. Stick with a database-backed queue when transactional locality is the main requirement. Choose a stream when replay is a first-class recovery operation. Keep a Node.js consumer when its runtime and operational tooling are already standard for the service, but do not let the runtime dictate ack semantics or hide the delivery ledger.

The decision rule is plain: pick the smallest operating model that can prove one shipment event becomes one subscriber effect, even after a timeout, a retry, a replica restart, and a rate-limit response. That proof is the feature the pager cares about.

## References

- https://en.wikipedia.org/wiki/Cron
- https://www.postgresql.org/docs/current/sql-select.html
- https://www.rfc-editor.org/rfc/rfc9110
