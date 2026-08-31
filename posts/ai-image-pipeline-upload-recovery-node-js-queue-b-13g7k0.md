# AI Image Pipeline Upload Recovery: Node.js Queue Backoff for 429 Limits

## Runbook summary

Short answer: treat image generation and object storage upload as separate durable steps, keep the generated artifact, and retry only the upload after a 429 with a capped exponential delay plus jitter. A queue message is not complete until the next state is persisted.

This boundary prevents the most expensive recovery mistake: generating the same image again because the first upload disappeared with a worker process. The queue should carry a job ID, while a ledger records the artifact reference, checksum, object key, attempt count, and next eligible time. A deterministic key such as `images/<job-id>.png` gives a redelivered worker something concrete to reconcile.

Small rule. Persist before acknowledge.

## Why can an AI image exist while its storage job is still incomplete?

The pipeline has at least two side effects. The image service may finish, then the storage request may be rate-limited, time out, or be interrupted after bytes arrive. A single `processing` flag cannot distinguish those cases. On restart, the worker either repeats generation unnecessarily or assumes delivery that never happened.

The failure is easy to misread in an incident timeline. At 10:02 the generation provider records a successful image, at 10:02:01 the worker receives the bytes, and at 10:02:02 the storage endpoint answers 429. If the handler catches that response and acknowledges the message, the queue reports progress while the customer has no object and the artifact may be eligible for cleanup. If the handler retries the whole function, a later delivery can create a second image with a different seed before the first one is ever stored. A durable `generated` record makes the missing interval visible: operators can see that creation succeeded, storage did not, and the next action is an upload of known bytes. That distinction also makes reconciliation safe after a timeout, because the worker can compare the expected checksum instead of guessing from a log line.

Use explicit transitions instead: `queued` -> `generated` after the artifact reference and checksum are durable, then `generated` -> `stored` after the object has been verified. If a process exits between instructions, the next worker reads the ledger and checks the deterministic key. An object with the expected checksum can be marked `stored`; an absent object needs only the upload step. Conditional writes or version checks help with concurrent workers, but the invariant is simpler than any particular storage API: one logical job owns one state record and one destination key.

The catch is operational weight. A one-off batch where a person can rerun a command may not justify a ledger, scheduler, and dead-letter path. Use this design when losing or duplicating an image affects a user, billing, or downstream delivery. Stick with a synchronous call for low-stakes work that has an explicit human retry path.

## How should a Node.js queue handle a 429 after image generation?

Classify the failed operation in the worker, retain the completed artifact, and schedule an upload retry. Honor an upstream wait value when the response provides one. Otherwise use exponential backoff, cap the delay, add jitter, and enforce a finite attempt budget. Retrying immediately only amplifies backpressure.

I once found a 429 loop that acknowledged 17 deliveries while failing to save `run_after`. Queue depth looked healthy until queue age exposed the gap. I'm not sure why the old metric counted an acknowledged attempt as progress; the postmortem action was clear: commit the retry state before returning success to the queue.

The policy below is deliberately independent of a queue client and a storage SDK. The adapter maps a response to `ErrRateLimited` and an optional server-directed delay; the repository owns the durable transition.

```go
package pipeline

import (
    "context"
    "errors"
    "math/rand/v2"
    "time"
)

var ErrRateLimited = errors.New("rate limited")

type Job struct {
    ID             string
    State          string
    Attempt        int
    RunAfter       time.Time
    LastErrorClass string
}

type Repository interface {
    SaveRetry(context.Context, Job) error
    MoveToDeadLetter(context.Context, Job) error
}

func ScheduleUploadRetry(ctx context.Context, repo Repository, job Job,
    cause error, upstreamDelay time.Duration, now time.Time) error {
    if !errors.Is(cause, ErrRateLimited) {
        return cause
    }
    if job.Attempt >= 6 {
        job.State = "dead_letter"
        job.LastErrorClass = "rate_limited"
        return repo.MoveToDeadLetter(ctx, job)
    }

    delay := upstreamDelay
    if delay <= 0 {
        delay = time.Second * time.Duration(1<<job.Attempt)
        if delay > time.Minute {
            delay = time.Minute
        }
        delay += time.Duration(rand.IntN(500)) * time.Millisecond
    }

    job.Attempt++
    job.State = "upload_retry_wait"
    job.RunAfter = now.Add(delay)
    job.LastErrorClass = "rate_limited"
    return repo.SaveRetry(ctx, job)
}
```

The queue acknowledgment follows `SaveRetry` or the terminal transition. Visibility timeouts must cover normal processing or be renewed; otherwise a second worker can receive the same job while the first still owns it. Your mileage may vary on exact caps and attempt counts, but those values belong in configuration and metrics, not in an invisible client default.

## Which failures should retry, pause, or stop?

Do not put every exception in one `catch` path. Generation and upload have different recovery boundaries, and a runbook needs to say what evidence is saved.

| Condition | Durable action | Next action | Signal |
| --- | --- | --- | --- |
| Generation finished; upload not attempted | Save artifact reference and `generated` | Upload retained bytes | Age of `generated` jobs |
| Upload returns 429 | Save attempt and `run_after` | Retry upload after delay | 429 count and retry age |
| Worker exits after upload | Reconcile key and checksum | Mark stored or repeat upload | Jobs stuck in `uploading` |
| Input validation fails | Save terminal failure | Do not retry | Validation failure count |
| Attempt budget is exhausted | Move to dead letter | Review or controlled replay | Dead-letter age |

Monitor oldest runnable job age, counts by state, 429 rate, scheduled retries, upload latency, and dead-letter age. A lower worker concurrency can be the correct response to destination backpressure, provided queued work remains durable. Log identifiers, transitions, durations, and error classes; keep credentials and image bytes out of logs.

Retention is part of reliability. Temporary artifacts consume space, transfers consume bandwidth, and a retry of generation can repeat expensive work. Set retention from the longest credible recovery window and data sensitivity, then test expiry as a failure case. FedRAMP describes a federal authorization assessment context; it does not replace a system-specific threat model, access review, retention policy, or incident plan.

Delivery has its own contract. Set `Content-Disposition` deliberately when serving an object and test whether clients receive `inline` or `attachment` plus the intended filename. An object that exists but cannot be delivered predictably is still an unfinished pipeline result.

## What should verification and rollback prove?

Before deployment, use fake generation and storage adapters. Make generation succeed and upload return a classified 429; assert that the artifact reference is unchanged, `run_after` advances, and no acknowledgment occurs before the retry record is durable. Kill a worker immediately after writing bytes. On redelivery, checksum reconciliation should mark the job stored without another generation call. Deliver the same job to two workers and verify that a conditional transition leaves one owner.

Release with a concurrency limit and watch state age rather than process uptime. Sample each terminal state: the generated checksum should match the stored object, the recorded key should be available to the application, and dead-letter entries should retain enough reason data for controlled replay.

Rollback means stopping new workers without deleting queued jobs or temporary artifacts. Restore an older worker only if it understands the same ledger states; otherwise pause consumption and migrate state first. Do not bulk-requeue dead letters during rollback. Reconcile one state at a time, starting with objects that may already exist, because blind replay can turn an upload incident into duplicate generation.

A queued pipeline is not suitable when the team cannot operate its reconciliation and durability paths. Narrow the workflow until ownership is clear. The release criterion is plain: every completed side effect remains discoverable after a worker disappears, and every retry identifies exactly which side effect it will repeat.

## References

- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Disposition
- https://www.fedramp.gov/
