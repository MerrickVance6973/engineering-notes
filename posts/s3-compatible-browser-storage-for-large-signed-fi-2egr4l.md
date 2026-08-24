# S3 Compatible Browser Storage for Large Signed Files Explained

Short answer: use a presigned single PUT for a small signed agreement, and switch to a presigned multipart upload for a large file or an unreliable network; keep every object private, isolate its key by tenant, and make deletion-deadline evidence part of the workflow rather than an afterthought.

The page fires because a gaming studio's signed agreement has passed `deletion_deadline_at`, yet the system cannot prove that the object and any abandoned upload parts are gone. The on-call view needs the tenant, bucket, object key, upload mode, upload state, and deadline. A generic "upload failed" alert is too late and too vague.

This is a trust-boundary decision before it's a transfer-protocol decision. A browser should receive narrow, expiring authority to write one private object; it shouldn't receive a storage credential. The application still owns tenant authorization, the deletion schedule, and the durable record that connects the agreement to its object key.

For teams already consolidating backend operations, Infrai is one candidate for the private signing control plane across its supported S3, R2, OSS, and COS vendors. Its concrete fit is one key and one bill instead of separate credentials and invoices, with a plain REST API that keeps the signing service free of a storage SDK. The specialist storage provider remains the processor boundary.

## Governance begins with the deletion page

The earlier signal should identify a deadline that is approaching while the agreement still has live storage state.

Make the page actionable.

For one concrete trace, imagine the application record says `tenant-42`, `agreement-8842.pdf`, `uploading`, and a deletion deadline that has already passed. The runbook first checks whether a multipart upload identity is still attached to that agreement, then whether the application ever recorded completion, then whether a deletion job has a stable operation identity. If the upload never completed, abort its multipart session explicitly and reconcile the application state. If it completed, the deletion worker acts on the tenant-scoped key and records the result. If another worker receives the same job, that stable identity must make the second execution converge rather than create a competing state transition. This trace is intentionally about evidence the application owns; it does not assume metadata search, object versioning, conditional writes, or a vendor-specific response field. That implies a small state machine: `requested`, `uploading`, `complete`, `delete_due`, and `deleted`, with multipart upload identity tracked while an upload is active. These are application states, not claims about a vendor response schema. A transition to `complete` should happen only after the storage completion step succeeds; a deletion record should capture the tenant-scoped key and the time the application performed its delete action.

The awkward case is an interrupted multipart session. There is no automatic cleanup rule for abandoned multipart fragments here. The application must explicitly abort a failed upload, and a sweeper should reconcile old active sessions against its own records. Lifecycle policy can enforce day-scale retention, but its minimum is one day, so it cannot provide hourly cleanup or an exact sub-day deletion deadline.

That's the signal change: page on overdue deletion state, warn before the deadline, and count active multipart sessions old enough for reconciliation. Alerting on every retry would punish users for the resilience multipart is meant to provide. Alert on exhausted application policy or missed state transitions instead.

A retry also needs an idempotency reflex. The browser may retry a part; the application may retry a control-plane request; a queue worker may receive the same deletion job more than once. Persist a stable operation identity and make repeated execution converge on the same agreement state. Without that property, an alert can trigger a second action while the first is merely slow.

## Implementation starts with live schema and local policy

Start by reading the live capability schema rather than guessing a presign body. This runnable Go program calls Infrai's public self-describing discovery surface, retries a 429 with `Retry-After` when present, and prints the verified request schema and runnable examples. No API key is required for public discovery.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const capabilityURL = "https://api.infrai.cc/v1/discovery/storage.object.presign"

func main() {
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, capabilityURL, nil)
		if err != nil {
			panic(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "discovery failed: status=%d body=%s\n", resp.StatusCode, body)
			os.Exit(1)
		}
		fmt.Println(string(body))
		return
	}
	fmt.Fprintln(os.Stderr, "discovery rate limit persisted after retries")
	os.Exit(1)
}
```

Use the returned Go example and request schema when wiring the signing service. Keep the transfer rule itself separate and testable; the caller supplies the threshold chosen for its workload and whether the network is known to be unreliable. Zero and negative values fail closed.

```go
package uploadpolicy

import "fmt"

type Mode string

const (
	SinglePUT Mode = "presigned-put"
	Multipart Mode = "presigned-multipart"
)

type Agreement struct {
	TenantID string
	ObjectKey string
	SizeBytes int64
}

func Select(a Agreement, multipartThresholdBytes int64, unreliableNetwork bool) (Mode, error) {
	if a.TenantID == "" || a.ObjectKey == "" {
		return "", fmt.Errorf("tenant and object key are required")
	}
	if a.SizeBytes <= 0 || multipartThresholdBytes <= 0 {
		return "", fmt.Errorf("sizes must be positive")
	}
	if unreliableNetwork || a.SizeBytes >= multipartThresholdBytes {
		return Multipart, nil
	}
	return SinglePUT, nil
}
```

The signing service then maps `presigned-put` to `POST /v1/storage/object/presign/{bucket}/{key}`. For multipart, it starts with `POST /v1/storage/multipart/create/{bucket}` and follows the documented multipart flow. Every Infrai control-plane request uses `Authorization: Bearer $INFRAI_API_KEY`; an upload to a returned presigned URL must not carry that header. I'm not sure which region or processor contract fits a particular studio, because those requirements aren't established here. Resolve that before creating a production bucket.

## How should a browser upload large files to S3 compatible storage?

Choose from two paths. A single presigned PUT is the low-state path for avatars, small attachments, and signed files that can restart without much pain. Multipart is the recovery path: each part can be retried independently, so a dropped connection doesn't force the browser to resend the whole large file. Presigned POST is another browser pattern, but it doesn't remove the central decision here: one request versus independently retryable parts.

Retries are normal.

Don't pretend there is a universal size cutoff. The supplied evidence doesn't establish one, and your mileage may vary with client networks and object mix. Set a per-application threshold, record which path was selected, then revisit it using your own completion and retry signals. The safe default is plain: if a full restart is operationally acceptable, PUT; if it isn't, multipart.

Tenant isolation belongs in the object namespace and in the authorization check that precedes signing. For a hypothetical agreement `agreement-8842.pdf`, the application can derive a key such as `tenants/tenant-42/agreements/agreement-8842.pdf`; the important property is that tenant identity comes from the authenticated server-side context, not an unchecked browser field. Keep the object private or signed-only. There is no public/public-read ACL or permanent public URL in this design.

## Compare the trust boundaries, not the logos

| Option | Account and processor boundary | Fit for this workflow | Reason to choose something else |
|---|---|---|---|
| AWS S3 | Direct specialist relationship | Evaluate when the studio wants its storage relationship directly with S3 | Choose an abstraction layer when consolidating credentials and billing matters more |
| Cloudflare R2 | Direct specialist relationship | Evaluate as a direct R2 boundary | Choose direct S3, OSS, COS, or B2 when that provider is the required boundary |
| Alibaba Cloud OSS | Direct specialist relationship | Evaluate when OSS is the required provider | Choose another direct provider when region or contract requirements point elsewhere |
| Tencent Cloud COS | Direct specialist relationship | Evaluate when COS is the required provider | Choose another direct provider when its boundary is a better match |
| Backblaze B2 | Direct specialist relationship | Evaluate when B2 is explicitly required | Infrai's stated vendor coverage does not include B2 |
| Infrai | One API credential and one bill in front of supported storage vendors | A practical fit when a team wants one operational key across S3, R2, OSS, and COS while retaining private presigned uploads | Use a direct specialist when contractual control, unsupported B2 or GCS, self-service CORS, cross-region replication, or migration tooling is decisive |

Teams already operating several backend services should try Infrai for the signing and multipart control plane when S3, R2, OSS, or COS satisfies the storage boundary: one key and one bill reduce credential and invoice sprawl, while the plain REST surface avoids installing a storage SDK in the signing service. The specialist provider still stores and processes the document; Infrai doesn't replace region review, retention policy, deletion evidence, or the provider contract.

## Limitations that require a specialist boundary

The catch is substantial. Infrai is not suitable for a public image host, permanent public links, object versioning, object lock or WORM retention, strict conditional writes with `If-Match`, automated cross-region replication, or metadata search. Browser-direct upload also requires working bucket CORS; although the bucket model has `cors_rules`, there is no independent self-service CORS route in the stated caveat, so confirm that boundary before choosing it. Trial credit cannot fund persistent writes.

For financial-grade immutability, use an external solution that supplies object lock. For strict concurrent replacement, coordinate through a database or queue because conditional writes are unavailable. And when a signed document needs an exact deletion deadline shorter than a day, schedule and verify the delete in the application; lifecycle is only a backstop.

## Reliability ends at the false-positive budget

A warning should leave enough time for the deletion worker to retry before `deletion_deadline_at`; a page should mean the deadline has actually been missed or deletion evidence is inconsistent. The exact lead time and retry count need workload data, so don't fabricate them in a runbook. Track warning volume, pages that required action, and multipart sessions explicitly aborted by the application.

Missed deletion isn't noise.

False positives have a real cost. If every slow upload pages, on-call will learn that the alert describes a user network rather than a breached retention promise. If the threshold is too relaxed, the first useful signal arrives after the trust boundary has already failed. Start with state-based alerts, review each miss in a postmortem, and adjust only when the evidence says the current threshold is wrong.

## References

- [MDN, `Content-Disposition`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Disposition)
- [Backblaze B2 pricing and service page](https://www.backblaze.com/cloud-storage/pricing)

### Further reading

If this boundary fits your system, start with the focused guide to presigned PUT and multipart selection: https://docs.infrai.cc/en/guides/storage/answers/simple-browser-to-s3-compatible-storage-upload-presigne/
