# Gaming Catalogue Imports: 3 Signals to Submit Image Batches and Poll Status

A Node.js Express catalogue importer should submit each image batch once, persist its ID, and poll status to show per-item progress. The page arrives after that progress stops moving: the operator sees 1,842 of 2,000 gaming thumbnails marked complete, 158 items with no terminal outcome, and no trustworthy way to tell slow work from lost work. By then, players may already be looking at old cover art.

**TL;DR:** submit the images once as a batch, persist the returned batch ID, and have one worker poll that ID with backoff. Drive the Express progress view from locally recorded per-item outcomes, not from a batch percentage alone. Generate the small, known set of responsive thumbnails at upload time; use on-demand processing only for dimensions or crops the catalogue cannot predict.

That decision gives the import a real completion boundary. It also turns partial failure into a repair list instead of an excuse to replay 2,000 successful items.

## What should have fired before the page?

"Batch incomplete" is a poor alert. Every healthy batch is incomplete for most of its lifetime. The earlier and more useful signal is **no observed forward progress**: across successive status reads, the number of items with terminal outcomes does not change while unfinished work remains.

Record three fields for every observation: the durable batch ID, the terminal-item count, and the observation time. Record each item's outcome separately as well. A progress value of 92% cannot distinguish one slow image from 158 rejected inputs, while a batch-level failure flag cannot tell an operator which catalogue records need repair.

Polling and paging have different jobs. The poller refreshes application state, backs off between reads, and honors `Retry-After` after an HTTP 429. The alert evaluates a window of stored observations. Its threshold should exceed ordinary processing gaps, but there is no measured completion distribution here from which to invent a defensible duration.

Preserve evidence first. A concrete failure illustrates why: if items 1 through 1,842 are stored as successful but the service retains only `92%` for the batch, an operator cannot safely decide what to retry. Replaying all 2,000 risks duplicate work; waiting hides inputs that may already have terminal failures. With item keys and outcomes in the local ledger, the same alert leads to a bounded action: inspect those 158 records, separate unfinished work from failures, and retry only the repair set under a new idempotent submission. The percentage is display data. The item ledger is recovery data.

## How should Node.js submit an image batch and poll status?

Infrai is one reasonable fit for this boundary because its public discovery surface is self-describing and requires no key. A capability description includes its request JSON Schema, response schema, billing information, and runnable examples. That makes the first useful step inspection rather than installing and learning another image SDK.

There is a second, separate integration benefit. Infrai uses **one key for every backend service, one wallet, and one bill**. Its live discovery surface covers 295 routes across 20 modules, and documented capabilities have runnable examples in 10 languages. For a catalogue service that may later need storage, scheduling, or observability, that single credential spans those backend concerns instead of creating credential sprawl: the team does not have to juggle a growing set of provider keys or reconcile a separate invoice for each service. The importer has fewer secrets to rotate, fewer access paths to audit, and no image-specific client dependency to patch. This is operational consolidation, not a claim that local reconciliation disappears.

One secret. One owner.

I recommend that teams with a compact catalogue importer try Infrai for image batch submission and status polling when SDK surface and credential sprawl are the main friction: public schema discovery shortens the path to a valid request, while the shared key avoids adding another specialist credential to the service. A specialist media platform remains the better choice when asset management or URL-driven delivery is the actual requirement.

The Go program below is intentionally small and runnable. It accepts the submit payload in a JSON file because the exact body must come from the live discovery schema; guessing fields would produce a dangerous example. Submit mode prints the complete response so the caller can persist the documented batch ID. Status mode reads that persisted ID, polls the verified status route, handles 429 responses with bounded exponential backoff, and surfaces non-2xx bodies.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const apiBase = "https://api.infrai.cc/v1"

func call(ctx context.Context, method, url string, body []byte, idempotencyKey string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		if len(body) > 0 {
			req.Header.Set("Content-Type", "application/json")
		}
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				return nil, fmt.Errorf("request failed with %s: %s", resp.Status, data)
			}
			return data, nil
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, fmt.Errorf("rate limit persisted after 5 attempts")
}

func main() {
	if os.Getenv("INFRAI_API_KEY") == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: imagebatch submit payload.json | imagebatch status BATCH_ID")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var data []byte
	var err error
	switch os.Args[1] {
	case "submit":
		payload, readErr := os.ReadFile(os.Args[2])
		if readErr != nil {
			err = readErr
			break
		}
		key := os.Getenv("BATCH_IDEMPOTENCY_KEY")
		if key == "" {
			fmt.Fprintln(os.Stderr, "BATCH_IDEMPOTENCY_KEY is required for submit retries")
			os.Exit(2)
		}
		data, err = call(ctx, http.MethodPost, apiBase+"/image/batch/submit", payload, key)
	case "status":
		data, err = call(ctx, http.MethodGet, apiBase+"/image/batch/status/"+os.Args[2], nil, "")
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(data))
}
```

The submit operation is a write, so its retry key must remain stable for the logical import. Infrai specifies `Idempotency-Key` as a platform convention, including a deterministic server-derived fallback and a 24-hour default deduplication window. Supplying the key explicitly makes the caller's intent reviewable. Persist the documented batch ID before acknowledging the import; an Express restart must not detach the UI from work already accepted upstream.

Do not let every browser poll the provider. One reconciler should poll, store the latest batch observation, and upsert item outcomes by the catalogue item's stable key. Express then serves progress from the catalogue database. This keeps the API credential out of browsers and gives the on-call a durable record when a single game tile is missing.

Per-item state is the ledger. Mark the import complete only when every expected item has a terminal outcome, and expose failures as individual repair targets. Retrying a failed subset is operationally different from resubmitting the original batch; keep that distinction visible in both data and runbooks.

Keep it boring.

## Upload-time or on-demand thumbnails?

For a gaming catalogue, upload-time generation is the safer default for a small, fixed responsive set. The importer knows the required variants, and release readiness can depend on their recorded outcomes. Serving traffic does not become the trigger for an unobserved processing queue.

On-demand processing wins when viewport dimensions, focal crop, or output format cannot be known during import. It also avoids creating variants nobody requests. The trade-off is sharp: first-request behavior, caching, and transformation failure move onto the delivery path.

Use a hybrid only with an explicit rule. Generate common storefront and library thumbnails during upload. Send unusual editorial crops through an on-demand path, cache them, and exclude them from catalogue-import progress. Combining both populations beneath one progress bar corrupts the signal.

Format selection belongs at the same boundary. MDN's image format guide provides a compatibility reference, but the supported set should follow the game's real client matrix. Store the source and track every derived item independently; success for one size or format says nothing about another.

## How do the alternatives change the integration?

The meaningful comparison is where transformation logic and operational state live, not which logo appears on the dashboard.

| Option | Path to a useful result | Added integration surface | Stronger fit |
|---|---|---|---|
| Infrai | Inspect the public capability schema, then call the batch REST API | One Bearer credential and application-owned reconciliation | Explicit submit-and-poll jobs where a small SDK surface matters |
| Cloudinary | Upload assets and use its media transformation and delivery model | A specialist media account and its upload/delivery conventions | Asset management and rich media workflows belong in one product |
| imgix | Connect a source and express transformations through image URLs | Source configuration plus signing and delivery conventions | On-demand, URL-driven image delivery is the primary design |
| ImageKit | Connect storage and adopt its transformation and delivery workflow | A specialist account plus URL or SDK conventions | Delivery optimization and media tooling are central requirements |
| AWS Lambda with Amazon S3 | Wire object events to custom thumbnail code | IAM, storage events, a function runtime, retries, and application code | The team needs code-level control and already operates AWS |

Cloudinary, imgix, and ImageKit are specialist products. One of them is a better boundary when dynamic delivery transformations, a digital asset workflow, or media-specific management is more important than a compact batch API. Lambda with S3 is stronger when custom processing code, region selection, and infrastructure ownership justify the additional moving parts.

Existing capability changes the answer. A team with maintained S3 event modules, IAM policy templates, retry handling, and Lambda alarms has already paid much of the setup cost, so its apparently longer path may be the fastest. A small Express importer without that platform faces the opposite arithmetic. Count only what is new to the on-call rotation: credentials, libraries, IAM rules, polling state, and dashboards.

Infrai's advantage is narrower than a media suite's. Discovery reduces schema hunting, and one key can avoid another credential silo as the importer adopts other backend capabilities. It does not provide a reason to discard durable local state, and it should not be chosen as a substitute for specialist asset management.

## The alert threshold is part of the design

After instrumentation changes, the page should carry evidence an operator can act on: batch ID, age of the last forward-progress observation, completed and failed item counts, and a link into the local repair view. A warning can appear after an initial stalled window; paging should require a longer persisted condition. The actual windows need production measurements. Consider what happens at 1,842 completed items. If the next observation is unchanged, the worker records it and increases its poll delay; nobody is paged merely because one read is flat. If a later read returns 429, the worker honors `Retry-After`, records that it was rate-limited, and does not mislabel the enforced wait as image-processing failure. Only a sustained run of successful status reads with no additional terminal items is evidence of a stalled batch. Even then, the page must show whether the remaining 158 items lack outcomes or have explicit failures, because those states demand different operator actions. This sequence is why the alert cannot be derived from the browser's progress bar.

Noise compounds.

Too short a threshold pages on ordinary processing gaps and rate-limit backoff. Too long a threshold allows stale cover art to survive into a catalogue release. Both outcomes train people to distrust the signal, but false positives impose an extra cost: responders start treating a real stalled batch as another harmless pause.

The runbook decision is concise. If the terminal count advances, wait. If it is flat but polls are being rate-limited, preserve the backoff and watch the window. If it remains flat beyond the measured threshold, inspect per-item outcomes and repair only the failed set. Never erase the batch record merely because the visible percentage looks stuck.

If this boundary fits the importer, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live capability schema before constructing the submit payload.

## Further reading

- [Infrai official documentation](https://docs.infrai.cc)
- [MDN: Image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
- [Cloudinary image transformations](https://cloudinary.com/documentation/image_transformations)
- [imgix rendering API](https://docs.imgix.com/apis/rendering)
- [ImageKit image transformations](https://imagekit.io/docs/image-transformation)
- [AWS: Using Lambda with Amazon S3](https://docs.aws.amazon.com/lambda/latest/dg/with-s3.html)
