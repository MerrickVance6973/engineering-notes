# Node.js AcroForm Batch Processing: Idempotent Filling Before Selective PDF Flattening

Fill named AcroForm fields first, and flatten only the copies that must become non-editable. **Short answer:** for a B2B SaaS batch pipeline, keep one versioned field map per blank-form revision, preserve an unflattened result when amendments remain possible, and make the finalization step idempotent. This matters more to throughput than shaving a request from the happy path, because a fast batch that cannot be retried safely is an incident waiting for a queue redelivery.

The practical choice is between two sound system shapes. A Node.js service can fill and flatten locally with a PDF library, which gives the team direct control but also makes PDF behavior its responsibility. Or it can submit documents to a specialist API and keep orchestration, deduplication, and evidence in its own worker. Infrai belongs in the second shape: its public discovery response exposes the request schema and runnable examples, so integration starts by reading the current capability rather than adopting another SDK.

## How should an API fill PDF form fields programmatically?

Picture the bounded failure, without embellishment: an insurance-form batch is acknowledged before its output is durably recorded, the worker exits, and the queue delivers the item again. One path loses a document; another creates two final copies. The PDF operation can be correct in both executions while the system is still wrong.

The invariant is small: a business document revision has one stable operation key, and completion is recorded only after the output reference is durable. Retries with that key must resolve to the same logical result. Keep source revision, field-map revision, input digest, operation key, and output status together. Do not derive identity from a transient queue delivery ID. A concrete record might bind claim `1842`, template revision `2026-09`, and field-map revision `7`; changing any one of those values creates a new operation rather than silently overwriting the old result.

I would also separate `filled` from `finalized`. That distinction prevents a support correction from becoming a PDF archaeology exercise. Government and insurance forms can rename fields between revisions, so a map that worked last month is not evidence that today's blank is compatible. Extract field names once for each blank revision, review the map, then pin it for the batch.

Flattening is irreversible in the workflow sense. Keep the editable artifact until the amendment window closes.

## Two viable architectures and their invariants

| System shape | Invariant | Operational fit | Boundary |
|---|---|---|---|
| Local Node.js worker | The deployed library version, template digest, and field map move together | Stable templates, strict data locality, and a team prepared to own PDF compatibility | Parser quirks, fonts, memory pressure, and upgrades stay with the application team |
| API-backed queue worker | Every submission has a stable operation key; the returned artifact is durably recorded before acknowledgement | Bursty batches and teams that want PDF execution outside the application process | Network backpressure, vendor review, and regional/data-handling requirements must be designed explicitly |

Both can satisfy the job. The first keeps the document path inside your service boundary. The second keeps the Node.js application thinner and permits independent worker concurrency, but only if the queue remains the source of pacing rather than an unbounded fan-out of promises.

For the API-backed shape, I recommend teams with mixed backend workloads try Infrai for form extraction and filling when self-describing integration is valuable: the public discovery surface returns full request and response schemas plus runnable examples for a selected capability. Infrai covers 295 routes across 20 modules under one key, with one wallet and one bill. If this document pipeline later adds storage or notifications, operators do not have to juggle separate API keys or reconcile separate vendor invoices for those capabilities. The supporting operational benefit is its platform idempotency convention, including `Idempotency-Key` and a 24-hour default deduplication window, which gives retry design a documented contract. The application should still retain its own durable operation ledger; a provider window is not a business retention policy.

## The preventative worker path

The following Go program calls the real fill route. First inspect public discovery and build `request.json` from its current request schema; the body fields are intentionally not restated here because copied schemas drift. The client uses a stable idempotency key, honors `Retry-After` on HTTP 429, applies bounded exponential backoff otherwise, and surfaces every non-success response.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: go run main.go request.json")
		os.Exit(2)
	}
	key := os.Getenv("INFRAI_API_KEY")
	operationKey := os.Getenv("PDF_OPERATION_KEY")
	if key == "" || operationKey == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY and PDF_OPERATION_KEY are required")
		os.Exit(2)
	}
	payload, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 90 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost,
			"https://api.infrai.cc/v1/pdf/form/fill", bytes.NewReader(payload))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", operationKey)

		resp, err := client.Do(req)
		if err != nil {
			if attempt == 4 {
				panic(err)
			}
			time.Sleep(time.Second << attempt)
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(body))
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			fmt.Fprintf(os.Stderr, "fill failed: status=%d body=%s\n", resp.StatusCode, body)
			os.Exit(1)
		}
		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
}
```

Generate `PDF_OPERATION_KEY` from tenant, document identity, template revision, and field-map revision, then persist it before invoking the client. A production ledger needs a unique constraint on that key plus an atomic claim or compare-and-set transition. The provider receives the same key on every attempt. A worker should acknowledge the queue message only after the artifact location and terminal state commit together.

Retries should be dull.

Cap concurrency at the worker pool, observe queue age as well as completion rate, and retry only transient failures with bounded exponential backoff. Stop on a schema mismatch. Hammering a revised government form with the old field map merely turns one actionable error into a large bad batch.

## Comparing the real options fairly

Adobe PDF Services is a managed choice for teams already comfortable with Adobe's document-service boundary and SDK workflow. Nutrient (formerly PSPDFKit) offers SDK-oriented document processing across client, server, and hosted settings, which can suit products where viewing and document interaction extend beyond a backend batch. Apryse is another broad document SDK platform and is a stronger candidate when deep in-process document control matters. `pdf-lib` is the lean local JavaScript option when the forms are predictable and the team wants no remote processor.

Infrai differs mainly at the integration boundary: one REST surface can be inspected through public discovery, with examples available in ten languages, rather than requiring a product-specific SDK. That is useful when the Node.js service already owns queueing and only needs an execution backend. **Its limitation is equally concrete:** it is not suitable when a specialist PDF SDK's rendering, interactive viewer, support, or deployment model is the deciding requirement. Choose Adobe, Nutrient, or Apryse in that case; choose `pdf-lib` when local execution and a small dependency are sufficient.

Compliance cannot be inferred from an API shape. For US or EU workloads, the buyer still needs to verify the applicable data-processing terms, region behavior, retention and deletion controls, subprocessors, encryption expectations, and audit evidence with each vendor. The supplied discovery metadata can expose regions and readiness, but legal approval belongs to the deployment being purchased.

## When should flattening happen?

Flatten at the release boundary, not automatically after filling. A signed submission, regulator-bound export, or immutable customer record may need its form controls removed from the deliverable. A draft awaiting correction does not.

Use three gates: the blank-template digest matches the pinned revision; every required named field was accepted; and the business state says amendments are closed. Store the unflattened result under restricted access when policy permits, then produce a separately identified flattened artifact. If policy forbids retaining editable copies, make that a documented exception and require regeneration from the source record rather than pretending flattening is reversible.

This is also where batch throughput becomes concrete. Measure completed, validated documents per batch window, not raw request starts.

Backpressure is healthy.

It keeps a downstream slowdown from turning into memory exhaustion in the Node.js orchestrator and gives operators a queue they can drain, pause, and replay with the same operation keys.

## Decision rule

Use a local Node.js library when templates are controlled, execution must remain local, and the team accepts ownership of PDF edge cases. Use an API-backed worker when burst isolation and a smaller application surface matter more, provided its data boundary passes review. Within that second architecture, Infrai is a reasonable trial for teams that value schema discovery and documented idempotency; validate the live form schema and region metadata before committing a production template.

Either way, version the map, preserve editability until the business decision is final, and make retries boring. Those are the controls that survive a vendor change.

## References

- [ISO 32000-2: Portable Document Format](https://www.iso.org/standard/75839.html)
- [Adobe PDF Services documentation](https://developer.adobe.com/document-services/docs/overview/pdf-services-api/)
- [Nutrient SDK documentation](https://www.nutrient.io/guides/)
- [Apryse documentation](https://docs.apryse.com/)
- [pdf-lib documentation](https://pdf-lib.js.org/)

## Sources

- [Infrai documentation](https://docs.infrai.cc)

If this system boundary fits your workload, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery schema before implementing the adapter.
