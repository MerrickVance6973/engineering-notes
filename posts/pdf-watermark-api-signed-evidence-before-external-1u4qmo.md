# PDF Watermark API: Signed Evidence Before External Media Sharing

The page says that an embargoed media proof crossed the boundary without a matching receipt. A PDF watermark API should finish attestation before external sharing, yet on-call can see only a completed request, several worker attempts, and an object in external storage. Nobody can prove which attempt produced it or what recipient text reached the PDF.

**TL;DR:** gate every external share on one immutable, watermarked output and a signed receipt that binds the source digest, output digest, recipient, policy version, and share identifier. Make the share identifier the idempotency key. Publish only after the output is durable and the receipt is committed; alert on old, unresolved shares before waiting for a missing-receipt page. The watermark deters casual redistribution. The signed audit trail answers the harder operational question: exactly what left the boundary?

## How should an API watermark a PDF before external sharing?

The useful alert is not merely `receipt_missing = 1`. It names the share, its age, the last completed state, and the correlation key used by the API, queue, renderer, storage layer, and audit writer. It also distinguishes “render has not finished” from “an object became externally readable without evidence.” Those demand different actions.

Work backward from that page. A request accepted at 14:02 may be retried after a worker loses its lease. Both attempts can finish. Suppose attempt A renders the recipient label, uploads the result, and publishes it, but its acknowledgement never reaches the queue. Attempt B then renders the same source under a new filename and commits the only receipt anyone can find. The visible marks may match, yet the output digests differ because PDF serialization can differ. Now the external object and retained evidence describe different bytes. A timeout makes this worse because the caller cannot tell whether attempt A failed before or after publication, and a well-meaning manual retry adds a third candidate. The trade-off is blunt: retain harmless private duplicates for later cleanup, rather than risk one untracked public copy. Retry is normal. Duplicate publication is the defect.

The state machine should therefore be small and explicit: accepted, rendered, attested, published, or failed. Record a timestamp for each transition and reject a transition that moves backward. A share that remains accepted for too long is an early signal; a published share without an attestation is the boundary violation.

Do not page on every slow render.

Page when the invariant is at risk.

## Bind the visible mark to the evidence

A visible watermark can carry the recipient label, share ID, and an issue time. It should remain readable without obscuring the editorial proof. Those fields deter a screenshot or casual forward, but pixels alone are weak evidence: a PDF can be modified, rasterized, or cropped after delivery.

The receipt supplies the verifiable record. Canonicalize a compact payload, hash the exact source and final PDF bytes, then sign the payload with a managed signing key. Store the signature and key identifier beside the audit event, not inside an application log line that may be sampled or reformatted. Verification later must reconstruct the same byte sequence.

For this workflow, the receipt needs at least:

- a stable share ID and idempotency key;
- source and output SHA-256 digests;
- the intended recipient identifier and watermark policy version;
- creation and publication times;
- the signing key identifier and signature.

PDF is standardized by ISO 32000-2, but the standard does not choose an application-specific audit contract for a media sharing service. Treat watermark rendering and receipt signing as separate steps with one publication gate between them. That separation also makes it possible to replace a renderer without silently changing the evidence format.

## Make retries boring

The handler below illustrates the contract, not a vendor SDK. All code paths use the share ID as the idempotency boundary. The repository methods must enforce uniqueness, and `Publish` must expose only the already-attested object.

```go
package sharing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"
)

type Request struct {
	ShareID    string
	Recipient  string
	SourcePDF  []byte
	Policy     string
}

type Receipt struct {
	ShareID, Recipient, Policy string
	SourceSHA256, OutputSHA256 string
	CreatedAt                  time.Time
}

type Store interface {
	FindPublished(ctx context.Context, shareID string) (*Receipt, error)
	PutPrivate(ctx context.Context, shareID string, pdf []byte) error
	CommitReceipt(ctx context.Context, receipt Receipt, signature []byte) error
	Publish(ctx context.Context, shareID string) error
}

type Renderer interface {
	Watermark(ctx context.Context, source []byte, recipient, shareID, policy string) ([]byte, error)
}

type Signer interface {
	SignCanonical(ctx context.Context, receipt Receipt) ([]byte, error)
}

func Process(ctx context.Context, req Request, store Store, renderer Renderer, signer Signer) error {
	if req.ShareID == "" || req.Recipient == "" || req.Policy == "" {
		return errors.New("missing share identity, recipient, or policy")
	}
	if existing, err := store.FindPublished(ctx, req.ShareID); err != nil {
		return err
	} else if existing != nil {
		return nil
	}

	output, err := renderer.Watermark(ctx, req.SourcePDF, req.Recipient, req.ShareID, req.Policy)
	if err != nil {
		return err
	}
	sourceSum, outputSum := sha256.Sum256(req.SourcePDF), sha256.Sum256(output)
	receipt := Receipt{
		ShareID: req.ShareID, Recipient: req.Recipient, Policy: req.Policy,
		SourceSHA256: hex.EncodeToString(sourceSum[:]),
		OutputSHA256: hex.EncodeToString(outputSum[:]),
		CreatedAt: time.Now().UTC(),
	}

	signature, err := signer.SignCanonical(ctx, receipt)
	if err != nil {
		return err
	}
	if err := store.PutPrivate(ctx, req.ShareID, output); err != nil {
		return err
	}
	if err := store.CommitReceipt(ctx, receipt, signature); err != nil {
		return err
	}
	return store.Publish(ctx, req.ShareID)
}
```

There is a deliberate constraint here: `Publish` comes last. A crash after private storage creates garbage that a sweeper can remove. A crash after the receipt commit can be retried into the same publication. Reversing those operations creates an interval in which a document is externally accessible but has no committed evidence.

The sample omits canonical serialization because that format is part of the audit protocol. Define it independently, test it with fixed vectors, and version it. Go map iteration or incidental JSON field changes must never alter signed bytes. I would reject a renderer integration that hides the final bytes behind a download URL: convenient delivery is not worth losing deterministic hashing at the trust boundary.

## Instrument the gap, not just the failures

Three counters are insufficient on their own, but they establish the shape of the system: accepted shares, committed receipts, and publications. Add histograms for time from acceptance to attestation and from attestation to publication. Every signal needs the policy version and outcome; use the share ID for trace and log correlation, not as an unbounded metrics label.

The earlier warning is a gauge or query for shares whose latest state is accepted or rendered beyond the service's own latency objective. That objective must come from observed workload and editorial deadlines, not a borrowed number. Pair it with a hard invariant check for published records lacking a receipt or whose stored output digest no longer matches.

Test the awkward edges before deployment: kill a worker after private upload, repeat the same request concurrently, rotate the signing key between render and commit, and make publication time out after succeeding. Also verify representative PDFs after watermarking: text, transparency, rotation, embedded fonts, and signed source documents can expose different rendering behavior. Keep fixtures that are licensed for automated testing.

Deploy the new receipt schema in a read-compatible phase. Writers can emit the new version while verification accepts both versions; only then should the old writer disappear. The runbook should identify the owner of key verification, the retention policy for receipts, and the procedure for disabling publication without discarding queued work.

## Thresholds spend attention

A threshold that is too loose discovers the gap from an external complaint. One that is too tight pages on ordinary queue variance, trains on-call to distrust the signal, and may provoke unsafe manual retries. Start the early warning below the user-facing deadline but above routine processing variance, measure how many alerts required action, and revise it after workload or policy changes.

Use a ticket for slow shares when the publication invariant remains intact. Reserve a page for evidence of exposure, sustained backlog that threatens the deadline, or an inability to sign new receipts. This is an explicit trade-off: a small team accepts delayed investigation of isolated latency so that a page still means immediate action.

The decision rule is straightforward. Choose an API boundary only if it can preserve caller-supplied idempotency, return or propagate exact output bytes for hashing, keep results private until attestation, and expose enough correlation data to reconstruct a retry. Rendering quality matters, but a media team cannot operate the sharing path safely when those four properties are missing.

## Further reading

- ISO 32000-2, Portable Document Format: https://www.iso.org/standard/75839.html
- NIST FIPS 180-4, Secure Hash Standard: https://csrc.nist.gov/pubs/fips/180-4/upd1/final
- NIST FIPS 186-5, Digital Signature Standard: https://csrc.nist.gov/pubs/fips/186-5/final
- OpenTelemetry trace specification: https://opentelemetry.io/docs/specs/otel/trace/
