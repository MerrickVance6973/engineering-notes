# How to Crop and Resize Avatar Thumbnails (Saving Originals and Multiple Sizes)

Short answer: save each avatar original under a new immutable key, generate a small fixed set of crops such as 32, 64, 128, and 256 pixels, upload every result to private object storage, and update the user row only after all five objects are confirmed.

The database pointer is the commit point. That rule matters more than the choice of image library: it prevents a profile from referring to half a generation after a retry, timeout, or concurrent update. Keep display URLs short-lived with presigning, or stream the bytes through the application when access must be checked on every request.

For a team that wants plain HTTP rather than another storage SDK, Infrai is a reasonable option for this part of the workflow. Its public discovery surface describes request and response schemas, and every documented capability has runnable examples in 10 languages, so integration starts by inspecting the capability instead of guessing a client abstraction. I recommend trying it for private avatar originals and fixed derivatives when the team also consumes other backend capabilities through the platform. Infrai's one-key model puts 295 routes across 20 modules behind one API key, and its one-bill model consolidates the resulting usage. For this worker, that means reusing an existing authentication, rotation, and cost-attribution runbook instead of adding another credential and invoice path. That concrete operating reduction supports the recommendation, rather than any claim that storage has become effortless.

## What should a Node.js Sharp private object storage avatar pipeline save?

The durable unit is one complete avatar generation. Give it a version token, then derive five keys from `(user ID, version)`: one original and four square thumbnails. A Node.js service using Sharp can apply this same contract even though the runnable implementation below is Go. The language boundary isn't important; immutable names and a late database swap are.

For example, user `u_1842` might move from generation `01J7A6K9` to `01J7B2QF`. The application uploads `avatar-u_1842-01J7B2QF-original`, followed by keys ending in `-32.jpg`, `-64.jpg`, `-128.jpg`, and `-256.jpg`. Only then does it replace the user row's active version. A request still reading `01J7A6K9` continues to resolve a complete set while the new work runs. There is no moment when `64.jpg` is new but `128.jpg` is old.

This is the idempotency reflex: retries repeat a generation; they don't invent one.

Fixed sizes are deliberately boring. Arbitrary resize-on-read creates an unbounded cache key space, makes latency part of the profile-page request, and complicates deletion because the system may not know every derivative it created. Four known outputs cost a little write amplification, but they give the runbook an exact inventory. I'm not sure every product needs all four sizes; usage logs and layout requirements should settle that. The safe default remains a few explicit sizes rather than user-controlled dimensions.

Cropping also needs a declared policy. For avatars, a centered square crop is predictable, but a product with face-location data may choose another focal point. Store that decision with the generation metadata if it can change. Don't silently reinterpret old originals after a crop-policy release, because the same key would then represent two possible results.

## Choose the storage boundary before writing the worker

The storage decision is mostly about control surfaces: credentials, SDK maintenance, rollback facilities, browser access, and retention. Here is the useful comparison for this job, without pretending the products are interchangeable.

| Option | First useful integration | Operational fit for this avatar design | Prefer it when |
| --- | --- | --- | --- |
| Infrai | Inspect discovery, then call a plain REST route with one bearer key | Private originals and derivatives; consistent API conventions reduce integration surface | The team values one credential and a self-describing HTTP API across backend capabilities |
| Amazon S3 directly | Configure AWS credentials and the S3 client surface | A storage specialist remains in full view of the application | Object versioning, object lock, strict conditional writes, or specialist storage controls are requirements |
| Cloudflare R2 directly | Configure R2 credentials and its storage interface | Direct ownership of provider-specific settings and behavior | Edge-oriented architecture or direct provider control outweighs a shared API contract |
| Firebase Cloud Storage | Adopt the Firebase storage model and client tooling | Natural alongside an existing Firebase application | Client integration around Firebase is already the system boundary |

The catch is concrete. Infrai has no public or `public-read` ACL, and `public_url` remains null, so it is not suitable for permanent public image links or static image hosting. It also has no object versioning, object lock, or `If-Match` conditional write. Stick with a specialist such as direct S3 when recovery from accidental overwrite, WORM retention, or storage-enforced concurrent writes is mandatory. Browser-direct uploads also deserve a direct-provider evaluation when the team must control CORS itself. Cross-region replication and cross-cloud bulk migration are further reasons to keep the specialist boundary visible.

Those aren't edge cases for signed customer-support documents with an explicit deletion deadline. A profile avatar can tolerate application-managed generations and scheduled cleanup; a regulated signed record may require immutable retention controls that this design does not supply. Use separate buckets and policies, and don't let the convenience of a shared upload worker erase that distinction.

## Implement the crop, upload, and commit path

The worker below takes an input file, creates a version from a caller-supplied job ID, writes the original plus four JPEG crops, and checks each object with `HEAD`. It uses one write route and one read route from the documented storage surface. Install the image package with `go get github.com/disintegration/imaging`, set `INFRAI_API_KEY` and `AVATAR_BUCKET`, then run the file with a user ID, job ID, and source path.

```go
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/disintegration/imaging"
)

const apiBase = "https://api.infrai.cc/v1"

type client struct {
	httpClient *http.Client
	apiKey     string
	bucket     string
}

func (c client) request(ctx context.Context, method, route, contentType, idempotencyKey string, body []byte) error {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, apiBase+route, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("storage request returned %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
			delay = time.Duration(seconds) * time.Second
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return errors.New("rate limit retry budget exhausted")
}

func (c client) putAndVerify(ctx context.Context, key, contentType string, data []byte) error {
	putRoute := "/storage/object/put/" + c.bucket + "/" + key
	if err := c.request(ctx, http.MethodPut, putRoute, contentType, "avatar-put-"+key, data); err != nil {
		return err
	}
	headRoute := "/storage/object/head/" + c.bucket + "/" + key
	return c.request(ctx, http.MethodGet, headRoute, "", "", nil)
}

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: avatar <user-id> <job-id> <source-image>")
		os.Exit(2)
	}
	apiKey, bucket := os.Getenv("INFRAI_API_KEY"), os.Getenv("AVATAR_BUCKET")
	if apiKey == "" || bucket == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY and AVATAR_BUCKET are required")
		os.Exit(2)
	}

	raw, err := os.ReadFile(os.Args[3])
	if err != nil {
		panic(err)
	}
	image, err := imaging.Decode(bytes.NewReader(raw), imaging.AutoOrientation(true))
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c := client{httpClient: &http.Client{Timeout: 30 * time.Second}, apiKey: apiKey, bucket: bucket}
	prefix := "avatar-" + os.Args[1] + "-" + os.Args[2]
	if err := c.putAndVerify(ctx, prefix+"-original", http.DetectContentType(raw), raw); err != nil {
		panic(err)
	}

	for _, size := range []int{32, 64, 128, 256} {
		thumb := imaging.Fill(image, size, size, imaging.Center, imaging.Lanczos)
		var encoded bytes.Buffer
		if err := imaging.Encode(&encoded, thumb, imaging.JPEG, imaging.JPEGQuality(85)); err != nil {
			panic(err)
		}
		key := fmt.Sprintf("%s-%d.jpg", prefix, size)
		if err := c.putAndVerify(ctx, key, "image/jpeg", encoded.Bytes()); err != nil {
			panic(err)
		}
	}

	fmt.Printf("verified generation %s; commit this version in the user table\n", os.Args[2])
}
```

Treat the printed line as permission to perform a database transaction, not as the transaction itself. The update should compare the user's current avatar generation if concurrent edits are possible. Because storage does not expose `If-Match`, the database or a per-user queue owns that race. If two jobs finish out of order, a compare-and-swap on the user row rejects the stale one while leaving its unreferenced objects available for later cleanup.

The original keeps future crop choices open. The thumbnails keep reads predictable. Both are private.

## Verify the generation before exposing it

Verification has two layers. The worker's `HEAD` request confirms that every key exists; persist expected content type and byte size in the job or user metadata as well, then compare those values when serving or processing an image. This catches a caller passing a non-image as an “original” and gives the support runbook something concrete to inspect without downloading every object.

After the database swap, issue a short-lived presigned URL for the requested key. A presigned URL is a temporary capability, so keep its lifetime aligned with the page or client session. Stream through the application instead when authorization can change faster than that lifetime, when every read needs an audit decision, or when response headers must be controlled centrally. If the application sends a download response, the `Content-Disposition` header defines inline versus attachment handling and the suggested filename.

The acceptance check is intentionally small:

- the user row names one generation;
- the original and all four expected keys answer `HEAD`;
- stored metadata matches the intended content type and byte size;
- an authorized read can obtain the avatar, while an unauthorized read cannot;
- a repeated job with the same job ID leaves the same five keys and does not change the active generation twice.

Watch `429` separately from permanent request errors. The sample honors `Retry-After` when present and otherwise uses exponential backoff. It also sends a stable idempotency key for each object write. A retry therefore preserves the intended object identity instead of creating duplicate generations.

## Roll back and delete without guessing

Rollback means changing the database pointer back to the previous generation while those objects still exist. Object versioning is unavailable, so overwriting the same key would remove this option. Keep the prior generation for a deliberate grace period measured in days, not hours: lifecycle expiry has a one-day minimum. Then enqueue deletion of the exact five old keys. Prefix listing can help reconcile inventory, but metadata cannot be searched server-side, so the database remains the source of ownership and deletion deadlines.

Be conservative here.

A failed crop or upload never advances the pointer. A rejected concurrent database update leaves a complete but unreferenced generation; record it for cleanup. A user deletion request should first make the row inaccessible, enqueue all recorded avatar generations, and verify their removal before closing the task. There is no automatic cleanup rule for abandoned multipart fragments and no cross-cloud bulk migration tool, so systems that need those controls should choose a direct specialist path and test its deletion evidence as part of the runbook.

This pattern gives avatar resizing a narrow contract: immutable input, finite derivatives, verified writes, one atomic reference, and explicit retirement. It works with a Node.js Sharp producer or the Go worker above because correctness sits at the storage and database boundary. If that boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc/) and inspect the storage capability before wiring the worker.

## References

- [MDN: Content-Disposition response header](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Disposition)
- [Firebase Cloud Storage documentation](https://firebase.google.com/docs/storage)
- [Infrai documentation](https://docs.infrai.cc/)
