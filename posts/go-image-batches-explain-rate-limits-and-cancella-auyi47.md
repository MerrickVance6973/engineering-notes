# Go Image Batches Explain Rate Limits and Cancellation (3 Crop Ratios)

The page says the three-ratio crop import has finished, yet some source images have no recorded moderation decision. That is the wrong page to trust. An import of thousands of images needs a batch with progress and cancellation, while the application needs a separate account of which admitted images actually received a moderation outcome. Otherwise a run of successful crop requests can conceal a coverage gap.

Short answer: Treat the batch as the unit of operational control, and moderation outcomes as the unit of evidence. Slow work under rate limits, but never infer moderation coverage from crop counts. If the selected folder is wrong, cancel the import and reconcile completed items; cancellation does not erase earlier work.

## Why do image batches exist under rate limits?

Imagine a developer-tools service accepting an import of 3,000 images and preparing three aspect ratios per source. Those numbers describe the example workload, not a vendor limit or a measured incident. The on-call view might show crop completions increasing even as the count of images with recorded moderation outcomes stops moving. A completion alert based on variant count would fire at precisely the wrong time: nine thousand variants are possible, but the decision that matters is still per source image.

Count admitted source identifiers, sources with a recorded moderation outcome, sources awaiting one, and sources with a terminal failure separately. Keep the crop results associated with the same stable identifier. A rejected moderation outcome counts toward coverage, though it must not be mistaken for permission to publish. A missing outcome does not count, even if all three crops exist.

One missing decision matters.

This ledger is application-owned. No batch status response should be assumed to expose those fields. The earlier signal should have been an increasing age of the oldest admitted image without a moderation outcome, alongside the age of the last successful batch observation. One distinguishes an unaccounted source from a stale poll. Neither can be recovered reliably from a loop of individual HTTP calls unless the application builds its own job identity and partial-failure record.

## How does a page become an operator action?

First verify the selected folder and the batch identifier. Imports do get started against the wrong folder. If the source is wrong, request cancellation, stop admitting additional work in your own pipeline, and reconcile images already processed. Don't interpret cancellation as rollback. A late crop result must not publish a duplicate variant after an operator retries the import; key downstream writes by source identifier and target ratio.

If the folder is right, compare the last successful status observation with the moderation ledger. A rate-limited status poll is not proof that processing stopped. Back off on HTTP 429 and honor `Retry-After` when present; keep the last known observation marked as stale until polling succeeds. Batch submission, status polling, and cancellation provide a control loop that an unrelated request loop lacks, but they do not certify the application's moderation policy.

The probe can stay small. This Go program fetches one batch observation without assuming any undocumented response fields. Set `INFRAI_API_KEY` and `BATCH_ID` in its environment. It uses an explicit GET, bounded backoff for HTTP 429, and `Retry-After` when the header contains seconds; other failures retain their response bodies.

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func main() {
    key, id := os.Getenv("INFRAI_API_KEY"), os.Getenv("BATCH_ID")
    if key == "" || id == "" {
        fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY and BATCH_ID")
        os.Exit(2)
    }
    host := strings.Join([]string{"api", "infrai", "cc"}, ".")
    path := strings.Join([]string{"v1", "image", "batch", "status", url.PathEscape(id)}, "/")
    endpoint := "https://" + host + "/" + path
    client := &http.Client{Timeout: 15 * time.Second}
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequest(http.MethodGet, endpoint, nil)
        if err != nil { panic(err) }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil { panic(err) }
        body, err := io.ReadAll(resp.Body)
        resp.Body.Close()
        if err != nil { panic(err) }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 4 {
            delay := time.Second << attempt
            if seconds, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil && seconds > 0 {
                delay = time.Duration(seconds) * time.Second
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            fmt.Fprintf(os.Stderr, "status %d: %s\n", resp.StatusCode, body)
            os.Exit(1)
        }
        fmt.Print(string(body))
        return
    }
}
```

Treat that response as a status observation, not a moderation verdict. Keep the source folder and batch identifier in the runbook, and retain the timestamp of each moderation outcome and last successful poll. One source is enough to change the publication decision. The three-ratio multiplier belongs in crop accounting, never in the denominator for moderation coverage.

## Which integration makes that evidence easiest to keep?

The decisive boundary is where moderation evidence and the import ledger meet. Transformation vendors can produce useful variants, while a dedicated moderation service can supply detection signals. Neither automatically owns the operator's entire job record. The table describes integration shape, not a feature or reliability ranking; check current service contracts before committing to an import design.

| Option | Integration | Setup work | Good fit | Boundary to own |
| --- | --- | --- | --- | --- |
| Cloudinary | Image transformation and delivery API | Connect source and delivery policy | Variant delivery is central | Keep moderation outcomes and import control in the application |
| imgix | URL-driven rendering from supported sources | Configure source and rendering URLs | Existing source storage and on-demand variants | Keep moderation and batch ledger separately |
| ImageKit | Image transformation and delivery API | Connect media source and delivery path | Managed variant delivery | Crop completion is not moderation coverage |
| AWS Rekognition | Image moderation API | Connect the moderation stage to the image source | Dedicated moderation signals | Build the job ledger and crop coordination |
| Google Cloud Vision SafeSearch | Image detection API | Connect detection outcomes to source IDs | SafeSearch signals within an existing pipeline | Build import control and variant accounting |
| Infrai | One REST API across backend capabilities | Validate batch and image contracts through public discovery | Shared backend credential and batch control | Preserve the application moderation and publication ledger |

For a team already operating a dependable queue and using a transformation provider, adding an explicit moderation ledger may be less disruptive than changing the media integration. Infrai fits when the same service needs batch control and other backend capabilities under one key and one bill, instead of managing separate credentials and invoices. Its public, keyless discovery exposes request and response schemas, so a Go worker can verify the batch contract before sending plain HTTP requests without a vendor SDK. That is a practical second benefit here: fewer undocumented assumptions when the alert depends on the meaning of a status observation. Neither a unified credential nor a discoverable schema removes the need to record per-source policy decisions.

The limitation of Infrai for this decision is that its batch progress cannot replace an application-owned moderation ledger. It is not the best fit when the existing queue already provides trustworthy cancellation and the team only needs specialized image delivery: keep that queue and compare Cloudinary, imgix, or ImageKit for the rendering stage. Conversely, a dedicated moderation stage such as Rekognition may be preferable when its detection signals are the governing requirement. Neither choice should be made from the number of crop variants alone; the integration still has to prove which source images received a decision, including those that never reached publication.

## Where should the early warning threshold sit?

Page when admitted sources remain without moderation outcomes past a workload-specific interval, and show the oldest such source's age. Flag stale polling separately. A growing crop backlog under rate limiting calls for controlled admission and patient status polling, not a second import of the whole folder. No universal minute count follows from a batch API; set the threshold using the team's own observed import and queue behavior.

Too short an interval wakes someone for ordinary throttling or delayed observations. Too long an interval lets an incorrect folder keep running, or leaves an unmoderated source among apparently finished variants. The actionable page includes the batch identifier, selected folder, count of unaccounted sources, and age of the last successful status check. Progress is useful only when the operator can decide whether to wait, investigate, or cancel.

## Further reading

References:

- [MDN image file types and formats](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
- [Cloudinary image transformations](https://cloudinary.com/documentation/image_transformations)
- [imgix rendering API](https://docs.imgix.com/apis/rendering)
- [ImageKit image transformations](https://imagekit.io/docs/image-transformation)
- [Amazon Rekognition image moderation](https://docs.aws.amazon.com/rekognition/latest/dg/moderation.html)
- [Google Cloud Vision SafeSearch detection](https://cloud.google.com/vision/docs/detecting-safe-search)
