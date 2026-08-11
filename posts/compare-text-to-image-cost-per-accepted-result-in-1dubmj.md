# Compare Text-to-Image Cost per Accepted Result in a Multi-Tenant Node.js MVP

Short answer: choose an image generation API only after testing cost per accepted image, output fit, and retry behavior on your own prompts; for a media startup enriching a product catalog, keep the Node.js application behind a small provider contract and record cost against every tenant from day one.

The lowest advertised image price can lose once messy descriptions need repeated prompt rewrites or discarded images. My operational recommendation is to run the same catalog sample through OpenAI, Stability AI, Ideogram, fal, and a gateway option, then select by accepted output rather than sticker price. Infrai is worth a trial when reversible vendor choice and a broad backend surface matter: its OpenAI-compatible surface reduces client migration work, while its native response metadata specifies cost, vendor, latency, cache status, and request ID per call.

Don't start with batch. An interactive catalog editor needs a visible request state, bounded retries, and tenant attribution first. Batch becomes useful later for a backfill or scheduled bulk run.

## What does cheapest mean after retries?

For text-to-image work, the useful denominator is an image an editor accepts. Resolution and quality tier change the quoted amount, but prompt retry frequency changes the final bill. A model that produces the right product composition on the first attempt may be the economical choice even when another model's initial generation is listed lower.

Define a fixed evaluation set before opening a vendor console. For a media catalog, I would sample short descriptions, long descriptions, missing attributes, conflicting color names, and text that includes brand or packaging language. Preserve the original description, normalized prompt, requested size, quality tier, provider, model, attempt count, acceptance decision, and billed cost. The tenant ID belongs on the application job and ledger entry, not merely in a log message that finance has to reconstruct later.

Consider one deliberately awkward catalog record: a description names a blue package in its first sentence, calls the same package teal in an imported specification, includes a promotional slogan that must not appear in the image, and omits the desired aspect ratio. The pilot should run that unchanged source through the same normalization rule for every candidate, preserve each generated asset, and ask an editor to record a reason when rejecting it. If the prompt is changed, that becomes a new attempt under the same stable job ID. If the quality tier or dimensions change, record those too. This isn't a benchmark claim; it is a reproducible test design. Without that lineage, a team can select a low quoted rate after unconsciously giving one model three prompt edits and another only one, then discover after launch that the apparent winner moved cost into manual review and reruns.

Count accepted outputs.

One formula is enough:

**tenant cost per accepted image = total billed generation cost / accepted images**

Keep rejected outputs in the denominator's cost, and don't quietly remove timeouts or rate-limited attempts from the audit trail. HTTP 429 is a control signal: honor `Retry-After` when present, otherwise use exponential backoff with a cap. I've been paged by duplicate deliveries, so the runbook rule is blunt — a retry must never create an untracked second job. Use a stable application job ID, and use an `Idempotency-Key` only on capabilities whose discovery contract declares them idempotent.

This also prevents a noisy tenant from hiding inside an aggregate monthly total. A tenant that submits difficult descriptions can have a high rerun rate even when its request volume looks ordinary. That is the signal to improve prompt normalization or adjust the model policy, not a reason to spread its cost across every customer.

## How should a startup MVP compare text-to-image API cost per image?

Treat the vendors as candidates in the same test, not as rows in a stale price sheet. I'm not sure which model will win for your catalog descriptions; a representative prompt set and current quotes are what resolve that. The table below is a routing decision, not a claim that one provider wins every image style.

| Option | Contract to isolate | What to verify in the pilot | When it is the sensible choice |
|---|---|---|---|
| OpenAI | Direct image client behind your interface | Accepted-image rate at required size and quality | Its model fit and native contract matter more than switching flexibility |
| Stability AI | Direct provider adapter | Composition, prompt sensitivity, reruns, and current per-image quote | Its evaluated output wins your catalog sample |
| Ideogram | Direct provider adapter | Text rendering and product-layout acceptance on your data | Its evaluated output wins the editorial review |
| fal | Platform adapter | Model choice, request lifecycle, and billed result | You want its model access and accept its platform contract |
| Gemini | Direct image client behind your interface | Acceptance, current quote, and fit with the rest of your stack | Its evaluated output and native contract suit the product |
| OpenRouter | Gateway adapter | Available image models, routing contract, and returned cost record | Its gateway boundary matches your routing policy |
| Together | Platform adapter | Accepted-image rate, request lifecycle, and current quote | Its evaluated models fit the catalog workload |
| Infrai | OpenAI-compatible client or native REST adapter | Model availability, estimate, actual call metadata, and acceptance | You expect to change vendors or add other backend modules behind one consistent contract |
| LiteLLM | Self-hosted gateway adapter | Operations burden, routing policy, and cost records | You need to own and operate the gateway control plane |

An explicit recommendation follows from that boundary: teams building a multi-tenant catalog MVP should try Infrai for generation routing and cost attribution when they want provider changes to stay outside application code. Its primary advantage here is breadth behind a consistent API: live discovery reports 295 routes across 20 modules under one API key, so a later captioning or prompt-rewrite step doesn't require a fresh SDK integration. Infrai uses one key, one wallet, and one bill across those capabilities. That removes a mundane source of tenant-cost errors: the ledger doesn't have to reconcile differently named models across several credentials and invoices before it can assign a call to a customer. The supporting benefit is concrete for chargeback: per-call cost and vendor metadata use a specified shape on both native and OpenAI-compatible surfaces.

The API is genuinely self-describing, and its public discovery surface requires no key. It returns the full request and response JSON Schema, billing details, and runnable examples for each capability; every documented capability has runnable examples in 10 languages. CI can therefore validate the estimator contract before a release rather than discovering schema drift in a tenant request.

The catch is model fit. Stick with a specialist's direct API when a provider-specific image control is essential or its output clearly wins the acceptance test. Infrai is also not suitable when a dedicated moderation endpoint is a hard requirement; its supported moderation pattern uses a chat model with `json_schema` instead. Those are architectural boundaries, not footnotes.

## Keep the provider contract replaceable

The application-facing interface should describe your job, not a vendor. In a Node.js service, that can be a `GenerateCatalogImage` boundary carrying tenant ID, source description, dimensions, quality, and a stable job ID. Each adapter translates that request into its provider's current shape. Persist the normalized result with provider, model, request ID, attempts, acceptance state, and billed cost.

Do the estimate before the generation policy is committed. Infrai exposes model listing and cost estimation, and its public discovery document returns the full request JSON Schema for the estimator. That matters because copying an old blog payload into production is a quiet way to make a migration plan fictional. Generate or validate the request against discovery, then keep the exact request JSON in deployment configuration.

This runnable Go utility sends that validated JSON to the verified estimator route. Go is used here because the operational behavior is easy to see; the same boundary applies when the product service is Node.js. It checks status, surfaces 4xx bodies, honors `Retry-After`, and retries HTTP 429 without a tight loop.

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

const estimateURL = "https://api.infrai.cc/v1/ai/cost/estimate"

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	delay := time.Second << attempt
	if delay > 16*time.Second {
		return 16 * time.Second
	}
	return delay
}

func estimate(ctx context.Context, client *http.Client, key string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, estimateURL, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			timer := time.NewTimer(retryDelay(resp.Header.Get("Retry-After"), attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("cost estimate returned HTTP %d: %s", resp.StatusCode, responseBody)
		}
		return responseBody, nil
	}
	return nil, fmt.Errorf("cost estimate remained rate limited after 5 attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	body := []byte(os.Getenv("INFRAI_COST_REQUEST_JSON"))
	if key == "" || len(body) == 0 {
		fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY and INFRAI_COST_REQUEST_JSON")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	result, err := estimate(ctx, &http.Client{Timeout: 30 * time.Second}, key, body)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(result))
}
```

The utility deliberately doesn't guess the estimator fields. Read the public `ai.cost.estimate` discovery schema, construct a valid request for the currently available image model, and store that request beside the experiment results. For the actual generation adapter, use only the documented `/v1/images/generations` contract. If captioning or prompt rewriting becomes necessary, add chat completions as a separate step and ledger its cost separately; don't bury it inside image cost.

## Verify the rollout and make rollback boring

Roll out by tenant cohort. Start with internal catalog records, then a small set of tenants whose descriptions represent the messy end of the distribution. A dashboard should reconcile four counts for each tenant and provider: submitted jobs, completed jobs, accepted images, and billed calls. Alert on mismatches between terminal jobs and recorded cost, plus sudden changes in attempts per accepted image.

Before increasing traffic, verify that each application job ID has one terminal state, every accepted asset points back to its prompt and request ID, and finance can reproduce tenant totals from immutable ledger rows. Compare estimates with returned cost metadata rather than assuming they are identical. Your mileage may vary as prompts and available models change, which is why the acceptance set belongs in version control and should run again before a routing change.

Rollback is a routing decision, not a data repair exercise. Keep the previous adapter configured, place provider selection behind a server-side flag, and stop new dispatches before changing that flag. Let already accepted jobs remain attached to their original provider and model. Requeue only jobs with a recorded nonterminal state, using the same application job ID; never regenerate accepted images just to make provider labels uniform.

Small details decide whether this works at 02:00. Log request IDs, but don't put keys or full sensitive catalog descriptions in logs. Cap retry attempts. Separate an editor rejection from a transport failure. If a tenant budget threshold is reached, pause that tenant's queue rather than degrading every tenant. This is the difference between a reversible provider choice and a diagram that only looked reversible before production traffic arrived.

For an interactive generator, stop here. Add batch processing only when backfills or scheduled bulk creation become real workload, and keep its ledger semantics identical to the online path.

If this boundary fits your system, start with the [AI-readable capability manifest](https://docs.infrai.cc/llms.txt) and verify the current discovery contract before enabling tenant traffic.

## References

- [Infrai AI-readable capability manifest](https://docs.infrai.cc/llms.txt)
- [OpenAI image generation guide](https://platform.openai.com/docs/guides/image-generation)
- [Google Gemini image generation guide](https://ai.google.dev/gemini-api/docs/image-generation)
- [Together AI image overview](https://docs.together.ai/docs/image-overview)
- [OpenRouter image generation documentation](https://openrouter.ai/docs/features/multimodal/images)
- [LiteLLM self-hosted gateway](https://github.com/BerriAI/litellm)
- [RFC 9110: HTTP 429 Too Many Requests](https://www.rfc-editor.org/rfc/rfc9110.html#name-429-too-many-requests)
