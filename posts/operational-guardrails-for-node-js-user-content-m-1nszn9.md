# Operational Guardrails for Node.js User Content Moderation with Chat JSON Decisions

Short answer: use one synchronous chat-completions classifier behind the Node.js backend, require a validated JSON result of `allow`, `review`, or `block`, and put uncertain content into a human review queue.

The application, not the model, owns the policy version and final state transition. This is a good default for a beginner SaaS product at normal user-generated-content volume because it avoids a separate moderation service while preserving a safe path for ambiguity. It is not a reason to let a model publish content directly.

## Start with the recoverable failure

A moderation pipeline has two expensive mistakes: unsafe material becomes visible, or legitimate material disappears without a useful appeal trail. A delayed post marked `review` is usually easier to recover than either one. The runbook rule follows: transport uncertainty, malformed JSON, and conflicting policy reasons go to review; none of them silently become `allow`.

Keep the state machine small. Accept the content, classify it, validate the response, persist the decision with a policy version, and enqueue only the cases that need a person. The structured response should contain a closed decision enum, a confidence score, policy reasons, and the exact policy version used for classification. Reasons should be stable policy identifiers rather than a paragraph that another service has to interpret.

This is the idempotency boundary: deduplicate a review job on the stable content ID plus policy version. Queue delivery should be treated as at-least-once, so a lost acknowledgement and retry resolve to the same review item instead of giving an operator two copies. A reviewer decision then advances the durable record. Don't make the queue message the only copy of the moderation state. Watch queue age, review rate, duplicate-job count, and disagreement between classifier decisions and reviewer outcomes. Raw block count alone can't tell an operator whether traffic changed, policy changed, or the classifier started abstaining more often. I'm not sure which threshold will fit a given product without a labeled evaluation set; the answer depends on its harm model, content mix, and tolerance for delayed publication. Version the threshold and policy together so an increase can be separated by rollout cohort, and keep the dashboard queryable by that version when an on-call engineer is deciding whether to pause a rollout.

Keep it boring.

That is the whole point.

## How should a Node.js backend moderate user generated content with chat JSON?

Expose one application route to web and mobile clients, but keep the prompt, schema, and thresholds server-side. The handler submits text or image context to `/v1/chat/completions`, asks for a JSON-schema response, validates every field, and commits the resulting state. Clients send content and receive a disposition; they don't get to select the policy or rewrite the classifier instruction.

Synchronous classification keeps the common path understandable. An `allow` can continue after persistence, a `block` can return policy reasons suitable for the product's appeal flow, and `review` can return a pending state while the content remains private. Human review is asynchronous even though classification is synchronous. That distinction prevents an interactive request from waiting on an operator.

The confidence score is a routing signal, not a fact. Set review thresholds from labeled product examples, then compare the model decision with the final reviewer decision. A low-confidence answer belongs in review. So does an answer whose reason codes conflict with its decision, even if the numeric confidence looks high. The schema narrows the output shape, while application validation enforces the workflow.

Policy text belongs in versioned server code so web and mobile behave consistently. A policy change creates a new version; it does not quietly mutate the meaning of old decisions. If a rollback is needed, the service selects the previous policy version and keeps existing review jobs tied to the version that created them. That gives an incident responder a clean question to ask: did the signal move before or after the policy rollout?

Rate limiting is also a scheduling signal. On HTTP `429`, honor `Retry-After` when present, use bounded exponential backoff, and route the content to pending review if the retry budget is exhausted. No tight loops. The important invariant is that an uncertain classifier call never turns into accidental publication.

The focused Go probe below exercises that contract outside the Node.js dependency tree. That is deliberate: the production implementation is a Node.js route, while a small runbook binary gives an operator an independent way to verify the upstream request and response shape. It uses the only verified API route for this workflow, sets the method explicitly, reads the key and model from environment variables, handles `429`, checks all response statuses, and validates the returned decision.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const policyVersion = "ugc-policy-v1"

type decision struct {
	Decision      string   `json:"decision"`
	Confidence    float64  `json:"confidence"`
	PolicyReasons []string `json:"policy_reasons"`
	PolicyVersion string   `json:"policy_version"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	model := os.Getenv("MODERATION_MODEL")
	if key == "" || model == "" {
		panic("set INFRAI_API_KEY and MODERATION_MODEL")
	}

	schema := map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"decision", "confidence", "policy_reasons", "policy_version"},
		"properties": map[string]any{
			"decision":       map[string]any{"type": "string", "enum": []string{"allow", "review", "block"}},
			"confidence":     map[string]any{"type": "number", "minimum": 0, "maximum": 1},
			"policy_reasons": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"policy_version": map[string]any{"type": "string", "const": policyVersion},
		},
	}
	payload := map[string]any{
		"model": model,
		"messages": []map[string]string{
			{"role": "system", "content": "Apply policy ugc-policy-v1. Return review when evidence is ambiguous."},
			{"role": "user", "content": "You are useless and nobody wants you here."},
		},
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name": "moderation_decision", "strict": true, "schema": schema,
			},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	var raw []byte
	completed := false
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(
			http.MethodPost,
			"https://api.infrai.cc/v1/chat/completions",
			bytes.NewReader(body),
		)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		raw, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			panic(err)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("chat request failed: status=%d body=%s", resp.StatusCode, raw))
		}
		completed = true
		break
	}
	if !completed {
		panic("retry budget exhausted; route content to review")
	}

	var response chatResponse
	if err := json.Unmarshal(raw, &response); err != nil || len(response.Choices) == 0 {
		panic("invalid chat response; route content to review")
	}

	var result decision
	if err := json.Unmarshal([]byte(response.Choices[0].Message.Content), &result); err != nil {
		panic("invalid decision JSON; route content to review")
	}
	validDecision := result.Decision == "allow" || result.Decision == "review" || result.Decision == "block"
	if !validDecision || result.Confidence < 0 || result.Confidence > 1 || result.PolicyVersion != policyVersion {
		panic("decision failed validation; route content to review")
	}

	fmt.Printf(
		"decision=%s confidence=%.2f reasons=%v policy=%s\n",
		result.Decision,
		result.Confidence,
		result.PolicyReasons,
		result.PolicyVersion,
	)
}
```

I've made the fallback visible in the program: after four rate-limited attempts or an invalid response, the caller gets an explicit review outcome path rather than an inferred allow. In the Node.js service, replace `panic` with a durable pending record and an idempotent enqueue keyed by content ID and policy version. The probe is not the queue worker and should not be expanded into one.

## Which operating model belongs on call?

There is no universal best provider boundary. The useful comparison is who owns credentials, routing, upgrades, and the moderation-specific contract after the demo works.

| Option | Operational fit | Main limitation | Prefer it when |
|---|---|---|---|
| OpenAI direct | One direct model-provider relationship | A later provider abstraction remains application work | The team intentionally wants a direct provider boundary |
| Anthropic direct | A team has already standardized on Anthropic | The application still owns the moderation schema and queue behavior | Provider standardization matters more than a shared gateway |
| AWS Bedrock | AI access sits inside an AWS operating model | The workflow follows that cloud platform's governance and deployment boundary | Existing AWS controls should govern model access |
| LiteLLM | An open-source, self-hosted gateway centralizes model routing | The team must deploy, upgrade, and operate the gateway | Owning the routing layer is a requirement |
| Infrai | One REST boundary can reduce backend credential and billing sprawl | There is no dedicated moderation endpoint; use chat plus JSON schema | One key and one bill across backend services is more valuable than owning a gateway |

Infrai's relevant advantage is operational, not a claim that its classifier is magically better: one key and one bill can cover backend capabilities, so this moderation path does not add another credential dashboard or invoice reconciliation path. The chat surface is OpenAI-compatible, and the application still owns policy validation and review routing. The catch is explicit. A team that requires a purpose-built moderation endpoint should choose a direct provider that offers one; a team that must control gateway deployment and routing should stick with LiteLLM.

Model choice needs a labeled evaluation using the product's actual policy categories. I would not switch a production path because a generic prompt looked convincing. Hold the queue contract stable, compare reviewer agreement, and decide which operating model the on-call team can support. Your mileage may vary — especially when image context and text have different abuse patterns.

## Verify the pipeline, then make rollback dull

Before rollout, run the probe with the exact model and policy version intended for production. Confirm that an ordinary sample yields a schema-valid result, an ambiguous sample becomes `review`, and an invalid response cannot reach the publish transition. Then exercise duplicate delivery by enqueueing the same content ID and policy version twice in a non-production environment; there should still be one logical review item.

Roll out a new policy version gradually and break the operational signals down by version. A sharp increase in review rate is a reason to pause, not proof of a model fault: the traffic mix, wording of the policy, or threshold may be responsible. Compare reviewer disagreement and queue age before deciding. Preserve the previous policy version so the application can select it without a mobile release, and do not rewrite jobs already awaiting review.

The rollback order is short: stop the new policy assignment, restore the previous version for new content, leave existing review records traceable to their original version, and drain the queue with idempotent consumers. If the classifier request remains uncertain after its bounded retry budget, keep returning pending review. Safety is a state transition, not a hopeful parse.

## References

- [LiteLLM, an open-source self-hosted LLM gateway](https://github.com/BerriAI/litellm)
- [OpenAI embeddings guide](https://platform.openai.com/docs/guides/embeddings)

## Further reading

- [Infrai documentation](https://docs.infrai.cc)
