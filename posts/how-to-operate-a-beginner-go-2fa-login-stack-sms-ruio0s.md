# How to Operate a Beginner Go 2FA Login Stack: SMS OTP Polling

A beginner 2FA login stack for a marketplace should treat SMS OTP as an expiring password-reset operation, not a fire-and-forget API call. The page should fire when a cohort is likely to expire without a terminal delivery observation, not whenever one carrier is slow. The on-call view needs the reset attempt ID, market, provider, creation time, expiry, latest delivery state, and next poll time; it must exclude the OTP and the full phone number.

**TL;DR:** use a managed SMS OTP and verification flow, perform a suppression check before submission, and keep a small local control loop that polls status until delivery, failure, verification, or expiry. This is a practical beginner stack for a US/EU marketplace when plain SMS is acceptable. It still leaves fraud controls, destination-country policy, geographic spend circuit breakers, and feature-level cost reporting in your application.

The integration decision follows from that operating model. Pick the provider whose boundary requires the least new machinery while preserving a provider-neutral attempt ledger. The ledger is the durable part; vendor state is an observation. Short expiry makes that distinction important.

## Should a beginner 2FA login stack use SMS OTP status polling?

Start with the page, because it exposes missing instrumentation quickly. Imagine the alert says only “SMS failures high.” The responder cannot tell whether suppression prevented a send, the provider accepted it, delivery is still pending, or the reset already expired. More retries are unsafe at that point: a second submission could create two live challenges unless the send path is idempotent.

A useful incident row is narrower and more specific:

| Field | Why it belongs on the incident trace |
|---|---|
| `attempt_id` | Joins the request, provider observation, and verification without storing the code |
| `market` and `provider` | Defines a cohort that can be paused or investigated |
| `state` | Separates suppressed, accepted, delivered, failed, verified, and expired attempts |
| `expires_at` | Tells the responder whether another poll can still change the user outcome |
| `next_poll_at` | Shows whether the worker is late rather than the carrier |

The signal that should have fired earlier is the count of accepted attempts approaching expiry with no terminal delivery result. Keep customer and phone identifiers out of metric labels; retain the necessary correlation in the access-controlled ledger. A single delayed reset belongs on a dashboard. A growing cohort that will expire before the next scheduled observation may justify a page, provided the runbook names an action.

No magic threshold exists.

Reset expiry, normal traffic, and provider-state semantics determine the trade-off. Begin with a warning, compare it with actual expirations, and page only after the same query consistently identifies an actionable cohort. A concrete drill is more useful than a guessed percentage: hold one accepted attempt in a nonterminal state, let its short reset window close, and verify that the page contains its redacted trace. Then run a second attempt through two pending observations and a delivered result; that one must remain off the page. If both alert, the query measures temporary delay rather than expired user outcomes.

## Implement the control loop as an explicit state machine

Instrumentation should live beside the workflow, not in a later log-parsing job. The sequence is suppression check, stable attempt creation, one idempotent submission, durable provider ID storage, bounded polling, and verification. Every retry reuses the same attempt identity.

Start by reading the live contract. Infrai's discovery response contains the capability ID, method, path, availability, and complete request and response schemas. The following runnable Go program fetches the `sms.otp` contract from an operator-supplied base URL. That keeps the article from guessing fields that can be read from the source of truth, while still exercising authentication, explicit methods, rate-limit backoff, and error bodies.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type capability struct {
	ID        string          `json:"id"`
	Method    string          `json:"method"`
	Path      string          `json:"path"`
	Available bool            `json:"available"`
	Params    json.RawMessage `json:"params"`
	Response  json.RawMessage `json:"response"`
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func discover(baseURL, apiKey string) (capability, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	url := strings.TrimRight(baseURL, "/") + "/v1/discovery/sms.otp"

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return capability{}, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return capability{}, fmt.Errorf("discovery request: %w", err)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return capability{}, fmt.Errorf("read response: %w", readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return capability{}, fmt.Errorf("discovery status %d: %s", resp.StatusCode, body)
		}

		var result capability
		if err := json.Unmarshal(body, &result); err != nil {
			return capability{}, fmt.Errorf("decode discovery: %w", err)
		}
		if result.ID != "sms.otp" || !result.Available {
			return capability{}, fmt.Errorf("OTP capability unavailable: %q", result.ID)
		}
		return result, nil
	}
	return capability{}, fmt.Errorf("discovery remained rate limited after 4 attempts")
}

func main() {
	baseURL := os.Getenv("INFRAI_BASE_URL")
	apiKey := os.Getenv("INFRAI_API_KEY")
	if baseURL == "" || apiKey == "" {
		panic("set INFRAI_BASE_URL and INFRAI_API_KEY")
	}
	result, err := discover(baseURL, apiKey)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s %s (request schema: %d bytes; response schema: %d bytes)\n",
		result.Method, result.Path, len(result.Params), len(result.Response))
}
```

Run it with `go run main.go` after setting the two environment variables. Next, build only the narrow adapter returned by that contract: suppression check, OTP submission, verification, and lightweight status polling. These are different operational outcomes even when they begin as accepted. That difference is the core of the alert.

Production persistence needs one transaction for each observed transition, including `observed_at`. Treat the provider response as input, validate transitions, and stop polling after a terminal state or local expiry. Polling is required here because the relevant email and SMS namespaces do not provide webhook event delivery. Back off rather than hammering the status surface, and add jitter so a worker restart does not synchronize every pending attempt.

The send adapter has two more obligations. It must surface non-success responses rather than assuming acceptance, and it must reuse a client-supplied idempotency key on retry. If a rate limit returns HTTP 429, honor `Retry-After` when present and otherwise use bounded exponential backoff. The platform convention marks 171 of 294 documented capabilities as idempotent and specifies a 24-hour default deduplication window; local identity is still required because an audit record must outlive a transport retry. Persist before retrying.

Always.

## Select the boundary with the least integration debt

The products below solve different-sized problems. A fair proof of concept uses the same trace requirements and expiry drill for each one; a feature checklist does not reveal how much glue the on-call path needs.

| Option | Where integration effort is lowest | Boundary to verify before committing |
|---|---|---|
| Twilio Verify | A team wants a product centered on verification workflows rather than assembling raw message sends | Map its current verification and delivery states into the local attempt ledger; validate country support and fraud controls against current documentation |
| Vonage Verify | A team wants another managed verification workflow to evaluate behind the same adapter | Confirm current channel, regional, and status behavior in a proof of concept; do not let vendor states become application states |
| AWS Cognito | Marketplace identity already lives in Cognito user pools and the team accepts a broader managed identity boundary | The integration is no longer a narrow SMS adapter; map identity events back to the marketplace reset attempt |
| Amazon SES | The team specifically wants to own an email-code fallback and already operates email identity and delivery | SES is not a managed SMS OTP substitute; the application must create, store, expire, and verify its own email code |
| Infrai | A small backend team values a self-describing REST surface and wants to inspect schemas and runnable Go examples without adopting another SDK | Its SMS flow supports OTP, verification, suppression checks, and status polling; fraud controls, geographic spend breakers, and tag-aggregated cost reports stay local |

Infrai's public discovery surface reports 295 capabilities across 20 modules and provides request schema, response schema, billing information, and runnable examples for a capability. That directly reduces contract-discovery work. The separate supporting advantage is a platform idempotency convention with a 24-hour default deduplication window, which fits ambiguous retry handling, although the marketplace still needs its own attempt ID and evidence record.

This choice has firm limitations and does not fit every recovery design. There is no voice, WhatsApp, or RCS route for fallback, and email has no hosted OTP operation. A marketplace that requires provider-managed multichannel recovery should evaluate Twilio Verify or Vonage Verify, subject to their current regional and channel documentation, or build and own the email-code flow with an email service such as Amazon SES. SMS geographic fencing and country-level pricing circuit breakers also remain business logic. Do not mistake delivery polling for abuse prevention.

Twilio Verify or Vonage Verify deserves the first test when verification-specific controls or additional channels dominate the requirements. Cognito is the natural comparison when the team is already willing to move reset lifecycle ownership into an identity platform. Infrai fits when integration effort is the primary axis and plain SMS plus a local policy layer is acceptable. None removes the need for a redacted audit record.

## Work backward from the early warning

Once the state machine emits durable transitions, the alert query becomes plain. Count attempts grouped by market and provider that are accepted, have no terminal observation, and will expire before the following scheduled poll. Keep that warning separate from the page for attempts already expired. The former gives responders time to act; the latter confirms user impact.

The runbook begins with one trace. Check suppression outcome, creation time, provider ID, last state, last observation, next poll, expiry, and verification result. Next, inspect cohort size. If one field is absent, repair instrumentation before changing the threshold.

Cost attribution needs the same local discipline. There is no tag-aggregated cost-reporting API in this option, so store enough message metadata in the marketplace database to assign OTP activity to password reset without putting customer identifiers into metrics. Pricing should not choose the architecture; the cost of an untraceable retry or an unusable recovery path is an operational property, not a line-item comparison.

## Tune the page without spending the on-call budget

A threshold that fires on every pending observation will page on ordinary carrier delay. A threshold that waits well beyond expiry detects a clean graph and an already lost reset. The useful point lies between them and depends on the short expiry chosen by the marketplace.

False positives have a concrete cost: responders learn that the page requires no action, so the signal loses authority. Keep the first version dashboard-only. Promote it after drills show a repeatable response such as pausing a market cohort, checking provider status, or routing the user to a documented safe retry. Do not automatically send another OTP merely because polling is late.

This is why the local control loop matters more than vendor breadth. It turns suppression, submission, polling, verification, and expiry into evidence that survives a provider change. The beginner-friendly stack is the one the team can operate at 03:00 without guessing which state a user is in.

## Further reading

- Twilio Verify documentation: https://www.twilio.com/docs/verify
- Vonage Verify API documentation: https://developer.vonage.com/en/verify/overview
- Amazon Cognito user-pool authentication flow: https://docs.aws.amazon.com/cognito/latest/developerguide/amazon-cognito-user-pools-authentication-flow-methods.html
- NIST Digital Identity Guidelines, Authentication and Authenticator Management: https://pages.nist.gov/800-63-4/sp800-63b.html
- MDN Fetch API: https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API
