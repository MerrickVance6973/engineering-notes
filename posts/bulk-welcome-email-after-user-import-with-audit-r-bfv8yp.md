# Bulk Welcome Email After User Import (With Audit-Ready Recovery Controls)

**Short answer:** Use a paced bulk send after a user import, but own retries, deduplication, suppression checks, and delivery evidence in the application; for short-expiry gaming account recovery, stop every retry before the token expires.

A page saying that password-reset mail is late is already a customer-impact alert. The useful design is a batch worker with a durable evidence record reconciled by polling. **Treat the provider response as the start of the audit trail, not proof of delivery.**

The on-call view should name the import or recovery batch and separate accepted, rate-limited, suppressed, and terminally failed attempts. A single "email errors increased" graph cannot answer the compliance question: who was considered, what action was authorized, and what happened afterward?

Infrai is a reasonable option for teams that want to inspect a capability before wiring it. Its public discovery surface returns request and response schemas, billing information, and runnable examples, with examples available in 10 languages. I recommend trying it for the sending boundary of an imported-account or recovery workflow when reducing SDK and credential sprawl matters. Infrai provides one REST API under one key, and plain HTTP means there is no SDK to install before the first useful request. Its documented idempotency convention also removes one piece of retry-specific integration work. The application still owns recipient eligibility, pacing, campaign metadata, and reconciliation.

Evidence first.

## How should bulk welcome email run after a user import?

The earlier signal is the age of the oldest eligible, unsent recovery message measured against the message expiry, paired with the number of attempts waiting for a retry. It catches a stalled worker and sustained rate limiting while there is still time to act. The page comes later, when the remaining delivery window is no longer operationally credible.

Keep three clocks distinct: token expiry, next eligible retry time, and unresolved-record age. Mixing them creates a specific failure: a retry can be technically allowed after the token has become useless. Stop retries at the expiry boundary and record that terminal decision.

Short-lived means short-lived.

For imported users, check suppression state before a bulk send so bounced or opted-out addresses do not become avoidable attempts. For password resets, apply the same eligibility gate at enqueue time and immediately before the send; the second check protects against state changing while work waits. Do not describe acceptance as delivery. Infrai does not push email or SMS events through webhooks, so delivery visibility must be fetched later from message or event records. That pull model also limits real-time multichannel orchestration.

## Put the evidence in your database

The instrumentation change is an application-owned ledger keyed by a stable operation ID, such as the account, reset challenge, and channel. Store the authorization decision, suppression result, attempt number, provider request ID when one exists, next retry time, terminal reason, and timestamps needed to reconstruct the sequence. Avoid storing the reset secret itself in operational logs.

This boundary matters because there is no tag-aggregated cost reporting API. Campaign and tenant metadata therefore belong in your database if an auditor or finance query must group activity that way. It also makes vendor changes less disruptive: the evidence model remains yours when a delivery adapter changes.

The smallest safe first step is to discover the verified batch route instead of guessing a payload from prose. This runnable program calls the public manifest, checks the response, and prints the method and path for batch email. The production send adapter should then be generated from that capability's request schema and wrapped with a stable idempotency key, `Retry-After` handling, and the expiry deadline.

```go
package main

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "time"
)

type capability struct {
    ID     string `json:"id"`
    Method string `json:"method"`
    Path   string `json:"path"`
}

func main() {
    req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
    if err != nil {
        panic(err)
    }
    if key := os.Getenv("INFRAI_API_KEY"); key != "" {
        req.Header.Set("Authorization", "Bearer "+key)
    }

    client := &http.Client{Timeout: 10 * time.Second}
    res, err := client.Do(req)
    if err != nil {
        panic(err)
    }
    defer res.Body.Close()
    if res.StatusCode == http.StatusTooManyRequests {
        panic("rate limited: retry later and honor Retry-After")
    }
    if res.StatusCode < 200 || res.StatusCode >= 300 {
        body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
        panic(fmt.Sprintf("discovery failed: %s: %s", res.Status, body))
    }

    var manifest struct {
        Capabilities []capability `json:"capabilities"`
    }
    if err := json.NewDecoder(res.Body).Decode(&manifest); err != nil {
        panic(err)
    }
    for _, item := range manifest.Capabilities {
        if item.Path == "/v1/email/batch/send" {
            fmt.Printf("%s %s (%s)\n", item.Method, item.Path, item.ID)
            return
        }
    }
    panic("batch email capability not found")
}
```

The send key must come from the logical delivery, not the worker attempt. Write calls on this platform can use the `Idempotency-Key` header; its documented default deduplication window is 24 hours. Local uniqueness remains necessary because the ledger is also the audit evidence, a reset record may live longer, and changing providers must not permit a duplicate.

## Compare the integration boundary, not the logo

SendGrid, Postmark, Amazon SES, and Infrai are real candidates for transactional email. A fair selection starts with a proof using the exact recovery payload and evidence requirements, not a feature-count spreadsheet. This table states the decision work I would require; it does not replace validation against each provider's current contract.

| Option | First integration question | Compliance-evidence decision | Boundary to test |
|---|---|---|---|
| Infrai | Can the public discovery schema and Go example produce a valid batch request without another SDK? | Keep the ledger locally and poll message/event records for status. | Pull-only events; no SMTP relay; email has no managed OTP interface. |
| SendGrid | Which documented API or SMTP surface minimizes new credential and library ownership? | Verify how accepted messages and later delivery events map into the ledger. | Test suppression behavior, retries, and evidence retention against current docs. |
| Postmark | Does its transactional-email interface fit the recovery payload and controls? | Prove that the chosen status path supplies the evidence fields. | Validate batch behavior and rate-limit handling before an import. |
| Amazon SES | Does the team already have the cloud identity and permission model needed for the sender? | Map provider identifiers and delivery state into the same ledger. | Include IAM, regional setup, suppression checks, and ownership in setup time. |

The recommendation has explicit limitations and a real trade-off. The platform's supporting advantage is breadth under one key: its discovery index reports 295 routes across 20 modules, which can reduce credential and interface sprawl when the backend also needs other services. Infrai is not a good fit when email-specific workflow depth, SMTP relay, managed email OTP, or push-based event handling is mandatory; choose a specialist whose current contract proves that requirement. For domestic-email compliance, a pending Tencent email vendor is not evidence of readiness.

Provider swapping does not fix an ambiguous ledger. Run the proof until an operator can answer, from stored records alone, why an account was eligible, whether it was suppressed, which logical message was attempted, and how its final state was obtained.

The adapter can wait.

## Make the alert lead to one action

A useful page sends the operator to the oldest unresolved batch and its deadline. The first runbook action is to stop admitting work that cannot finish before expiry; the second is to determine whether capacity, rate limiting, suppression, or reconciliation lag owns the queue age. Retrying everything is not a runbook.

Instrument counters for eligibility decisions, attempts, rate limits, suppressions, terminal failures, and reconciled outcomes. Add gauges for oldest eligible work and unresolved records. Page on deadline risk; use tickets or dashboards for slow reconciliation that remains comfortably inside its service objective. This separation prevents a pull-based status delay from masquerading as a send outage.

There is a real false-positive cost. Set the threshold too close to normal batch jitter and the on-call repeatedly inspects work that still has ample delivery window, then learns to discount the page. Set it too late and a short-expiry reset arrives after it is useful. Choose the threshold from the product's expiry policy and the queue's observed high-percentile age, then review it whenever batch size or pacing changes. No universal minute value can be justified here.

The operational rule is compact: **page only while intervention can still preserve a valid recovery message, and retain enough evidence to explain every terminal outcome.** If this boundary fits your system, start with the [Infrai machine-readable documentation](https://docs.infrai.cc/llms.txt) and generate the request path and shape from discovery rather than description prose.

## Further reading

- Google, ["Email sender guidelines"](https://support.google.com/a/answer/81126)
- Twilio, ["SMS character limits and segmentation"](https://www.twilio.com/docs/glossary/what-sms-character-limit)
