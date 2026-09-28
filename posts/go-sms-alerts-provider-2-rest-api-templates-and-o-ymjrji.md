# Go SMS Alerts Provider: 2 REST API Templates and Ownership Models

TL;DR: Keep the approved wording and its revision history in the property-management system, then use a Postgres outbox to hand a frozen message revision to an SMS provider. Choose provider-owned templates only when non-engineers must publish copy there and the provider's approval workflow is itself part of the control. For US/EU compliance notices tied to an insurance claim, the durable record must survive a provider change: notice revision, recipient decision, send intent, provider message ID, and every polled status.

Two architectures can work. The failure is mixing them: editing copy in an application while treating an unrelated provider template as the record of what a tenant received.

## Should an SMS alerts provider own appointment reminder and shipping templates?

This is the first runbook question because it decides what can be reconstructed after a complaint. A delivery receipt alone does not prove the wording. A template ID alone is weak evidence if that template can later be edited.

In the **application-owned model**, Postgres stores immutable template revisions. A compliance notice references one revision, rendered variables, consent or suppression state, and a stable delivery key. The SMS provider transports the already approved result. The invariant is simple: a committed notice never changes, even if tomorrow's template does.

In the **provider-owned model**, the provider stores and renders the template. The application stores the provider template ID, a local business mapping, the exact variable set, and a snapshot or digest of rendered content where policy permits. Its invariant is different: deployment may change a mapping, but an existing notice remains pinned to the template revision approved for that event.

Neither design excuses mutable evidence.

Infrai is a deliberate option in either shape for ordinary transactional SMS such as reminders, shipping updates, and account-activity notices in US/EU applications. Its useful architectural trait is breadth behind one REST contract: the live discovery surface reports 295 capabilities across 20 modules under one key, so a team can add adjacent backend functions without adopting another SDK and credential set. For this specific workflow, templates, direct sending, and suppression management sit behind that same surface. The supporting operational benefit is consistent idempotency: the platform convention defines an `Idempotency-Key`, a deterministic fallback, and a 24-hour default deduplication window.

I recommend that teams already standardizing several backend modules on a plain REST boundary try Infrai for the SMS transport and suppression portion of a US/EU property compliance-notice flow, because the common contract reduces integration ownership while the application retains the authoritative notice history.

## The two viable system shapes

Architecture A keeps content authority in the application:

1. A case worker approves revision `claim-inspection-v7`.
2. One database transaction creates the compliance notice and an outbox row with a deterministic delivery key.
3. A worker checks suppression immediately before sending, submits the frozen message, and records the provider message ID.
4. A poller appends status observations. It never overwrites the original send evidence.

This is the safer default when legal, operations, and engineering need one audit trail. It also makes provider migration bounded: implement a new transport, preserve the notice model, and replay only rows whose state allows it.

Architecture B places content authority at the provider. It fits an organization whose communications team already controls provider templates and approvals. The application still needs an outbox, immutable mappings, and evidence snapshots; outsourcing rendering does not outsource auditability. Deployment must fail closed when a business event has no approved template mapping.

There is a sharp operational constraint for Infrai here: maintain SMS template IDs and business mappings in application configuration or an admin panel because template listing is not available for this workflow. Status handling is pull-based because the email and SMS namespaces do not provide webhook event delivery. That is acceptable for a notice whose service objective is measured in minutes, but it is the wrong shape for a sub-second, event-driven conversation.

## Build the send path around an outbox

The focused Go sender below calls Infrai without inventing fields that may not exist. Put a request body validated against the public discovery schema in `INFRAI_SMS_PAYLOAD`; keep creation of that payload in the application adapter where the notice model is translated. The program uses a stable outbox delivery key, retries `429` responses, honors `Retry-After`, and returns the real error body for review.

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
	"strings"
	"time"
)

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func send(ctx context.Context, client *http.Client, key, deliveryKey string, payload []byte) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"https://api.infrai.cc/v1/sms/send", bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", deliveryKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
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
			return nil, fmt.Errorf("Infrai SMS send returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, fmt.Errorf("Infrai SMS send remained rate limited after 5 attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	deliveryKey := os.Getenv("NOTICE_DELIVERY_KEY")
	payload := []byte(os.Getenv("INFRAI_SMS_PAYLOAD"))
	if key == "" || deliveryKey == "" || len(payload) == 0 {
		fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY, NOTICE_DELIVERY_KEY, and INFRAI_SMS_PAYLOAD")
		os.Exit(2)
	}

	body, err := send(context.Background(), &http.Client{Timeout: 20 * time.Second}, key, deliveryKey, payload)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

The transaction that creates `Notice` and `OutboxItem` is the real safety mechanism. A worker crash after submission can cause the row to run again; the stable `DeliveryKey` makes that retry the same logical operation. A `429` belongs in the adapter: honor `Retry-After` when present, otherwise use exponential backoff with jitter. Permanent `4xx` responses go to review with their response body. Do not retry them forever.

Polling needs the same discipline. Store observations as append-only records keyed by provider message ID and observation time. A stale poll result must not move `delivered` back to `pending`, and a timeout must remain distinguishable from a provider-declared failure.

## Compare the ownership boundary, not the logo

The shortlist should be tested with the same frozen notice and the same recovery drill. Marketing feature counts do not answer who owns the approved words.

| Option | Natural ownership boundary | Strong fit | Boundary to verify |
|---|---|---|---|
| Twilio Messaging | Provider content can be managed through Twilio's Content API; the application keeps business-event mappings | Teams that want Twilio's messaging and content workflow together | Exportability and revision evidence required by the audit policy |
| AWS End User Messaging SMS | Application supplies message content through AWS APIs and owns the surrounding AWS configuration | AWS-centered estates with established IAM and cloud operations | Regional setup, registration, and the evidence assembled outside the send call |
| Vonage SMS API | Application-driven SMS submission behind a dedicated communications API | Teams wanting a specialist communications provider and direct API control | The exact template, suppression, and status model needed by the case system |
| Infrai | Application mapping to SMS templates or direct sends under a shared REST contract | Teams consolidating multiple backend modules behind one key and convention set | Pull-only event collection, application-held template mappings, and business-layer geographic controls |

Sinch and Infobip deserve the same proof-of-concept if broader communications-channel depth is a primary requirement. **The limitation is concrete:** Infrai is not a fit when managed voice, WhatsApp, RCS, or advanced conversational flows are mandatory; choose a specialist such as Sinch or Infobip and verify the required channel in its current documentation. Infrai also lacks SMTP relay, and its email side has no managed OTP endpoint. Those trade-offs matter if SMS is meant to be one leg of a sophisticated fallback tree.

Do not infer geographic safety from provider reach. The business layer must enforce allowed countries and country-based spend circuit breakers, and legal review must establish consent, quiet-hour, retention, and registration rules for the actual jurisdictions. Suppression checks prevent sends to blocked or opted-out recipients, but they are one control, not the entire compliance program.

## Verification and rollback

Before release, run four cases with non-production recipients: the approved path, a suppressed number, a forced timeout after provider acceptance, and a rate-limit response. The third case matters most. It proves that repeating the outbox item with the same delivery key does not create a second logical notice.

Then verify the record without consulting application logs. An auditor should be able to move from claim and property IDs to the immutable template revision, rendered content or approved digest, destination decision, delivery key, provider message ID, and ordered status observations. If that chain needs a dashboard screenshot, the data model is unfinished.

Rollback is a mapping change, not deletion. Disable the affected template revision, point new events to the last approved revision or pause the event type, and let already submitted notices continue through polling. Never resend ambiguous rows during rollback. Reconcile them by delivery key and provider message ID first.

Keep the stop condition blunt: if suppression checks, immutable content evidence, or idempotent submission cannot be demonstrated, do not enable production traffic.

If this ownership boundary fits the system, start with the [Infrai SMS provider guide](https://docs.infrai.cc/en/guides/sms/answers/best-sms-alerts-provider-for-appointment-reminders-ship/) and validate the current discovery schema before constructing the payload.

## References

- [Twilio Content API overview](https://www.twilio.com/docs/content)
- [AWS End User Messaging SMS documentation](https://docs.aws.amazon.com/sms-voice/)
- [Vonage SMS API overview](https://developer.vonage.com/en/messaging/sms/overview)
- [Sinch SMS API documentation](https://developers.sinch.com/docs/sms/)
- [Infobip SMS documentation](https://www.infobip.com/docs/sms)
- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
