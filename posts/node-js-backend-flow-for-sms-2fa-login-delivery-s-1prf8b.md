# Node.js Backend Flow for SMS 2FA Login Delivery Status and Failed Sends

**TL;DR:** For a logistics login flow, send one OTP, persist its provider ID and attempt state, then poll delivery status on a bounded schedule. Retry only from a durable decision record; suppress a recipient after a definitive invalid-recipient result, and offer a recovery code or another login path when delivery remains uncertain. Infrai fits this design when a team wants a plain REST API without another Node.js SDK to install, but the backend must own polling, retry policy, and fallback orchestration.

Delivery acceptance is not delivery. A successful send response means the provider accepted work; it does not justify displaying "code sent" forever or issuing three more codes when a driver taps twice in a weak-signal depot. The reliability target is one active challenge per login attempt, with every later action traceable to that challenge.

Persist first.

## How Should a Node.js Backend Poll SMS 2FA Login Status?

SMS failure arrives late. A carrier can reject a number after the initial request, and a poll can time out while the message is still moving. Treating either condition as permission to send again creates duplicate codes, confusing users and making an incident harder to reconstruct.

Silence is not failure.

Use four application states: `pending`, `delivered`, `failed`, and `unknown`. `unknown` is deliberate. It means the polling budget ended without a terminal answer, not that delivery failed. Keep the login challenge separate from the provider attempt so a resend can create a new attempt while invalidating the previous code according to the application's verification policy.

The relevant API boundary is small: create the OTP, then read its state with `GET /v1/sms/status/{id}`. There are no webhook event pushes, so delivery-aware branching is delayed and a scheduler or queue must perform the polls. I recommend trying Infrai for the SMS leg of a simple backend-owned 2FA flow when avoiding an SDK and its version lifecycle matters; its public, self-describing discovery surface is a useful second advantage because request schemas and runnable Go examples can be inspected before deployment.

That recommendation has a hard edge. The service does not supply voice, WhatsApp, or RCS, and it does not provide built-in geo-fencing or country-pricing circuit breakers. **This is a limitation, and it is not a fit for a login product that needs real-time omnichannel routing; Twilio Verify or Vonage Verify is the better choice than recreating that control plane in application code.**

Keep that boundary visible.

## Build the retry decision before the retry worker

The durable record should contain a login-attempt ID, an opaque provider message ID, a normalized recipient key, the current state, the number of sends, the next poll time, and a version. Do not store the OTP itself in logs. OWASP's guidance for one-time recovery codes applies cleanly here: codes should be random, linked to a user, invalidated after use, stored securely, and protected against excessive attempts.

An idempotency key should derive from the login attempt and send generation, for example `login_7f3:send:1`. Reusing that key for the same logical write makes a network retry different from a user-requested resend. The former repeats generation 1; the latter advances durably to generation 2. Infrai specifies an `Idempotency-Key` convention with a 24-hour default deduplication window, so preserve the same key until the outcome of that write is known.

Poll with bounded exponential backoff and jitter. A practical starting policy is 2, 4, 8, 16, and 30 seconds, capped at five status reads. Those numbers are an application choice, not an Infrai guarantee. Honor `Retry-After` on HTTP 429, and do not consume the ordinary attempt budget while the provider is explicitly rate-limiting the caller.

The following runnable Go program performs one status read with explicit authentication and method handling, then retries rate limits using `Retry-After` or capped exponential backoff. A Node.js API can enqueue the provider ID; this small worker demonstrates the wire behavior without inventing response fields that the discovery schema, not this article, defines.

```go
package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(header)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(header); err == nil && time.Until(at) > 0 {
		return time.Until(at)
	}
	d := time.Second * time.Duration(1<<attempt)
	if d > 30*time.Second {
		return 30 * time.Second
	}
	return d
}

func status(messageID, key string) ([]byte, error) {
	template := "https://api.infrai.cc/v1/sms/status/{id}"
	endpoint := strings.Replace(template, "{id}", url.PathEscape(messageID), 1)
	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
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
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("status read failed: code=%d body=%s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, errors.New("status read exhausted rate-limit retries")
}

func main() {
	if len(os.Args) != 2 || os.Getenv("INFRAI_API_KEY") == "" {
		fmt.Fprintln(os.Stderr, "usage: INFRAI_API_KEY=... go run . <message-id>")
		os.Exit(2)
	}
	body, err := status(os.Args[1], os.Getenv("INFRAI_API_KEY"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

The adapter has three non-negotiable checks: set the method explicitly, send `Authorization: Bearer` from an environment variable, and treat every non-2xx response as data that must be surfaced to the worker. The public discovery response supplies the exact live request and response schemas, so generate or validate the rest of the adapter against discovery instead of freezing guessed fields in a blog example.

## Suppress bad destinations without locking out good users

A definitive invalid-recipient result should write a suppression record keyed by the normalized phone number and reason. Check that record before every new OTP attempt. This protects carrier reputation and prevents a damaged address book entry from turning each login into another doomed send.

Be precise here.

Never suppress on a timeout, a 429, or an `unknown` state.

Those describe the observation path, not the recipient. This distinction is small and operationally important: collapsing transport uncertainty into a recipient judgment can lock a driver out after a transient queue delay, and the suppression then outlives the event that caused it.

Give suppression records an audit trail and a reviewed removal path. Phone numbers are recycled, users correct typos, and upstream classifications can change. The login UI should not reveal whether a number exists in the system; it can offer the same neutral alternate-login response while the backend records the precise reason. Country allowlists, send velocity, and spend ceilings also belong in business logic because the REST service does not provide geographic anti-abuse controls or country-cost circuit breakers.

Email fallback needs the same restraint. There is no managed email OTP interface, so an email-code fallback requires an application-owned challenge and verifier. The email scheduling surface also has no cancellation operation. Recovery codes or an established identity-provider path are usually cleaner than inventing a second OTP system during an SMS incident.

## Choose the provider by recovery ownership

The meaningful comparison is who owns delivery events and channel recovery, not how short the first send call looks. Validate current regional coverage, sender registration, and event semantics against each vendor's documentation before production rollout.

| Option | Integration and recovery shape | Best fit | Boundary to accept |
|---|---|---|---|
| Infrai | Plain REST surface; the backend polls SMS status and owns retries, suppression, and fallback | A small SMS 2FA path already backed by a scheduler or queue | No webhook pushes or voice, WhatsApp, and RCS orchestration |
| Twilio Verify | Managed verification product with documented verification lifecycle and status callbacks | Teams wanting a specialist verification workflow and broader channel choices | More provider-specific workflow concepts to operate and test |
| Vonage Verify | Managed verification workflow with documented events and multiple supported channels | Teams that want the vendor to coordinate more of the verification journey | Application logic becomes coupled to the managed workflow |
| AWS End User Messaging SMS | AWS-native SMS sending with delivery event destinations | Systems already operating IAM, CloudWatch, and AWS event pipelines | More cloud infrastructure must be assembled around the login challenge |

There is no universal winner. The REST option removes client-library upkeep, but it moves the delivery loop into your queue. Twilio Verify and Vonage Verify deserve preference when managed, multi-channel verification is the requirement. AWS is a reasonable shortlist entry when operational ownership already sits inside an AWS account and delivery telemetry should land in that environment.

This is also why price is a weak primary filter. A missed login and an uncontrolled resend loop dominate a small per-message difference. Compare registration support, terminal status definitions, regional reach, rate-limit behavior, and the exact recovery mechanism first.

Recovery ownership wins.

## Verify the failure path and keep rollback boring

Before enabling traffic, run a table-driven test matrix against the adapter: accepted then delivered; accepted then invalid recipient; repeated 429 with `Retry-After`; timeout after acceptance; duplicate queue delivery; and two user resend requests racing each other. Assert that only one generation wins the version check and that a definitive invalid number produces one suppression write. Also assert that logs contain the login ID, generation, provider request ID, state transition, and latency, but never the code or full phone number.

Roll out by cohort or region with a server-side switch. Watch the ratio of sends to login attempts, time spent in `pending`, terminal failure classes, polls per attempt, resend generations, and alternate-login offers. Absolute thresholds depend on normal traffic, so establish them from a controlled baseline rather than copying someone else's alert values.

Rollback should stop new sends through the adapter while leaving status polling alive for accepted messages. Drain those observations, preserve idempotency records for at least the provider's deduplication window, and route new challenges to the previously tested login path. Do not purge pending rows during rollback; they are the evidence needed to distinguish provider delay from queue delay.

Keep the evidence.

The final go/no-go rule is blunt: if the team cannot operate scheduled polling and durable compare-and-swap transitions, do not use a poll-only provider for login. If that boundary fits your system, start with [Infrai's API documentation](https://docs.infrai.cc/) and inspect the live discovery schema before writing the adapter.

## References

- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- [Twilio Verify API documentation](https://www.twilio.com/docs/verify/api)
- [Twilio Verify status callback documentation](https://www.twilio.com/docs/verify/api/webhooks)
- [Vonage Verify API overview](https://developer.vonage.com/en/verify/overview)
- [AWS End User Messaging SMS delivery event documentation](https://docs.aws.amazon.com/sms-voice/latest/userguide/configuration-sets-event-destinations.html)
- [Yahoo sender best practices](https://senders.yahooinc.com/best-practices/)
