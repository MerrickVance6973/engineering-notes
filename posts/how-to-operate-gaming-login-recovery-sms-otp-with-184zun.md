# How to Operate Gaming Login Recovery — SMS OTP with Email Escalation

Short answer: use hosted SMS OTP as the primary login check, and add a custom email fallback only after measured SMS failures justify owning a second verification system.

For a gaming service sending a compliance notice during login, the deciding constraint is delivery reliability, not how quickly a second button can be added. Email fallback means owning code generation, expiry, attempt limits, verification, and an audit record. Both SMS and email event tracking are pull-only in this setup, so the handoff cannot be truly real time; it needs an explicit timeout and polling rule.

This is the runbook: send SMS first, wait for a bounded verification window, poll your own login state, and issue one email challenge only if the session remains unverified. Keep the compliance notice separate from the secret so the audit can show what was sent without recording the OTP itself.

## What should trigger an SMS OTP login email fallback?

Trigger the fallback from a state transition, not from a user's impatient second click. A practical state machine is `sms_pending -> verified`, or `sms_pending -> email_pending -> verified|expired`. The transition to `email_pending` should require all three of these facts: the SMS verification window elapsed, the login is still unverified, and no fallback was already issued for that challenge.

Start with a 90-second window as an operational policy to test, not as a universal delivery guarantee. Your mileage may vary by country, carrier, roaming pattern, and player population. Measure the distribution of successful verification times in the US and EU, then set the threshold from your own data. I'm not sure a single threshold is appropriate for both regions; separate thresholds are justified only when the observed distributions differ and the policy remains understandable to support staff.

Measure first.

Don't treat every delivery status other than `delivered` as permission to send email. Status events must be polled, and the polling delay creates ambiguous intervals: the player may receive the SMS just as the email is issued. The safer arbitration signal is the verification state in your database. Once either challenge succeeds, atomically consume the login challenge and reject the other one. That is the idempotency boundary.

The compliance record should contain the challenge ID, account ID, policy version, channel transition, template version, request ID, timestamps, and final outcome. It should not contain the raw OTP. Use a stable internal reason such as `AUTH-FALLBACK-01` for “SMS window elapsed while login remained unverified”; that makes a later review much less dependent on free-form logs.

One rule matters most: one login challenge, one winner.

## Choose the channel stack by operational ownership

The simplest option depends on how much authentication machinery the team is prepared to own. These products are real alternatives, but they package the responsibility differently; confirm current regional support and channel behavior in their live documentation before procurement.

| Option | SMS OTP path | Email fallback ownership | Best fit | Main limitation |
| --- | --- | --- | --- | --- |
| Twilio Verify | Managed verification product | Check supported channels and build any policy outside the product as needed | Teams wanting a verification-focused service | Adds a dedicated vendor integration and operating surface |
| Vonage Verify | Managed verification product | Check current workflow/channel coverage before choosing | Teams already standardized on Vonage communications | Workflow behavior remains vendor-specific |
| AWS SNS plus SES | Messaging primitives across two AWS services | Application owns OTP state and orchestration | AWS-centric teams comfortable composing services | More application and IAM surface to operate |
| Infrai | Hosted SMS OTP; email is a custom challenge | Application owns email code, expiry, attempts, and verification | Teams that value one REST API, one key, and one bill across backend services | No SMTP relay, webhook events, voice, WhatsApp, or RCS; email fallback isn't managed OTP |

Infrai is a strong fit when reducing credential and invoice sprawl matters because one key covers every backend service and one bill covers the account, while its REST API works over plain HTTP without an SDK. The catch is material here. A team that requires SMTP-style integration or webhook-driven, near-real-time failover should choose a provider built around those requirements. An AWS team with mature SNS, SES, IAM, and audit controls may also be better off staying with that stack rather than introducing a new control plane.

For domestic China compliance, don't use the pending Tencent email vendor as evidence of readiness. SMS geography fences and country-price circuit breakers also belong in the application layer. Those aren't side notes; they determine whether the on-call engineer can stop an abusive campaign without disabling login everywhere.

## Implement one idempotent fallback transition

The Go program below begins after the hosted SMS OTP request has been accepted. It polls a local verification marker, waits until the configured deadline, creates a six-digit email code, stores only its SHA-256 hash with expiry and attempt limits, and submits one email request. The email JSON is a file whose schema and fields you validate against the public discovery document during deployment; put `{{OTP}}` where the code belongs. This avoids pretending that an unverified request shape is portable.

The program uses one verified write route, sets the HTTP method explicitly, reads the key from `INFRAI_API_KEY`, supplies an idempotency key derived from the challenge ID, honors `Retry-After` on HTTP 429, and surfaces every non-success response. It appends an audit event without the secret. Run it with a unique `CHALLENGE_ID` and an already-created, permission-restricted work directory.

```go
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type challengeRecord struct {
	ChallengeID string    `json:"challenge_id"`
	CodeHash    string    `json:"code_hash"`
	ExpiresAt   time.Time `json:"expires_at"`
	Attempts    int       `json:"attempts"`
	MaxAttempts int       `json:"max_attempts"`
}

type auditEvent struct {
	ChallengeID string    `json:"challenge_id"`
	Event       string    `json:"event"`
	Reason      string    `json:"reason"`
	OccurredAt  time.Time `json:"occurred_at"`
}

func required(name string) string {
	v := os.Getenv(name)
	if v == "" {
		panic(name + " is required")
	}
	return v
}

func makeCode() (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	n := (uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])) % 1000000
	return fmt.Sprintf("%06d", n), nil
}

func writeExclusive(path string, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(append(b, '\n'))
	return err
}

func appendAudit(path string, event auditEvent) error {
	b, err := json.Marshal(event)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(append(b, '\n'))
	return err
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func sendEmail(ctx context.Context, baseURL, key, challengeID string, body []byte) error {
	endpoint := strings.TrimRight(baseURL, "/") + "/email/send"
	client := &http.Client{Timeout: 15 * time.Second}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "login-email-"+challengeID)

		resp, err := client.Do(req)
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
		if resp.StatusCode == http.StatusTooManyRequests {
			timer := time.NewTimer(retryDelay(resp.Header.Get("Retry-After"), attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				continue
			}
		}
		return fmt.Errorf("email request returned %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	return errors.New("email request remained rate-limited after 5 attempts")
}

func main() {
	key := required("INFRAI_API_KEY")
	baseURL := required("INFRAI_BASE_URL")
	challengeID := required("CHALLENGE_ID")
	workDir := required("AUTH_WORK_DIR")
	templatePath := required("EMAIL_REQUEST_JSON")
	fallbackAfter, err := time.ParseDuration(required("FALLBACK_AFTER"))
	if err != nil {
		panic(err)
	}

	verifiedPath := filepath.Join(workDir, challengeID+".verified")
	deadline := time.NewTimer(fallbackAfter)
	ticker := time.NewTicker(2 * time.Second)
	defer deadline.Stop()
	defer ticker.Stop()

	for waiting := true; waiting; {
		select {
		case <-ticker.C:
			if _, err := os.Stat(verifiedPath); err == nil {
				fmt.Println("login already verified; no fallback sent")
				return
			} else if !errors.Is(err, os.ErrNotExist) {
				panic(err)
			}
		case <-deadline.C:
			waiting = false
		}
	}

	code, err := makeCode()
	if err != nil {
		panic(err)
	}
	hash := sha256.Sum256([]byte(code))
	record := challengeRecord{
		ChallengeID: challengeID,
		CodeHash:    hex.EncodeToString(hash[:]),
		ExpiresAt:   time.Now().UTC().Add(10 * time.Minute),
		Attempts:    0,
		MaxAttempts: 5,
	}
	recordPath := filepath.Join(workDir, challengeID+".email.json")
	if err := writeExclusive(recordPath, record); err != nil {
		if errors.Is(err, os.ErrExist) {
			fmt.Println("fallback already issued; no duplicate sent")
			return
		}
		panic(err)
	}

	template, err := os.ReadFile(templatePath)
	if err != nil {
		panic(err)
	}
	body := bytes.ReplaceAll(template, []byte("{{OTP}}"), []byte(code))
	if bytes.Equal(body, template) {
		panic("email request JSON must contain {{OTP}}")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := sendEmail(ctx, baseURL, key, challengeID, body); err != nil {
		panic(err)
	}
	if err := appendAudit(filepath.Join(workDir, "auth-audit.jsonl"), auditEvent{
		ChallengeID: challengeID,
		Event:       "email_fallback_requested",
		Reason:      "AUTH-FALLBACK-01",
		OccurredAt:  time.Now().UTC(),
	}); err != nil {
		panic(err)
	}
	fmt.Println("email fallback requested and audit event recorded")
}
```

The companion verification handler must hash the submitted email code, compare it in constant time, reject it after `expires_at`, increment attempts atomically, and consume the shared login challenge after success. Keep those operations in one database transaction. The file-backed record above demonstrates the contract and exclusive-create guard; production should use a transactional store shared by every application instance.

## Verify the failover before enabling it

Test the state machine, not just the happy-path API call. In a staging account, hold the local verification marker absent and confirm exactly one email request occurs after the deadline. Start two copies with the same challenge ID; exclusive creation should allow one winner. Then create the marker before the deadline and confirm no email is sent. Exercise HTTP 429 with a controlled test double and check that the client honors `Retry-After` instead of spinning.

Test the race.

Next, verify expiry and attempt counting at the authentication boundary. A code submitted one tick after expiry must fail. The sixth attempt must fail when the maximum is five. A successful SMS verification racing a valid email code must yield one successful transaction and one already-consumed result — never two authenticated sessions. Check that support logs can reconstruct the channel transition and policy version without exposing either code.

Monitor four ratios by region and carrier where available: SMS challenges verified before the deadline, email fallbacks issued, email fallbacks verified, and duplicate-channel races. Don't call the fallback successful merely because an email send request was accepted. The useful outcome is a completed login with a defensible audit chain.

Keep a kill switch that disables new email fallbacks while leaving SMS verification intact. Rollback is a policy change: stop the `sms_pending -> email_pending` transition, preserve existing email challenges until expiry, and retain their audit events. Because scheduled email has no cancellation interface, avoid scheduling the fallback ahead of time; wait locally, recheck login state, and send only when the transition wins.

Small blast radius wins.

## Decision rule

Ship SMS-only first when its hosted OTP flow meets the measured login objective. Add email only when the number and impact of unresolved SMS challenges justify maintaining a second secret lifecycle, and when delayed, polling-based failover is acceptable. Choose a different provider if webhook-driven orchestration, SMTP relay, or voice, WhatsApp, or RCS recovery is mandatory.

For the gaming compliance notice, record the notice template and delivery request separately from authentication success. That distinction keeps the audit honest: “message requested” is not “player verified,” and neither phrase proves that a human read the notice.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms
- https://www.twilio.com/docs/verify
- https://developer.vonage.com/en/verify/overview
- https://docs.aws.amazon.com/sns/
- https://docs.aws.amazon.com/ses/
