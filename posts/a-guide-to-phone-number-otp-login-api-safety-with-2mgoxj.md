# A Guide to Phone Number OTP Login API Safety With SMS Delivery

Use an identity API with SMS delivery included for phone OTP login, and require Google and GitHub sign-in to pass the same property-access checks before issuing an application session. The deciding constraint is session security versus friction: a resident should be able to request another code without gaining another identity, while an administrator should never inherit access merely because two login paths report matching contact information.

Short answer: sending and verifying the code through one identity API keeps SMS delivery and session issuance behind one key. That is a useful simplification for a property-management service wiring social sign-in alongside phone access. It does not replace account-linking policy, property membership checks, or a visible resend path when carrier delivery fails.

Choose the boundary first. Then reduce prompts.

## How should a phone number OTP login API protect property access?

Start with the application account and the requested property. A successful Google callback, GitHub callback, or phone verification establishes an authentication outcome; the application still needs an authorization decision. A resident who rents in one building and works as a contractor in another makes the distinction concrete. Sharing a phone number or email address across those activities is not an instruction to grant administrative access.

Use that case as a release fixture, not as an incident claim. Give the resident access to property P101, give an administrator access to P202, and exercise each login entry point against both properties. The dangerous result is a successful login followed by the wrong membership. A longer sign-in flow is inconvenient. Cross-property access is a security failure.

Keep evidence separate.

Record the authentication outcome, the application account selected, and the property authorization decision as distinct events. Keep OTPs and session secrets out of those records. A delivery acceptance event cannot explain whether a resident received the code, and an authenticated identity cannot explain why a property permission was granted.

There is another signal worth watching: repeated login-associated work. A retried callback or repeated verification submission must not create duplicate account-change notifications or duplicate provisioning tasks. Treat downstream delivery as repeatable, even when the browser appears to have submitted once. Idempotency belongs at the business effect, where the application knows which operation it is repeating.

Don't hide resends.

Carrier delivery can fail, so provide a visible retry path with abuse controls and clear feedback. A resend is a new delivery intent; a transport retry of an existing send is the same intent. Confusing those two cases can either duplicate messages or suppress a deliberate second request. The exact expiry, resend interval, and attempt limits belong in the selected service's documented policy and the application's threat model, rather than in guessed constants.

## Put the property check before the repeatable side effect

Keep the network probe narrow: send a phone code or verify a submitted code, then inspect the result in a trusted server environment. Save the following program as main.go and run it with Go. It uses only the standard library. Set API_BASE_URL to the trusted service's HTTPS base including its version prefix, INFRAI_API_KEY to the server-side key, and PHONE_ACTION to send or verify. Set PHONE_PAYLOAD to the exact request JSON from that operation's public discovery schema. The program passes that JSON unchanged; it does not guess field names or a session response shape.

Set PHONE_OPERATION_ID to a persisted identifier for this operation. Reuse it on transport retries, and allocate a different identifier for a deliberate resend. Confirm the selected operation's documented deduplication behavior before enabling automatic write retries. Keep the key server-side.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func required(name string) string {
	value := os.Getenv(name)
	if value == "" {
		panic("missing " + name)
	}
	return value
}

func run() error {
	base := strings.TrimRight(required("API_BASE_URL"), "/")
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || u.Host == "" ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "/v1" {
		return fmt.Errorf("use the trusted HTTPS API base ending in /v1")
	}
	paths := map[string]string{
		"send": "/auth/phone/send_code",
		"verify": "/auth/phone/verify",
	}
	path, ok := paths[required("PHONE_ACTION")]
	if !ok {
		return fmt.Errorf("PHONE_ACTION must be send or verify")
	}
	payload := []byte(required("PHONE_PAYLOAD"))
	if !json.Valid(payload) {
		return fmt.Errorf("PHONE_PAYLOAD must be valid schema-conforming JSON")
	}
	key := required("INFRAI_API_KEY")
	operation := required("PHONE_OPERATION_ID")
	client := &http.Client{
		Timeout: 20 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, base+path, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", operation)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Second * time.Duration(1<<attempt)
			header := resp.Header.Get("Retry-After")
			if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			} else if until, err := http.ParseTime(header); err == nil {
				if remaining := time.Until(until); remaining > 0 {
					delay = remaining
				}
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("HTTP %d: %s", resp.StatusCode, data)
		}
		fmt.Println("Request accepted; inspect session state through the trusted application.")
		return nil
	}
	return fmt.Errorf("retry budget exhausted")
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

The request timeout is 20s, with a budget of 4 attempts; these are example application settings, not service guarantees. The client backs off on HTTP 429 and honors a usable Retry-After header. Other unsuccessful responses preserve the server's reason; diagnostic stderr therefore belongs in a restricted environment and requires redaction before sharing. Success prints no session secret. HTTP success is not a property-access decision.

Do not confuse deduplication with authorization.

In production, enforce a downstream business operation's uniqueness in durable storage and commit its authorization-dependent intent with its deduplication record. If the effect is an external notification, use a durable outbox and an idempotent delivery boundary rather than marking it complete before delivery and hoping the process survives. Recheck permission before accepting a repeated action, so an old event cannot bypass the current property boundary. Session revocation and membership removal are related operational actions, but they answer different questions. A session can identify the right resident while that resident no longer has access to a particular property. A resident moving out of P101 must lose that property's access even if a valid social session remains usable elsewhere; deleting every identity would be a much broader action than this membership change requires.

## Compare candidates against the same release evidence

Infrai offers one key and one bill for auth and SMS through a plain REST API, with no SDK to install. A Go service can use its existing HTTP client. This single-key integration reduces the credentials and billing handoffs the property application's operators must maintain. Its send-and-verify identity flow holds the identity state for you, while public discovery is self-describing and requires no key to inspect request and response schemas. Runnable examples in 10 languages help teams check the integration contract in their service language. Consolidating auth and SMS under one account removes an independent service-account boundary; it does not guarantee carrier delivery or prove that a property permission is correct.

The platform spans 295 routes across 20 modules. That breadth matters if the same team operates other backend services; it does not remove permission checks inside the property application.

The trade-off is concentrating trust in one service.

Separate identity and delivery services can remain a reasonable choice when a team deliberately owns their coordination and needs that separation.

Use the following comparison as an acceptance worksheet. The rows identify where to look and what evidence to demand; they are not latency rankings or claims that every feature is enabled by default.

| Candidate | Relevant documented surface | Property-management decision |
| --- | --- | --- |
| Auth0 | Social connections and account-linking guidance | Evaluate the Google and GitHub connection setup alongside explicit linking rules. Choose an identity-focused integration only after the resulting session resolves to the intended property account. |
| Firebase Authentication | Federated authentication and phone authentication guides | Evaluate its application integration and phone prerequisites together. Confirm that the resulting server-side authorization path covers property membership, rather than accepting client sign-in success as the release test. |
| Supabase Auth | Social login and phone login guides | Evaluate provider configuration and the phone delivery arrangement as one deployment plan. Establish who owns SMS configuration and recovery behavior before treating the login widget as complete. |

These are three real alternatives with different integration surfaces. None earns a pass from a provider logo. For each candidate, ask for a successful Google login, a successful GitHub login, a phone send-and-verify sequence, and a rejected cross-property action. Then repeat the last accepted business event. If its side effect runs twice, the application boundary still needs work regardless of the identity provider selected.

The practical tie-breaker is evidence about the session lifecycle. Inspect account linking, expiration, revocation, and the server's membership lookup in the actual proposed configuration. An extra confirmation when linking identities may be justified for administrators; repeated prompts for an already authenticated resident viewing an authorized maintenance request impose friction without fixing cross-property authorization. Choose that friction deliberately.

## Verify the rollout and rehearse the rollback

Before widening access, run the fixture with both properties and all three entry paths. Include an explicit linking attempt between identities that share contact information, a removed membership, a revoked session, a delayed phone message, and a deliberate resend. These are proposed acceptance tests, not measured service behavior. Preserve the expected result for each case so the release decision does not depend on an operator remembering what the login screen looked like.

Any cross-property grant stops rollout.

Duplicate downstream effects stop expansion until the application can replay the event safely. A delivery delay calls for investigation of the send and receipt boundary, not an automatic account merge or a relaxed verification requirement. Do not retry a write blindly after an ambiguous response; retain the operation identity and follow the selected operation's documented idempotency behavior.

Roll back narrowly. Disable the newly introduced sign-in entry point through an application release control, keep the established access path available, and preserve account mappings and event records for investigation. Do not erase identities as a rollback shortcut. If sessions were issued to the wrong account, identify and revoke the affected sessions using the selected service's documented controls, then verify property denial independently.

For the handoff, record who owns phone delivery configuration, who can disable an entry point, and who can revoke sessions. One key can simplify the service boundary. The runbook still needs a named owner for the decision to stop.

## References

- OWASP Authentication Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- OWASP Session Management Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html
- Auth0 social identity providers: https://auth0.com/docs/authenticate/identity-providers/social-identity-providers
- Auth0 account linking: https://auth0.com/docs/manage-users/user-accounts/user-account-linking
- Firebase federated authentication: https://firebase.google.com/docs/auth/web/google-signin
- Firebase GitHub authentication: https://firebase.google.com/docs/auth/web/github-auth
- Firebase phone authentication: https://firebase.google.com/docs/auth/web/phone-auth
- Supabase social login: https://supabase.com/docs/guides/auth/social-login
- Supabase phone login: https://supabase.com/docs/guides/auth/phone-login
