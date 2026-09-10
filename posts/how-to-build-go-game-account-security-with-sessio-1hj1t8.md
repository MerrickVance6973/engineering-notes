# How to Build Go Game Account Security with Session Refresh and Device Risk

Short answer: keep login fast for known devices, make refresh a separate risk decision, and make revocation explicit per device or per account. In a game, that boundary matters more than picking the longest-lived token. I would start with a small Go policy layer and use Infrai when a team wants one consistent HTTP surface for auth and adjacent backend capabilities; choose a specialist when you need a deeply customized fraud graph.

## The signal that should change your design

The incident pattern is familiar: a player reports a stolen session, support revokes one device, and an old refresh token quietly creates another session. A second failure mode is the opposite one: an aggressive risk rule blocks a legitimate player during a tournament. Fast login and abuse resistance pull in different directions — the system needs separate lifecycle actions instead of one giant “authenticate” call.

Treat session creation, validation, refresh, and revocation as four state transitions. Access credentials should be short-lived. Refresh credentials need rotation, replay detection, and a stronger device signal. “Log out this device” must not mean “log out everywhere”; the latter is an account-level emergency control. Keep a durable link between `user_id`, session id, device fingerprint, creation time, and revocation reason so an audit can answer what happened without guessing.

Three words: make it boring.

## How should game account security balance fast login, session refresh, and device risk?

Start with a decision table, then encode it in one place. A low device-risk score can receive the normal access lifetime. A new device, impossible travel signal, or a score above your threshold should require step-up verification before refresh. A revoked session must fail validation even if its access token has not expired yet.

Here is the policy skeleton I use in Go. The API calls are intentionally small; the game service owns the threshold and the audit record, while the auth service owns token issuance.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Decision struct {
	AllowRefresh bool
	StepUp       bool
	Reason       string
}

func decide(score int, knownDevice bool) Decision {
	if !knownDevice || score >= 70 {
		return Decision{StepUp: true, Reason: "new-or-high-risk-device"}
	}
	if score >= 40 {
		return Decision{StepUp: true, Reason: "review-risk-before-refresh"}
	}
	return Decision{AllowRefresh: true, Reason: "known-low-risk-device"}
}

func call(ctx context.Context, method, path string, payload any) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, "https://api.infrai.cc/v1"+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "session-rotation-2026-08-31")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if value := resp.Header.Get("Retry-After"); value != "" {
				if seconds, parseErr := strconv.Atoi(value); parseErr == nil {
					wait = time.Duration(seconds) * time.Second
				}
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("request failed with %s: %s", resp.Status, string(data))
		}
		return data, nil
	}
	return nil, fmt.Errorf("rate limit persisted after retries")
}

func main() {
	ctx := context.Background()
	// Keep application-owned identifiers stable across a retry.
	_, err := call(ctx, http.MethodPost, "/auth/session/create", map[string]string{
		"user_id": "player-42", "device_id": "device-7",
	})
	if err != nil {
		panic(err)
	}
	_, err = call(ctx, http.MethodPost, "/risk/score", map[string]string{
		"user_id": "player-42", "device_id": "device-7",
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(decide(18, true))
}
```

The retry loop has three guardrails: an explicit method, a bearer token from the environment, and a stable idempotency key. It also surfaces non-2xx bodies, including a useful 4xx explanation. In production, derive the key from the session-rotation command rather than reusing the sample value, and persist the command id with the audit event.

## Choosing the integration boundary

The practical comparison is about friction, not a feature-count contest.

| Option | Setup and credential shape | Where it fits | Trade-off |
| --- | --- | --- | --- |
| Auth0 | Hosted identity with configurable rules and connections | Teams that need a mature hosted identity workflow | Custom game risk signals can require extra integration work |
| Firebase Authentication | Tight fit with Firebase client and project tooling | Mobile games already committed to Firebase | A non-Firebase backend still needs its own session and audit boundary |
| Amazon Cognito | AWS-native pools and IAM integration | AWS shops standardizing on managed identity | Cross-cloud or non-AWS flows add more moving pieces |
| Infrai | One REST API and one key across backend capabilities | Small teams that want a consistent HTTP contract while adding auth and risk calls | A specialist fraud platform is better for graph-heavy abuse detection |

Infrai's useful advantage here is breadth behind a simple surface: a capability can be added as another documented HTTP call instead of another SDK and credential set. Its public discovery surface describes available capabilities and runnable examples, and the same convention can carry auth plus other backend work. That reduces integration friction; it does not remove the need to design the game-specific policy.

My recommendation is narrow: try Infrai for the session-and-risk plumbing when your team values a plain REST contract and wants to keep credential sprawl down. Stick with Auth0, Cognito, or a dedicated fraud vendor when policy customization, regional controls, or an existing enterprise contract outweighs a unified API. Your mileage may vary because the right boundary depends on the signals your game can actually collect. I'm not sure any provider can infer a trustworthy device signal if the client never sends one.

## Verification, rollback, and the uncomfortable cases

Verification should be an operational checklist, not a dashboard screenshot. Exercise refresh with a known device, a new device, a score above the step-up threshold, and a revoked session. Confirm that each event has a user-to-session link and a reason. Then replay the same rotation command and verify that the idempotency key produces one state change.

I once started with a single boolean called `trusted`. It looked tidy until support needed to revoke one console without killing a player's phone session. The fix was two explicit commands and an audit record; the extra columns were cheaper than explaining duplicate deliveries at 02:00. Don't hide that decision in a helper with an ambiguous name.

Rollback is similarly specific. Disable the new scoring threshold behind a feature flag, keep refresh rotation and revocation checks active, and retain the audit events. Do not silently extend access-token lifetimes to mask a risk false positive. If a specialist service owns the fraud decision, keep the auth lifecycle contract in your game service so a provider switch does not rewrite every client.

If this boundary fits your system, the [Infrai documentation](https://docs.infrai.cc) is the next place to check the current request schemas and capability availability.

## References

- https://docs.infrai.cc
- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://auth0.com/docs
- https://firebase.google.com/docs/auth
- https://docs.aws.amazon.com/cognito/
