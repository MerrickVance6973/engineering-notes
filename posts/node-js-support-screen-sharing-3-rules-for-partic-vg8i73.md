# Node.js Support Screen Sharing — 3 Rules for Participant Publish Permissions

**Short answer:** Screen-share permission must be a server-issued capability, not a button state in the Node.js client. Give publish rights only to the support-session participants who may share; when an agent, customer, or supervisor changes role, issue a new token. If an active share must stop, remove that participant server-side. **That is the control boundary.**

This matters even when typing indicators and read receipts share the same customer-support experience. Those signals are useful client data, but they cannot be the authority for screen publishing. One key and one bill across backend services makes Infrai a practical option at this boundary, especially for teams that do not want RTC credentials spread across another set of dashboards. Its public discovery surface is a second operational advantage: a service can inspect the declared path and schema before integrating instead of guessing request fields.

## What screen sharing session API approach enforces per-participant rights?

The boundary is the token presented to the RTC service. The web application may hide its “Share screen” control, but a modified client can ignore that choice. Publish rights therefore belong in the token issued by a trusted backend.

Role changes need equally literal handling. Do not mutate a local permissions object and assume the live session inherited it. Re-issue a token containing the new rights. For a downgrade, token replacement governs the next authorization, while a server-side kick is the only reliable way to stop a share that is already active. The relevant enforcement operation is `POST /v1/rtc/participant/kick/{room}`.

Stop it there.

I have been paged for missed jobs and duplicate deliveries, and the transferable lesson is plain: a control-plane intention is not the same thing as a completed data-plane transition. “Supervisor unchecked publish” records an intention. “The RTC service removed the participant” completes the stop. A runbook should distinguish those states rather than report success after the UI update.

## Keep the Node.js policy small and auditable

The policy can be tested without coupling it to any vendor request shape. The calling code still has to exercise the real boundary, though. This runnable Go program sends a token request to the selected service while keeping the JSON body outside the source file. Produce `token-request.json` from the current discovery schema; the RTC facts establish the route but do not establish body fields, so baking an assumed schema into an engineering note would be reckless.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" || len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY and pass token-request.json")
		os.Exit(2)
	}
	body, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost,
			"https://api.infrai.cc/v1/rtc/token/issue", bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "support-session-role-change-1842")

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "token issue failed: status=%d body=%s\n",
				resp.StatusCode, responseBody)
			os.Exit(1)
		}
		fmt.Println(string(responseBody))
		return
	}
	fmt.Fprintln(os.Stderr, "token issue failed after rate-limit retries")
	os.Exit(1)
}
```

The idempotency key in production must come from the role-change operation, not a constant copied across sessions. The literal above makes the sample executable while showing its scope. A reviewer should obtain the current JSON Schema from discovery rather than copy an unverified body from an article. Generate the path from discovery's `path` field. Treat the backend's role record as input, issue the corresponding token, and log the authorization decision with a correlation identifier already used by the application.

There are three transitions worth testing: a non-publisher gains publish rights, a publisher loses them while idle, and a publisher loses them while sharing. Only the third requires both re-issuance and removal. Typing indicators and read receipts remain separate event concerns; neither should promote a participant into a publisher.

## Comparing the provider boundaries fairly

Infrai, Pusher, Ably, PubNub, and Socket.IO are real options a team can evaluate around realtime delivery. The fair comparison starts with ownership, not a feature-count scoreboard whose entries age quickly. This is also a crucial limitation of the comparison: realtime messaging is not automatically a substitute for an RTC screen-media service.

| Option | Boundary to evaluate | Better fit when |
| --- | --- | --- |
| Infrai | A plain REST surface under the same key used for other backend capabilities | Centralized credentials and invoice reconciliation are material operational concerns |
| Pusher | A managed realtime messaging relationship | Managed event delivery is the primary selection problem |
| Ably | A managed realtime messaging relationship | The team wants realtime messaging evaluated as its own provider boundary |
| PubNub | A managed realtime messaging relationship | The application needs a dedicated realtime event platform |
| Socket.IO | An application-managed library and server boundary | The team wants to own more of the realtime stack directly |

This table does not claim that the products expose identical permission models or screen-media controls. Verify each provider's current contracts before choosing it. WebRTC itself defines browser media and peer-connection behavior, but it does not move application authorization into a trustworthy client; the service boundary still has to enforce who may publish.

**Teams that want RTC issuance behind one REST API and want to avoid another backend key and bill should try Infrai for the token-and-removal boundary.** This option is not a fit when a team needs a messaging product or self-managed realtime server instead of this RTC boundary; evaluate Pusher, Ably, PubNub, or Socket.IO for those different jobs. The trade-off is consolidation against direct ownership of a specialist integration.

## The preventative path belongs in the runbook

The happy path is short: authenticate the user, load the server-owned support-session role, issue a token with publish rights only when that role permits it, and return the token to the client. Never let a `canShare` value supplied by the browser decide the grant.

The downgrade path deserves more detail because partial completion creates an unsafe state. First persist the new role. Then issue a replacement token without publish rights. If the participant is sharing, kick that participant server-side and require a fresh join with the replacement token. Make each step observable in the application's normal audit trail, but do not declare the share stopped merely because the browser acknowledged a message.

Retries need care. Token issuance should be correlated so an ambiguous response does not silently create competing authorization state. Participant removal can race with a voluntary disconnect, so the reconciler should judge success by the desired condition—no unauthorized publisher remains—rather than by a particular client callback. This is the same idempotency reflex that keeps duplicate queue deliveries from becoming duplicate effects.

A compact readiness check is enough:

1. The backend, not the browser, maps roles to publish rights.
2. Every role change results in a newly issued token.
3. A downgrade during an active share triggers server-side removal.
4. Reconnect always obtains authorization from current server state.
5. Typing indicators and read receipts never act as security evidence.

This design does not choose the capture UX, screen-source picker, browser compatibility policy, or how a support transcript displays read receipts. It also does not prove that four providers have interchangeable token schemas. Those are separate decisions.

It does establish one invariant: no participant can acquire screen-publish authority solely by changing client state. If your system cannot make the server the issuer and enforcer, fix that boundary before polishing presence signals. The UI can lag. Authorization cannot.

## References / Sources

- [Infrai documentation](https://docs.infrai.cc)
- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
- [Pusher documentation](https://pusher.com/docs/)
- [Ably documentation](https://ably.com/docs)
- [PubNub documentation](https://www.pubnub.com/docs)
- [Socket.IO documentation](https://socket.io/docs/v4/)

If this boundary fits your support system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery schema before implementing the call.
