# Node.js Support Chat Rooms — Scoped Tokens and Reconnect-Safe Delivery

Give each support conversation its own realtime channel, issue the visitor a token scoped only to that channel, and keep the durable transcript in your application database. The realtime service should wake clients up; it should not become the sole record of what an agent promised a customer. **For a reconnect-safe B2B support widget, database commit precedes fan-out, and replay closes every delivery gap.**

TL;DR: authenticate the visitor with the application, mint a short-lived conversation-scoped token on the server, commit every message under a client-generated ID, and fan it out only after that commit succeeds. On reconnect, fetch messages after the last persisted sequence before resuming live delivery. This produces a useful guarantee: at-least-once notification with effectively-once transcript insertion, rather than an unverifiable claim of exactly-once delivery.

## The incident lesson: live is not durable

I have been paged for missed jobs and duplicate deliveries. The same failure shape appears in chat: a process completes step one, loses its connection before step two, and a retry arrives without enough identity to distinguish recovery from duplication. I initially treated a successful publish as the important boundary. It is not. The database commit is the boundary because it is the only event from which the transcript, unread count, audit trail, and reconnect response can be rebuilt.

Consider a visitor sending message ID `01J...` while an agent's browser changes networks. The server inserts the message and then publishes a notification. If publication fails, an outbox worker retries it. If the browser receives the event twice, it collapses both copies by message ID. If it receives neither, its reconnect request asks for rows after sequence 418 and retrieves 419. No special recovery channel is required.

This invariant is deliberately modest: **the transcript must converge even when fan-out duplicates or disappears.** It does not promise that every tab renders every event exactly once. That stronger statement tends to dissolve as soon as a websocket disconnects between receipt and acknowledgement.

Email the transcript when the conversation closes, but derive that email from committed rows. A close notification racing ahead of the final live event should not produce a truncated transcript.

## What Node.js product support chat API approach survives reconnects?

A channel per conversation makes authorization reviewable. The token issuer can bind visitor `V` to conversation `C`; the browser never receives a credential broad enough to subscribe to another customer's room. Agent authorization remains a separate server-side decision based on tenant and assignment. Small boundaries are easier to revoke and harder to misuse.

Do not encode trust in a client-supplied channel name. The server must look up the conversation, verify tenant membership and participant access, then issue the scoped token. Keep its lifetime short enough for role changes to take effect, while allowing refresh through the authenticated application session. Never place the provider's management key in the widget.

Reconnect handling needs an explicit order. Pause live rendering, query the transcript after the client's last sequence, apply those rows in sequence order, then consume buffered realtime notifications and discard IDs already seen. There is a narrow race between the query and subscription unless the provider offers an atomic resume cursor; buffering plus deduplication handles that race without pretending it vanished.

Presence is useful for “agent is viewing,” but it is not proof of delivery. Neither is a websocket `send` returning successfully.

## The preventative write path

The database transaction described above remains the critical path behind a Node.js widget. The integration-specific piece is token issuance. Because the request schema is available from public discovery and may evolve, this small Go program accepts a JSON document that has already been constructed and validated against that schema; it does not guess at fields. It calls the verified token-issuance route with a management key held only on the server. The client-generated operation ID supplies retry identity, and a 429 response honors `Retry-After` before exponential backoff.

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

func main() {
	baseURL, key := os.Getenv("INFRAI_BASE_URL"), os.Getenv("INFRAI_API_KEY")
	body, opID := os.Getenv("TOKEN_REQUEST_JSON"), os.Getenv("TOKEN_OPERATION_ID")
	if baseURL == "" || key == "" || body == "" || opID == "" {
		panic("INFRAI_BASE_URL, INFRAI_API_KEY, TOKEN_REQUEST_JSON, and TOKEN_OPERATION_ID are required")
	}
	if !json.Valid([]byte(body)) {
		panic("TOKEN_REQUEST_JSON must be valid JSON matching the discovery schema")
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, baseURL+"/realtime/token/issue", bytes.NewBufferString(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", opID)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("token issue failed: status=%d body=%s", resp.StatusCode, responseBody))
		}
		fmt.Println(string(responseBody))
		return
	}
	panic("token issue failed after retries")
}
```

Generate `TOKEN_REQUEST_JSON` from the live discovery schema rather than copying fields from an old article. The application must first verify that the authenticated visitor belongs to the conversation represented in that request. The example deliberately runs server-side; sending `INFRAI_API_KEY` to a browser would expand a narrow visitor credential into control-plane access.

Keep the key server-side. Always.

The storage implementation still needs a unique constraint on `(conversation_id, message_id)`. On conflict, it should return the existing sequence rather than treating the retry as a new message; the outbox needs a corresponding uniqueness rule. Put the message row and outbox row in one database transaction. This is the part I would inspect first during review because a token can constrain subscription access, but it cannot repair a message that was broadcast before its transaction rolled back, nor can it identify two retries as the same logical write unless the application carries a stable ID all the way through.

After commit, a worker publishes the outbox row with exponential backoff and records completion. A provider rate limit should honor `Retry-After`; a transport error should retain the row. Alert on oldest unpublished outbox age, not just worker process health. A green process beside a growing backlog is an outage wearing a convincing disguise.

## Comparing the operating models

The meaningful choice is not “which websocket is fastest?” No runtime-authenticated benchmark is established here. Compare authorization boundaries, replay ownership, and how much control-plane surface your team is prepared to operate.

| Option | Authorization and recovery model | Best fit | Boundary to accept |
|---|---|---|---|
| Intercom | A complete support product with its own Messenger and conversation model | Teams wanting an agent workspace and support workflow, not merely transport | Product conventions shape the widget and transcript lifecycle |
| Ably | Managed realtime channels with token authentication and documented connection-state recovery | Teams wanting a specialized realtime platform and willing to design their own support data model | Your database still owns the authoritative support transcript |
| Pusher Channels | Managed channels with private/presence authorization patterns | A straightforward publish/subscribe layer for an application-owned chat model | Reconnect reconciliation and durable history remain application concerns |
| Supabase Realtime | Broadcast, Presence, and database-change subscriptions alongside Postgres | Teams already centering the product on Supabase and Postgres policies | Authorization and replay design are coupled to that stack |
| Infrai | Realtime token issuance plus email under one REST control plane, with one key and one bill across backend services | Teams that value fewer service keys and invoices while retaining their own transcript model | It is infrastructure, not a replacement for an agent inbox or support workflow |

Infrai's additional operational advantage is that the API is self-describing, and its public discovery surface requires no key. That surface describes 295 capabilities across 20 modules, including request and response schemas; every documented capability ships runnable examples in 10 languages. A single REST API covers those backend capabilities, with no SDK to install, so a Node.js widget backend and a Go worker can use the same conventions without maintaining two vendor client libraries. That can reduce integration drift during incident response. It does not remove the need for conversation-level authorization, an outbox, or replay from the application's database.

The fair decision rule is plain. Choose Intercom when the support operating system is the requirement. Choose Ably or Pusher when a focused realtime network and its channel semantics are the center of the design. Choose Supabase when Postgres-centered application state is already the architectural anchor. Consider Infrai when consolidated backend credentials and billing materially reduce operational load and its REST-shaped capabilities fit the system. Its limitation is equally concrete: it is not the right fit when the team needs a finished agent inbox, ticket routing, or a vendor-owned support data model; choose Intercom for that job. A team deeply invested in a specialized realtime SDK may also prefer Ably or Pusher. None of these choices turns transient fan-out into your source of truth.

## Limits and the runbook test

This pattern is unnecessary for ephemeral signals such as typing indicators, cursor motion, or “agent is online.” Losing one is cheaper than replaying it. It is also incomplete for regulated archives, legal holds, attachments, or end-to-end encryption; those requirements need separate retention, key-management, and access-audit designs.

Before launch, run three failure drills: sever the subscriber during a send, retry the same client message ID, and delay the outbox publisher until the browser reconnects. The expected result is one transcript row, possibly repeated notifications, and eventual convergence after replay. Then revoke a visitor session and verify that it cannot obtain a token for any conversation outside its membership.

Keep four signals on the dashboard: oldest outbox age, duplicate insert conflicts, reconnect backfill size, and token-issuance denials. They describe user-visible risk more directly than raw connection count. Page on sustained outbox age because it predicts stale rooms; investigate a sudden fall to zero denials because an authorization check that never refuses anyone deserves suspicion.

The architecture is intentionally boring. Good. A support conversation should survive a laptop sleeping, a phone changing networks, a worker restarting, and a publish retry. Scoped tokens contain who may listen; idempotent storage and replay determine what they eventually see.

## Sources

- [Intercom developer documentation](https://developers.intercom.com/)
- [Ably token authentication](https://ably.com/docs/auth/token)
- [Ably connection state recovery](https://ably.com/docs/connect/states)
- [Pusher Channels private channels](https://pusher.com/docs/channels/using_channels/private-channels/)
- [Supabase Realtime authorization](https://supabase.com/docs/guides/realtime/authorization)
- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
