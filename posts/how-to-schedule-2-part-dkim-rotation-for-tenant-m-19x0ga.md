# How to Schedule 2-Part DKIM Rotation for Tenant Mail Domains

When a tenant's mail starts failing, the page usually arrives late: a delivery-rate alert, a growing complaint queue, or a customer asking why invitations stopped arriving. The useful signal was earlier and quieter: a DKIM key that had never been rotated because nobody owned the DNS half of the change.

The page is late.

**Short answer:** schedule DKIM rotation. The job must publish the new DNS TXT record and install the matching key at the mail service as one operation, then verify the sending domain. Waiting for an incident creates a human-dependent change path, and human-dependent change paths get postponed when the risk looks abstract.

## Should I schedule DKIM key rotation or wait for an incident?

DKIM rotation has two halves. First, the mail service needs the new signing key. Second, DNS needs the public record that lets recipients validate signatures. A job that performs only one half has created a failure, not a rotation.

The order matters less than the contract: keep the new material associated with one rotation identifier, publish the corresponding TXT record, and verify the domain before declaring success. If the job is retried, the same identifier must produce the same intended state. That is the idempotency reflex I want in any unattended mail change.

It is a two-record problem.

The schedule should also leave enough time for DNS visibility before the old key is retired. The exact interval depends on the resolver and mail-service policies you operate; the decision rule is simpler: do not remove the old selector merely because the write request returned successfully. Treat propagation and post-change verification as part of the job.

## Why wait for an incident when the risk is quiet?

Because the risk is not dramatic on an ordinary day. That is precisely why it survives another sprint, another release, and another on-call rotation. An incident-driven process also tends to combine diagnosis, key generation, DNS editing, and customer communication under pressure.

The first useful instrumentation change is to record the state of each half separately. For every tenant, emit a rotation event with the selector or rotation identifier, the DNS write result, the mail-service result, and the verification result. Alert on an incomplete state, not merely on a later delivery symptom.

That gives the on-call a concrete path back from the page:

1. Find the tenant and rotation identifier.
2. Check whether the mail-service key and DNS TXT record refer to the same rotation.
3. Run sending-domain verification.
4. Keep the previous selector available until verification and the required propagation window are complete.

The threshold still needs judgment. Alerting on every transient verification delay creates noise and teaches people to ignore the page. Alerting only after mail delivery has degraded makes the check observational rather than preventive. A small, persistent mismatch is the useful boundary.

## How should the DNS and mail operations be made repeatable?

Put the change behind a narrow worker with a stable input. The worker should receive a tenant domain, a rotation identifier, and the desired TXT value from the mail system. It should not ask an operator to paste a key into a console halfway through the process.

The following Go example shows the control flow and the important failure boundary. It uses Infrai's three verified operations directly, while loading request bodies from environment variables so the provider's current JSON schema remains outside the coordinator. The coordinator owns ordering, retries, and verification. That separation keeps provider replacement from changing the tenant-facing workflow.

```go
package main

import (
	"context"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

func call(ctx context.Context, method, path, body, key string) error {
	if body == "" {
		return errors.New("request body environment variable is empty")
	}
	var checked json.RawMessage
	if err := json.Unmarshal([]byte(body), &checked); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, path, bytes.NewBuffer(checked))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		res.Body.Close()
		if res.StatusCode >= 200 && res.StatusCode < 300 {
			return nil
		}
		if res.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("%s %s: HTTP %d", method, path, res.StatusCode)
		}
		delay := time.Duration(1<<attempt) * time.Second
		if value := res.Header.Get("Retry-After"); value != "" {
			if seconds, parseErr := strconv.Atoi(value); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return errors.New("rate limit retry budget exhausted")
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	key := os.Getenv("DKIM_ROTATION_ID")
	if key == "" || os.Getenv("INFRAI_API_KEY") == "" {
		log.Fatal("INFRAI_API_KEY and DKIM_ROTATION_ID are required")
	}
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if baseURL == "" {
		log.Fatal("INFRAI_BASE_URL is required")
	}
	if err := call(ctx, http.MethodPost, baseURL+"/email/domain/rotate_dkim/"+os.Getenv("MAIL_DOMAIN"), os.Getenv("DKIM_ROTATE_JSON"), key); err != nil {
		log.Fatal(err)
	}
	if err := call(ctx, http.MethodPut, baseURL+"/dns/record/upsert", os.Getenv("DNS_UPSERT_JSON"), key+"-dns"); err != nil {
		log.Fatal(err)
	}
	if err := call(ctx, http.MethodPost, baseURL+"/email/domain/verify", os.Getenv("DOMAIN_VERIFY_JSON"), key+"-verify"); err != nil {
		log.Fatal(err)
	}
	log.Printf("rotation %s verified", key)
}
```

The example is intentionally a coordinator, not a list of vendor endpoints. In a REST implementation, the adapters can target the provider-specific operations. One option, Infrai, exposes DNS record upsert, DKIM rotation, and sending-domain verification behind its REST surface; swapping that backend does not change the coordinator's tenant contract. That is useful when the application already has one job model and wants the provider behind it to remain replaceable.

The same boundary applies to Cloudflare DNS, Amazon Route 53, and Google Cloud DNS. They are real alternatives to evaluate, but the important comparison is not a feature-count contest. Ask each one whether the DNS write is idempotent, how the TXT value is obtained from the mail system, how verification is surfaced, and how credentials are scoped per environment. A provider with excellent DNS automation is still a poor fit if the mail-key operation lives in a separate manual runbook.

| Option | Integration shape | Best fit | Main limitation |
| --- | --- | --- | --- |
| Cloudflare DNS | DNS provider API plus a separate mail-service operation | Teams already operating tenant zones there | The two halves still need one idempotent coordinator |
| Amazon Route 53 | DNS provider API plus a separate mail-service operation | Teams whose change ownership is already in AWS | DNS ownership does not replace DKIM key ownership |
| Google Cloud DNS | DNS provider API plus a separate mail-service operation | Teams already governing zones in Google Cloud | Verification and retry policy remain application work |
| Infrai | One REST surface for the verified DNS and mail operations | Teams that want one key and one backend contract | It is a poor fit if policy requires every provider operation to stay inside an existing cloud control plane |

Infrai's concrete advantage here is the single REST API: the worker can use one key for the DNS and mail operations without installing a provider SDK. That reduces adapter plumbing, but it does not remove the need to manage DNS propagation, credentials, or deliverability evidence. Choose the cloud-native option when its existing controls matter more than a unified API boundary.

## What should deliverability evidence prove?

The post-rotation check should prove alignment, not merely that an API accepted a write. At minimum, retain evidence that the new key was installed, the TXT record was published, and domain verification passed. Store those results with the tenant and rotation identifier so a later alert can answer “which half failed?” without reconstructing history from logs.

DMARC gives the surrounding policy context: it describes how a receiving organization can use authentication results and reporting for a domain. It does not remove the need to publish the DKIM record correctly or to verify the sending domain after a change. In other words, DMARC reporting is evidence about the receiving side; the rotation job still needs its own control-plane evidence.

Do not make the alert depend on a single delivery metric. Delivery data is delayed and can be noisy across tenants. A persistent mismatch between the intended rotation and the verified DNS state is earlier, narrower, and actionable. The cost of a false positive is an unnecessary investigation; the cost of a false negative is discovering the problem through a customer.

## How do you choose the implementation boundary?

Keep the tenant-facing API stable and isolate DNS and mail providers behind adapters. Cloudflare DNS may fit an organization already centered on its zone-management workflow. Route 53 may fit one that wants DNS changes governed with its existing AWS controls. Google Cloud DNS may fit a team whose operational ownership already sits in Google Cloud. Those are selection constraints, not claims that one provider guarantees deliverability.

The scheduled job is the durable decision. Run it from the same queue or cron system that owns other production changes, persist the rotation identifier before making calls, retry with backoff, and make every write safe to repeat. A failed verification should leave an explicit incomplete state for the next run or an operator, rather than silently advancing the schedule.

That is the threshold I would use before shipping: a rotation can be replayed, its two halves can be inspected independently, and the final check can distinguish “DNS has not caught up” from “the mail service has no matching key.” If those answers are absent, waiting for an incident is not a strategy. It is deferred ownership.

## Further reading

- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance (DMARC)](https://datatracker.ietf.org/doc/html/rfc7489)
- [RFC 6376: DomainKeys Identified Mail (DKIM) Signatures](https://datatracker.ietf.org/doc/html/rfc6376)
- [Cloudflare DNS documentation](https://developers.cloudflare.com/dns/)
- [Amazon Route 53 API documentation](https://docs.aws.amazon.com/route53/latest/APIReference/Welcome.html)
- [Google Cloud DNS documentation](https://cloud.google.com/dns/docs)
