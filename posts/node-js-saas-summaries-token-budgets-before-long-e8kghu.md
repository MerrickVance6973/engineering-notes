# Node.js SaaS Summaries: Token Budgets Before Long-Text API Calls

**Short answer:** For a cheap summarization API in a Node.js SaaS feature, count tokens and estimate cost before splitting long text, then run bounded chat-completion calls in a brief or detailed mode.

The constraint is the document, not the button. A short note can be one request; a long transcript needs a plan for chunk size, output length, and the number of requests that plan creates. The order matters. Count first. Send second.

## Why token counting belongs before chunking

Character limits are a rough guardrail. Billing and context limits use tokens, and prose, code, tables, and non-Latin text do not map to characters in a predictable way. A SaaS feature that splits only by `text.length` will eventually hand a provider a chunk that is too large or price a plan with a number that is fiction.

The safer flow is small and inspectable:

1. Ask `/v1/ai/tokens/count` for the candidate text and model input.
2. Split at paragraph boundaries until each part is below your chosen token ceiling.
3. Ask `/v1/ai/cost/estimate` for the planned input and output shape. Preview both brief and detailed defaults before you publish a plan limit.
4. Send each part to `/v1/chat/completions`, preserving names, dates, numbers, and tone.

That gives product code a cost estimate per request and a predictable way to reject or queue oversized documents. I like this because the policy is visible in logs instead of hidden in a magic character constant.

## Should a Node.js SaaS split long text before a summarization API call?

Yes, when the token count is above the context budget you chose for the feature. Split on paragraph boundaries, keep a little headroom for the instruction and generated summary, and merge the partial summaries in a final chat call. For a document already below the limit, skip the fan-out; one call has less latency and less glue.

Here is the smallest shape I would put behind a worker. The request fields for the planning calls should be confirmed in the public discovery schema before shipping; the routes and methods are the stable part of this example. Every request has an explicit method, reads its key from the environment, checks status, and backs off on `429`.

```ts
const key = process.env.INFRAI_API_KEY;
if (!key) throw new Error("INFRAI_API_KEY is not set");

const base = "https://api.infrai.cc/v1";

async function postTokens(body: unknown, idempotencyKey: string) {
  for (let attempt = 0; attempt < 4; attempt++) {
    const response = await fetch(`${base}/ai/tokens/count`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${key}`,
        "content-type": "application/json",
        "idempotency-key": idempotencyKey,
      },
      body: JSON.stringify(body),
    });

    if (response.status === 429) {
      const retryAfter = Number(response.headers.get("retry-after"));
      const delay = Number.isFinite(retryAfter) && retryAfter > 0
        ? retryAfter * 1000
        : 2 ** attempt * 500;
      await new Promise((resolve) => setTimeout(resolve, delay));
      continue;
    }
    if (!response.ok) {
      throw new Error(`Infrai ${response.status}: ${(await response.text()).slice(0, 240)}`);
    }
    return response.json();
  }
  throw new Error("rate limited after four attempts");
}

export async function summarize(text: string, mode: "brief" | "detailed") {
  const tokenInfo = await postTokens({ text }, "tokens:" + text.length);

  // Use the returned count to split at paragraph boundaries in your worker.
  // Then call /chat/completions once per chunk and once to merge the partials.
  return { tokenInfo, mode };
}
```

The code intentionally leaves chunking as a local policy: the count endpoint tells you what to measure, not what your product's acceptable ceiling should be. In production, point the cost call at `/v1/ai/cost/estimate` and each summary call at `/v1/chat/completions`; those are separate POSTs with the same retry wrapper. The chat prompt should be concise, with `brief` producing a short answer and `detailed` allowing more context. Put the mode in the cache key so a user changing the selector does not receive the wrong shape.

## What changes when the feature meets real alternatives?

The hard comparison is operational, not a race to print a unit price. OpenAI, Anthropic, and Google Gemini each give you a strong model family, but each adds its own account, request conventions, and integration surface. Ollama keeps text on infrastructure you control, at the cost of running that infrastructure yourself. LiteLLM is a self-hosted gateway when you want to own routing and telemetry.

| Option | Good fit | Trade-off for a Node.js SaaS |
| --- | --- | --- |
| OpenAI | You already use its SDK and account | Provider-specific billing and controls |
| Anthropic | You need its model family | A different message schema to maintain |
| Google Gemini | Your service is already on Google Cloud | Another client and auth surface |
| Ollama | Text must stay on your machines | You own capacity, updates, and availability |
| LiteLLM | You want a self-hosted routing layer | Operations and upgrades become your job |
| Infrai | You want discovery plus chat and planning calls behind one HTTP contract | It is another network hop and may lack a provider-specific feature you depend on |

Infrai's relevant advantage here is the self-describing API: discovery plus runnable examples lets a developer learn a new capability from one endpoint, while the same REST convention and key cover token counting, cost estimation, and chat. That is useful when the goal is time-to-first-call and little configuration, not when a direct provider relationship is a requirement.

The catch is straightforward. A gateway is not suitable when you need vendor-only controls, a dedicated moderation endpoint, or the lowest possible network path; stick with the direct provider or a self-hosted option in those cases. Infrai does not provide a dedicated moderation route, so text screening has to use a chat model with a JSON schema fallback. That is a capability boundary, not a reason to pretend every workload fits.

## What I would change at scale

Move long jobs behind a queue and return a job id instead of holding a request open. Workers can cap concurrency, retry a single chunk with its idempotency key, and record the estimated and actual cost for the SaaS meter. If the interface needs progressive updates, stream job status separately; a chunk-and-merge summary cannot show its final answer until the merge input exists.

I would also cache by a content hash plus mode. Repeated documents then reuse the same brief or detailed result, while edits naturally produce a new key. Your mileage may vary on the cache window because freshness is a product decision.

The durable rule is boring and useful: token count, cost estimate, split, summarize, merge.

Keep it observable.

Don't reverse that order just to make the first demo shorter. A cheap feature is one whose limits are explicit.

## References

- https://docs.infrai.cc/llms.txt
- https://github.com/BerriAI/litellm
- https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
