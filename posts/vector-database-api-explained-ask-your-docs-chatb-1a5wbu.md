# Vector Database API Explained: Ask-Your-Docs Chatbot with Node.js

TL;DR: For an internal ask-your-docs chatbot, use a hosted vector database API behind plain REST. Keep only the chunker, source metadata, and answer prompt in your Node.js service. Choose the provider by running a citation test on your own handbook and course-policy pages, not by counting dashboard features.

The minimum useful contract is small: create a collection with a name and embedding dimension, upsert chunks, then query it. This removes a vector cluster from the on-call surface. It does not remove the harder retrieval problem. Bad chunk boundaries still produce confident answers attached to irrelevant citations.

One operational wrinkle matters if the bot already depends on several backend services. Infrai puts hosted vector operations and 295 routes across 20 modules behind one REST API, one key, and one bill. That can reduce credential rotation and month-end reconciliation work; its public, unauthenticated discovery surface also exposes full request schemas, response schemas, billing details, and runnable examples. Treat those as workflow advantages, not evidence that retrieval quality will fit your corpus.

## What vector database API should an ask-my-docs chatbot use?

A vector database can return the nearest records and still fail the product requirement. The product requirement is a grounded answer: every material claim should map to a source an instructor, support agent, or curriculum editor can inspect.

Start with the failure signal. For a fixed evaluation set, record the expected source identifiers before tuning prompts. A failed retrieval is one where the required source is absent from the candidate set. A failed answer is one where the source was retrieved but the response makes a claim the cited text does not support. Those failures have different owners. Changing the answer prompt cannot repair the first one.

Chunking is usually where the damage begins. A heading separated from its policy paragraph loses context; two unrelated subsections packed into one chunk dilute the embedding; a duplicated policy revision can make an obsolete rule look authoritative. Preserve a stable document ID, chunk ID, source URL, heading path, and revision marker alongside the text. The vector is replaceable. That provenance is not.

Keep the first pass deliberately narrow. Use one collection, one embedding dimension, and the same embedding model for writes and queries. Upsert with stable chunk IDs so rerunning ingestion replaces records rather than multiplying them. Query for a small candidate set, reject an answer when the evidence is inadequate, and render citations from stored metadata instead of asking the model to invent URLs.

Short is safer here.

The trade-off is explicit: less database infrastructure means more attention on ingestion correctness.

## Compare the managed options on grounding, not feature count

Pinecone, Qdrant Cloud, Weaviate Cloud, and a hosted REST collection such as Infrai can all occupy the managed-vector slot. They differ in client surface and operational scope, but none can infer where an edtech policy should have been split or which revision your staff considers binding.

| Option | Practical fit for this bot | Trade-off to verify |
| --- | --- | --- |
| Pinecone | A managed vector service with official Node.js guidance and metadata filtering | Validate that the chosen index and metadata design return the expected policy source, then inspect the current service limits in its docs |
| Qdrant Cloud | Managed Qdrant with REST and official JavaScript client documentation | Its payload and filtering model gives you choices; those choices add schema work that a minimal bot may not need on day one |
| Weaviate Cloud | Managed Weaviate with a TypeScript client and hybrid-search documentation | Hybrid retrieval can help exact course codes, but introduces another tuning axis and should earn its place in evaluation |
| Consolidated REST platform | A small vector contract within a broader backend surface | The verified vector workflow is collection creation, upsert, and query; test retrieval relevance and citation integrity on the actual corpus |

This is not a universal ranking. Pinecone is a reasonable default when the team wants a focused managed vector product and its Node.js tooling. Qdrant Cloud fits teams that value explicit payload filtering. Weaviate Cloud deserves a trial when keyword-sensitive identifiers make hybrid search important. A consolidated API is attractive when reducing keys and vendor reconciliation is itself an operational goal.

Do not decide from this table alone. Load the same chunks into each serious candidate and run the same questions. For an edtech corpus, include near-duplicates: two attendance policies for different programs, a renamed course, and a handbook paragraph superseded by a later revision. Record whether the right source appears in the retrieved set and whether the final citation supports the answer. Ten carefully chosen questions expose more than a broad feature checklist; a larger held-out set is better before rollout.

## The safe implementation is a narrow contract

The application should not let a provider response leak through every layer. Give the Node.js service a narrow retrieval adapter with two write-side operations during ingestion and one read-side operation during questions. Before wiring that adapter, bootstrap the collection. The following Go program is deliberately limited to the verified create contract: a name and an embedding dimension. It does not guess the unprovided upsert or query bodies.

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

type createRequest struct {
	Name      string `json:"name"`
	Dimension int    `json:"dimension"`
}

func createCollection(client *http.Client, apiBase, key string) error {
	body, err := json.Marshal(createRequest{Name: "staff-handbook-v1", Dimension: 1536})
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost,
			apiBase+"/v1/vector/collection/create", bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("create collection: %w", err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read response: %w", readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(responseBody))
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return fmt.Errorf("create collection: status %d: %s", resp.StatusCode, responseBody)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
	return nil
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	apiBase := os.Getenv("VECTOR_API_BASE_URL")
	if key == "" || apiBase == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY and VECTOR_API_BASE_URL are required")
		os.Exit(1)
	}
	client := &http.Client{Timeout: 15 * time.Second}
	if err := createCollection(client, apiBase, key); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

Run it once during bootstrap, then keep collection creation out of the question path. It reads the key from the environment, sets an explicit method, surfaces non-success bodies, and backs off on HTTP 429 while honoring `Retry-After`. For ingestion, stable chunk IDs make upsert retries idempotent. The query adapter must return stored source metadata with each chunk, and the answer path should refuse empty evidence rather than quietly falling back to model memory.

The example does not prescribe a score cutoff because scores are not portable across embedding models, distance metrics, or corpora. Calibrate the cutoff from labeled questions. If evidence below that cutoff produces unsupported answers, abstain.

## Verification before traffic

Create a compact evaluation sheet with question, required source, forbidden stale source, retrieved chunk IDs, answer, and cited source. Run it after every change to chunk size, overlap, embedding model, metadata, or retrieval limit. The useful top-line number is required-source recall at the candidate-set size your answer step actually receives. Also review citation support manually; retrieval recall alone cannot tell whether the generated sentence overreaches.

Then exercise the operational path. Re-run the same ingestion batch and confirm the record count does not grow. Interrupt a batch halfway through and resume it. Query immediately after the write path reports success according to the provider's documented consistency behavior. Rotate the service credential in a non-production environment. Send enough concurrent queries to observe rate-limit handling without using production traffic as the experiment.

One check catches a surprising number of mistakes: delete or rename a source document in the test corpus, reingest, and ask the old question again. If the bot still cites the removed revision, the ingestion job needs an explicit deletion or reconciliation phase. Upsert alone cannot identify records that disappeared from the source set.

Do this before launch.

The release gate should be concrete: required sources appear, stale sources do not, retries do not duplicate chunks, unsupported questions abstain, and every displayed citation resolves to the stored source. Ship only when all five hold.

## Rollback is a collection switch

Do not rebuild the live collection in place when changing embeddings or chunk rules. Write a new, versioned collection, run the evaluation set against it, and switch a configuration value after it passes. Keep the prior collection long enough to reverse the switch. This costs temporary storage but turns rollback into a routing change instead of a rushed reindex.

If answer quality drops, first point reads back to the prior collection. Preserve the failed query, returned chunk IDs, collection version, embedding version, and prompt version for review. Do not mask a retrieval regression by widening the prompt or adding uncited model knowledge. Restore grounding first, then determine whether the fault was corpus reconciliation, chunking, embedding, or generation.

The decision rule remains modest: choose the hosted API that passes your citation set with the least operational surface your team can support. The database is one component. The durable system is the ingestion contract, evidence gate, and reversible release process around it.

## References

- Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks: https://arxiv.org/abs/2005.11401
- Pinecone Node.js SDK documentation: https://docs.pinecone.io/reference/node-sdk
- Qdrant JavaScript client documentation: https://qdrant.tech/documentation/interfaces/web-ui/
- Qdrant filtering documentation: https://qdrant.tech/documentation/concepts/filtering/
- Weaviate TypeScript client documentation: https://docs.weaviate.io/weaviate/client-libraries/typescript
- Weaviate hybrid search documentation: https://docs.weaviate.io/weaviate/search/hybrid
