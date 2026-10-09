// Package gateway is the only HTTP client to the AI gateway (PLAN.md section 2.2.5; register D10, D15, N130). Pattern:
// Strategy by capability (interface segregation): a consumer depends on the one capability it uses, so an extractor
// test fakes Structured and nothing else. M0.1 declares the signatures only.
package gateway

import (
	"context"
	"iter"
)

// Model names a gateway model.
type Model string

// Usage is the metering record of one gateway call.
type Usage struct {
	Model                          Model
	PromptTokens, CompletionTokens int
	CostMicros                     int64
}

// UsageHook receives the usage of every call; hooks feed quota.Meter and telemetry.
type UsageHook func(ctx context.Context, u Usage)

// RateLimiter is the per-model limiter: Wait blocks (bounded by ctx) rather than fails, so that a burst is smoothed
// instead of rejected (429 handling is the client's retry loop).
type RateLimiter interface {
	Wait(ctx context.Context, m Model, tokens int) error
}

// Structured is JSON-decoded structured output; a 4xx is a PermanentLLMError.
type Structured interface {
	ChatStructured(ctx context.Context, r StructuredRequest, out any) (*Usage, error)
}

// Chatter streams tokens and tool calls (Reflect).
type Chatter interface {
	Chat(ctx context.Context, r ChatRequest, sink ChatSink) (*Usage, error)
}

// Embedder embeds one text per request or a batch of at most 64. Vectors are L2-normalised by the client; the caller
// adds the nomic prefix (`search_document: ` / `search_query: `, D15).
type Embedder interface {
	Embed(ctx context.Context, r EmbedRequest) ([]float32, *Usage, error)
	EmbedBatch(ctx context.Context, r EmbedBatchRequest) ([][]float32, *Usage, error) // at most 64 texts
}

// Reranker scores at most 300 documents per call (D15).
type Reranker interface {
	Rerank(ctx context.Context, r RerankRequest) ([]RerankScore, *Usage, error)
}

// Batcher is the gateway batch API.
type Batcher interface {
	SubmitBatch(ctx context.Context, r BatchJobRequest) (*BatchJob, error)
	PollBatch(ctx context.Context, job string) (*BatchJobStatus, error)
	BatchResults(ctx context.Context, job string) iter.Seq2[BatchResult, error]
}

// ChatSink receives a streamed chat: token deltas and tool calls.
type ChatSink interface {
	Delta(ctx context.Context, text string) error
	ToolCall(ctx context.Context, c ToolCall) error
}

// StructuredRequest asks for output matching Schema.
type StructuredRequest struct {
	Model    Model
	Prompt   string // the versioned prompt id, e.g. "extract/v1"
	System   string
	User     string
	Schema   []byte // JSON schema
	MaxToken int
}

// ChatRequest is a streamed chat with tools.
type ChatRequest struct {
	Model    Model
	Messages []Message
	Tools    []ToolSpec
	MaxToken int
}

// Message is one chat message.
type Message struct {
	Role    string // system | user | assistant | tool
	Content string
	Call    *ToolCall
}

// ToolSpec describes a tool the model may call.
type ToolSpec struct {
	Name        string
	Description string
	Schema      []byte
}

// ToolCall is a tool invocation requested by the model.
type ToolCall struct {
	ID   string
	Name string
	Args []byte
}

// EmbedRequest is one text; Text already carries the nomic prefix.
type EmbedRequest struct {
	Model Model
	Text  string
}

// EmbedBatchRequest is at most 64 prefixed texts.
type EmbedBatchRequest struct {
	Model Model
	Texts []string
}

// RerankRequest is a query and at most 300 documents.
type RerankRequest struct {
	Model Model
	Query string
	Docs  []string
}

// RerankScore is the score of one document, by its index in the request.
type RerankScore struct {
	Index int
	Score float64
}

// BatchJobRequest, BatchJob, BatchJobStatus and BatchResult are the batch API values.
type (
	BatchJobRequest struct {
		Model    Model
		Requests []StructuredRequest
	}
	BatchJob struct {
		ID string
	}
	BatchJobStatus struct {
		ID    string
		State string // submitted | running | done | failed
		Done  int
		Total int
	}
	BatchResult struct {
		Index int
		Raw   []byte
		Usage Usage
	}
)

// HTTPClient implements all five capabilities. Retries 429/502/503/504 with jittered backoff (100 ms to 5 s, at most 5
// attempts) and then returns errs.Unavailable; other 4xx are errs.PermanentLLM. A per-model rate limiter blocks
// (bounded by ctx) rather than fails. Admission (N130): no gateway call is made without a prior quota.Reserve; the
// client itself does not enforce it, the activities do.
type HTTPClient struct{}

// Options configure an HTTPClient.
type Options struct {
	BaseURL string
	Hooks   []UsageHook
}

// NewHTTPClient builds the client.
func NewHTTPClient(o Options) *HTTPClient { panic("stub") }

// ChatStructured implements Structured.
func (c *HTTPClient) ChatStructured(ctx context.Context, r StructuredRequest, out any) (*Usage, error) {
	panic("stub")
}

// Chat implements Chatter.
func (c *HTTPClient) Chat(ctx context.Context, r ChatRequest, sink ChatSink) (*Usage, error) {
	panic("stub")
}

// Embed implements Embedder.
func (c *HTTPClient) Embed(ctx context.Context, r EmbedRequest) ([]float32, *Usage, error) {
	panic("stub")
}

// EmbedBatch implements Embedder.
func (c *HTTPClient) EmbedBatch(ctx context.Context, r EmbedBatchRequest) ([][]float32, *Usage, error) {
	panic("stub")
}

// Rerank implements Reranker.
func (c *HTTPClient) Rerank(ctx context.Context, r RerankRequest) ([]RerankScore, *Usage, error) {
	panic("stub")
}

// SubmitBatch implements Batcher.
func (c *HTTPClient) SubmitBatch(ctx context.Context, r BatchJobRequest) (*BatchJob, error) {
	panic("stub")
}

// PollBatch implements Batcher.
func (c *HTTPClient) PollBatch(ctx context.Context, job string) (*BatchJobStatus, error) {
	panic("stub")
}

// BatchResults implements Batcher.
func (c *HTTPClient) BatchResults(ctx context.Context, job string) iter.Seq2[BatchResult, error] {
	panic("stub")
}

var (
	_ Structured = (*HTTPClient)(nil)
	_ Chatter    = (*HTTPClient)(nil)
	_ Embedder   = (*HTTPClient)(nil)
	_ Reranker   = (*HTTPClient)(nil)
	_ Batcher    = (*HTTPClient)(nil)
)
