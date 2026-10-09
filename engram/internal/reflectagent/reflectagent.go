// Package reflectagent is the bounded agent loop of Reflect (PLAN.md section 2.2.15; register D12, N73, N130, N136,
// N139, N140, N157). It is not named `reflect`: the old name shadowed the standard library package (N140). Forced
// searches (search_pages first once pages exist, then observations, then facts), at most 10 free iterations, 100 k
// context tokens, 300 s, tool deadline 10 s; tools search_memories, search_observations, search_pages, get_page,
// expand_fact; citations filtered to ids a tool actually returned. Pattern: Strategy (Tool) in a bounded loop; the
// agent runs inside `engram-api` with the caller's scope, no second authz path. M0.1 declares the signatures only.
package reflectagent

import (
	"context"
	"encoding/json"
	"time"

	"github.com/gstamatakis95/engram/internal/gateway"
	"github.com/gstamatakis95/engram/internal/id"
)

// ToolResult is what a tool returns; Returned feeds the citation verifier (no authz import, N157).
type ToolResult struct {
	Content  json.RawMessage
	Returned []id.MemoryRef
}

// Tool is one capability of the agent.
type Tool interface {
	Name() string
	Schema() json.RawMessage
	Call(ctx context.Context, sc id.Scope, c id.Caller, args json.RawMessage) (ToolResult, error)
}

// Directive is a typed persona directive filtered by the call's tags.
type Directive struct {
	Text     string
	Priority int
	Active   bool
	Tags     []string
}

// Request is the Reflect call: the query and the persona (mission, directives, disposition).
type Request struct {
	Scope       id.Scope
	Caller      id.Caller
	Query       string
	Context     string
	Tags        []string
	Mission     string
	Directives  []Directive
	Disposition map[string]int
	Schema      json.RawMessage // optional JSON-schema output
}

// Caps are the bounds of one run: 10 iterations, 100 k context tokens, 300 s, tool deadline 10 s.
type Caps struct {
	MaxIterations int
	MaxTokens     int
	Wall          time.Duration
	ToolDeadline  time.Duration
}

// Event is one streamed element of a run: a token delta, a tool call or result, a citation or the answer.
type Event struct {
	Kind string // delta | tool_call | tool_result | citation | answer | stats
	Data json.RawMessage
}

// EventSink receives the events of a run in order.
type EventSink interface {
	Emit(ctx context.Context, e Event) error
}

// Result is the final answer with the citations that survived the verifier.
type Result struct {
	Answer     string
	Structured json.RawMessage
	Citations  []id.MemoryRef
	Iterations int
	Usage      gateway.Usage
}

// Agent runs Reflect.
type Agent interface {
	Run(ctx context.Context, req Request, caps Caps, sink EventSink) (*Result, error)
}

// CitationVerifier keeps the citations a tool actually returned.
type CitationVerifier interface {
	Verify(cited []id.MemoryRef, returned map[id.MemoryRef]struct{}) (kept, dropped []id.MemoryRef)
}

// SchemaValidator validates the optional structured output.
type SchemaValidator interface {
	Validate(schema, doc json.RawMessage) error
}

// Registry holds the tools of a run.
type Registry interface {
	Register(t Tool)
	Get(name string) (Tool, bool)
	Specs() []gateway.ToolSpec
}
