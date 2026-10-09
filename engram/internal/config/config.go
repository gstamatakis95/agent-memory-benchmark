// Package config is the layered configuration of PLAN.md section 2.2.22 (namespace over tenant over system). M0.1
// declares the signatures only; the allow-list generator (N66) and the resolver arrive in M0.8 and M1.6.
package config

// Layer is one configuration layer as set by an operator, a tenant or a namespace: allow-listed key to value. Unknown
// keys are errs.Validation when a layer is resolved.
type Layer struct {
	Values map[string]any
}

// Key is one entry of the allow-list (N66): the resolver, the Namespace.config_overrides proto comment and the docs are
// generated from it.
type Key struct {
	Name    string // dotted path, e.g. "consolidation.max_rebuilds_per_round"
	Type    string // bool | int | duration | string | enum
	Default any
	Doc     string
}

// Keys is the single allow-list (N66).
var Keys []Key

// Models are the model classes of D15. Embed and EmbedDims are NOT here: they are fixed at namespace creation and read
// from the catalog entry (N111).
type Models struct {
	Extract     string
	Consolidate string
	Reflect     string
	Rerank      string
}

// Prompts pins the prompt version per prompt id, e.g. "extract" to "extract/v1" (section 6).
type Prompts struct {
	Versions map[string]string
}

// Chunk is the chunker configuration (2.2.9).
type Chunk struct {
	TargetChars, MinChars, MaxChars int
}

// Retain is the retain configuration; Mission steers extraction.
type Retain struct {
	Mission string
}

// Recall is the recall configuration (arm caps by budget, rerank depth).
type Recall struct {
	RerankTopByBudget map[string]int
}

// Quota carries the per-namespace limits of D13.
type Quota struct {
	RecallsPerMin, RetainsPerMin int64
	LLMTokensPerDay, MaxFacts    int64
}

// Consolidation is the consolidation configuration (2.2.14, section 2.5).
type Consolidation struct {
	Enabled                 bool
	Debounce                int64 // nanoseconds, a time.Duration in the resolver
	Mission                 string
	ObservationScope        string
	MaxObservationsPerScope int
	MaxRebuildsPerRound     int
}

// Reflect is the Reflect configuration.
type Reflect struct {
	KeepTranscripts bool
}

// Resolved is the effective configuration of a namespace. Resolution happens once per catalog entry and is cached with
// it; Version (a hash of the resolved values) travels in activity inputs so that a mid-operation change is visible in
// traces without changing an in-flight activity's model.
type Resolved struct {
	Models        Models
	Prompts       Prompts
	Chunk         Chunk
	Retain        Retain
	Recall        Recall
	Quota         Quota
	Consolidation Consolidation
	Reflect       Reflect
	Version       string
}

// Resolver merges the three layers: namespace over tenant over system; unknown keys are errs.Validation.
type Resolver interface {
	Resolve(system, tenant, namespace Layer) (*Resolved, error)
}
