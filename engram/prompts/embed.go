// Package prompts embeds the released prompt files of Engram (PLAN.md section 6, registry in 6.0): one directory per
// prompt version, prompts/<name>/v<N>/, holding the template, the JSON schema of the structured output, the injection
// defenses of section 6.7 as data, the snippets the typed input builders use, a VERSION file and the HASH pin. The
// Go side that parses and renders them is internal/prompts; this package exists only because go:embed cannot reach
// outside the directory of the package that embeds, and the prompt files are a top-level, reviewable tree.
package prompts

import "embed"

// FS holds every prompts/<name>/v<N>/ directory (testdata/ and this file are not part of it).
//
//go:embed */v[0-9]*
var FS embed.FS
