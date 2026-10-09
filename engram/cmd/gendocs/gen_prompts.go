package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/gstamatakis95/engram/internal/prompts"
)

// genPrompts renders the prompt registry and checks every pin: a released prompt version is never edited (PLAN.md 6.0),
// so a file whose hash differs from its HASH is a failure of make gen-docs as well as of the unit test.
func genPrompts() (string, []string, error) {
	ids, err := prompts.Names()
	if err != nil {
		return "", nil, err
	}
	d := NewDoc("Prompt registry", "`prompts/<name>/v<N>/`", "PLAN.md 6.0 to 6.7; register N87, N110, D11, D15")
	d.Para("Each prompt version is a directory of files; `HASH` pins their content (SHA-256 over every other file of " +
		"the directory, in name order). A change to a released version is a new version directory. The extraction " +
		"cache key uses the prompt id and `schema_version` together with the render hash of the variables " +
		"(`internal/prompts` `ExtractionKey`).")
	var problems []string
	var rows [][]string
	for _, id := range ids {
		name, ver, _ := strings.Cut(id, "/v")
		v, err := strconv.Atoi(ver)
		if err != nil {
			return "", nil, err
		}
		p, err := prompts.Get(name, v)
		if err != nil {
			return "", nil, err
		}
		switch {
		case p.Pin == "":
			addf(&problems, "prompts: %s has no HASH pin; run `go run ./cmd/gendocs pin %s`", id, id)
		case p.Pin != p.Hash.String():
			addf(&problems, "prompts: %s changed without a version bump (pinned %.12s, files hash to "+
				"%.12s)", id, p.Pin, p.Hash)
		}
		limit := fmt.Sprint(p.Meta.MaxOutputTokens)
		switch p.Meta.MaxOutput {
		case "request":
			limit = "request"
			if p.Meta.DefaultMaxOutputTokens > 0 {
				limit += fmt.Sprintf(" (%d)", p.Meta.DefaultMaxOutputTokens)
			}
		case "none":
			limit = "-"
		}
		rows = append(rows, []string{"`" + id + "`", p.Meta.ModelClass, p.Meta.Temperature, limit,
			fmt.Sprint(p.Meta.SchemaVersion), "`" + p.Hash.String()[:16] + "`"})
	}
	d.Table([]string{"Prompt", "Model class", "Temp.", "Max out", "Schema", "Hash (first 16)"}, rows)
	d.H2("Inputs")
	for _, id := range ids {
		name, ver, _ := strings.Cut(id, "/v")
		v, _ := strconv.Atoi(ver)
		p, _ := prompts.Get(name, v)
		untrusted := "none"
		if len(p.Defenses.DataInputs) > 0 {
			untrusted = strings.Join(p.Defenses.DataInputs, ", ")
		}
		d.Bullet("`" + id + "`: " + strings.Join(p.Inputs(), ", ") + ". Untrusted: " + untrusted + ".")
	}
	d.EndList()
	return d.String(), problems, nil
}
