package pipeline

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	"github.com/olebedev/when"
)

// Date extraction is strictly rule-based (no LLMs): absolute timestamps go
// through explicit layouts + dateparse.ParseStrict, relative expressions
// ("last Tuesday") through olebedev/when. Everything is normalized to UTC
// (docs/01-retrieval.md section 4.6 step 3).

// locomoLayouts covers the LoCoMo free-form session timestamps, e.g.
// "1:56 pm on 8 May, 2023" (docs/06-testing.md Tier 0). time.Parse matches
// month names case-insensitively, but am/pm tokens are case-sensitive, so
// both variants are listed.
var locomoLayouts = []string{
	"3:04 pm on 2 January, 2006",
	"3:04 PM on 2 January, 2006",
	"3:04 pm on 2 January 2006",
	"3:04 PM on 2 January 2006",
	"15:04 on 2 January, 2006",
	"15:04 on 2 January 2006",
	"2 January, 2006",
}

// weekdayParen strips the parenthesized weekday LongMemEval embeds in
// haystack_dates, e.g. "2023/05/20 (Sat) 02:21" -> "2023/05/20  02:21".
var weekdayParen = regexp.MustCompile(`(?i)\(\s*(?:mon|tue|wed|thu|fri|sat|sun)[a-z]*\s*\)`)

// ParseTimestamp parses an absolute timestamp string — LoCoMo
// session_N_date_time free-form style or a LongMemEval haystack_dates entry —
// and returns it normalized to UTC. Ambiguous forms such as "03/04/2023"
// (mm/dd vs dd/mm) are an error, never a guess: after the explicit layouts,
// parsing falls through to dateparse.ParseStrict, which rejects them.
func ParseTimestamp(s string) (time.Time, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return time.Time{}, fmt.Errorf("pipeline: empty timestamp")
	}
	for _, layout := range locomoLayouts {
		if t, err := time.Parse(layout, trimmed); err == nil {
			return t.UTC(), nil
		}
	}
	cleaned := weekdayParen.ReplaceAllString(trimmed, " ")
	cleaned = strings.Join(strings.Fields(cleaned), " ")
	t, err := dateparse.ParseStrict(cleaned)
	if err != nil {
		return time.Time{}, fmt.Errorf("pipeline: unparseable or ambiguous timestamp %q: %w", s, err)
	}
	return t.UTC(), nil
}

// relParser is the shared rule set for relative-date expressions. when.EN is
// the library's pre-built English parser (common + en rules).
var relParser *when.Parser = when.EN

// ParseRelative resolves a relative date expression ("last Tuesday", "two
// weeks ago") against a base time using olebedev/when. The second return is
// false when the text contains no recognizable expression. The result is
// normalized to UTC.
func ParseRelative(s string, base time.Time) (time.Time, bool, error) {
	r, err := relParser.Parse(s, base)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("pipeline: relative parse %q: %w", s, err)
	}
	if r == nil {
		return time.Time{}, false, nil
	}
	return r.Time.UTC(), true, nil
}

// ExtractQueryTime finds a temporal anchor in a benchmark question: absolute
// timestamps win; otherwise a relative expression resolved against base
// (e.g. LongMemEval question_date). Returns found=false when the question
// carries no parseable temporal signal — callers must then skip temporal
// boosting rather than guess.
func ExtractQueryTime(question string, base time.Time) (time.Time, bool) {
	if t, err := ParseTimestamp(question); err == nil {
		return t, true
	}
	if t, ok, err := ParseRelative(question, base); err == nil && ok {
		return t, true
	}
	return time.Time{}, false
}
