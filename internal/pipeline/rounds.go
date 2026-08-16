package pipeline

import (
	"fmt"
	"strings"
	"time"
)

// Turn is one dialog turn, using the verified benchmark field names of
// docs/01-retrieval.md section 2: LoCoMo turns carry {speaker, dia_id, text}
// (two named human speakers — never assume an assistant role); LongMemEval
// turns carry {role, content} with a synthesized turn id.
type Turn struct {
	SessionID string     // "session_1" (LoCoMo) / haystack session id (LME)
	TurnID    string     // LoCoMo dia_id, or synthesized turn index for LME
	Speaker   string     // speaker_a/b (LoCoMo) or user/assistant (LME)
	Text      string     // raw dialog text
	TS        *time.Time // session timestamp, already UTC-normalized
}

// Round is the indexed retrieval unit (LongMemEval Finding 1: round is the
// best granularity). It groups a user turn with its reply while retaining
// the member turns, because LoCoMo evidence is annotated at turn level and
// turn rows must be kept alongside round rows.
type Round struct {
	RoundID   string
	SessionID string
	Turns     []Turn
	TS        *time.Time // timestamp of the first turn in the round
}

// TurnIDs returns the member turn ids (LoCoMo dia_ids) in order.
func (r Round) TurnIDs() []string {
	ids := make([]string, len(r.Turns))
	for i, t := range r.Turns {
		ids[i] = t.TurnID
	}
	return ids
}

// Text renders the round as one "speaker: text" line per turn — the raw
// content that downstream steps normalize, tokenize and embed.
func (r Round) Text() string {
	lines := make([]string, len(r.Turns))
	for i, t := range r.Turns {
		lines[i] = t.Speaker + ": " + t.Text
	}
	return strings.Join(lines, "\n")
}

// AssembleRounds groups consecutive turn pairs into rounds
// (docs/01-retrieval.md section 4.6 step 4). A round closes when it already
// holds two turns, when the session changes, or when the incoming turn is by
// the same speaker as the round's opening turn (two consecutive turns by one
// speaker are separate rounds — this handles both LongMemEval's
// user/assistant alternation and LoCoMo's two-human speaker_a/speaker_b
// dialogs without assuming roles). The input turns are referenced, not
// consumed: callers keep the individual turn rows for turn-level evidence.
func AssembleRounds(turns []Turn) []Round {
	var rounds []Round
	for _, t := range turns {
		if n := len(rounds); n > 0 {
			last := &rounds[n-1]
			if len(last.Turns) < 2 &&
				last.SessionID == t.SessionID &&
				last.Turns[0].Speaker != t.Speaker {
				last.Turns = append(last.Turns, t)
				continue
			}
		}
		rounds = append(rounds, Round{
			RoundID:   fmt.Sprintf("%s:round:%d", t.SessionID, len(rounds)),
			SessionID: t.SessionID,
			Turns:     []Turn{t},
			TS:        t.TS,
		})
	}
	return rounds
}
