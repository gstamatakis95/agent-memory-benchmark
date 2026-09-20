package eval

import (
	"encoding/json"
	"fmt"
	"os"
)

// Fixtures is the local test-corpus format for testdata/fixtures.json —
// the hand-built ~20-turn conversation Tier 3 runs end to end
// (docs/06-testing.md), small enough that Recall@5 == 1.0 is the only
// acceptable result. Deliberately trivial:
//
//	{
//	  "turns": [
//	    {"id": "t1", "session_id": "s1", "speaker": "alice",
//	     "text": "I visited Paris last spring.",
//	     "date_time": "2023-05-30 14:02"}
//	  ],
//	  "questions": [
//	    {"id": "q1", "question": "Where did alice travel?",
//	     "evidence": ["t1"], "question_date": "2023-06-10"}
//	  ]
//	}
//
// Timestamps are strings parseable by pipeline.ParseTimestamp; evidence
// entries reference turn ids. Phase 6 writes testdata/fixtures.json to
// match this struct.
type Fixtures struct {
	Turns     []FixtureTurn     `json:"turns"`
	Questions []FixtureQuestion `json:"questions"`
	// Conversations is the multi-conversation form of the same format, for
	// bring-your-own datasets (docs/08-custom-datasets.md): each entry is
	// one retrieval scope with its own turns and (optional) questions. When
	// it is non-empty the top-level Turns/Questions must be empty.
	Conversations []FixtureConversation `json:"conversations,omitempty"`
}

// FixtureConversation is one retrieval scope of a custom dataset. Questions
// may be empty: such a dataset can be ingested and searched but yields no
// eval metrics.
type FixtureConversation struct {
	ID        string            `json:"id"`
	Turns     []FixtureTurn     `json:"turns"`
	Questions []FixtureQuestion `json:"questions,omitempty"`
}

// Normalize returns the conversations of a document in the multi-
// conversation form: the top-level turns/questions become one conversation
// with the given default id. It validates ids (non-empty, unique per
// conversation), that every turn carries a session id, and that every
// evidence id names a turn of the same conversation.
func (f *Fixtures) Normalize(defaultID string) ([]FixtureConversation, error) {
	convs := f.Conversations
	if len(f.Turns) > 0 || len(f.Questions) > 0 {
		if len(convs) > 0 {
			return nil, fmt.Errorf("eval: fixtures: use either top-level turns/questions or conversations, not both")
		}
		convs = []FixtureConversation{{ID: defaultID, Turns: f.Turns, Questions: f.Questions}}
	}
	seenConv := make(map[string]bool, len(convs))
	for i := range convs {
		c := &convs[i]
		if c.ID == "" {
			return nil, fmt.Errorf("eval: fixtures: conversation %d has no id", i)
		}
		if seenConv[c.ID] {
			return nil, fmt.Errorf("eval: fixtures: duplicate conversation id %q", c.ID)
		}
		seenConv[c.ID] = true
		if len(c.Turns) == 0 {
			return nil, fmt.Errorf("eval: fixtures: conversation %q has no turns", c.ID)
		}
		turnIDs := make(map[string]bool, len(c.Turns))
		for j, t := range c.Turns {
			switch {
			case t.ID == "":
				return nil, fmt.Errorf("eval: fixtures: conversation %q turn %d has no id", c.ID, j)
			case turnIDs[t.ID]:
				return nil, fmt.Errorf("eval: fixtures: conversation %q has duplicate turn id %q", c.ID, t.ID)
			case t.SessionID == "":
				return nil, fmt.Errorf("eval: fixtures: conversation %q turn %q has no session_id", c.ID, t.ID)
			case t.Text == "":
				return nil, fmt.Errorf("eval: fixtures: conversation %q turn %q has empty text", c.ID, t.ID)
			}
			turnIDs[t.ID] = true
		}
		for _, q := range c.Questions {
			if q.ID == "" || q.Question == "" {
				return nil, fmt.Errorf("eval: fixtures: conversation %q has a question without id or text", c.ID)
			}
			for _, ev := range q.Evidence {
				if !turnIDs[ev] {
					return nil, fmt.Errorf("eval: fixtures: conversation %q question %q: evidence %q is not a turn id", c.ID, q.ID, ev)
				}
			}
		}
	}
	return convs, nil
}

// FixtureTurn is one corpus turn.
type FixtureTurn struct {
	ID        string `json:"id"`
	SessionID string `json:"session_id"`
	Speaker   string `json:"speaker"`
	Text      string `json:"text"`
	DateTime  string `json:"date_time,omitempty"`
}

// FixtureQuestion is one query with its gold evidence turn ids.
type FixtureQuestion struct {
	ID           string   `json:"id"`
	Question     string   `json:"question"`
	Evidence     []string `json:"evidence"`
	QuestionDate string   `json:"question_date,omitempty"`
}

// ParseFixtures decodes a fixtures document.
func ParseFixtures(data []byte) (*Fixtures, error) {
	var f Fixtures
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("eval: parse fixtures: %w", err)
	}
	return &f, nil
}

// LoadFixtures reads and decodes a fixtures JSON file.
func LoadFixtures(path string) (*Fixtures, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("eval: load fixtures: %w", err)
	}
	return ParseFixtures(data)
}
