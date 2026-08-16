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
