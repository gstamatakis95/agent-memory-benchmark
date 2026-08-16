package eval

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// LongMemEval loader (docs/01-retrieval.md section 2). The file
// (longmemeval_s.json / _m / _oracle) is a JSON array of questions.

// LongMemEvalTurn is one turn of a haystack session; evidence turns
// carry has_answer=true.
type LongMemEvalTurn struct {
	Role      string `json:"role"` // "user" | "assistant"
	Content   string `json:"content"`
	HasAnswer bool   `json:"has_answer,omitempty"`
}

// LongMemEvalQuestion is one benchmark item. HaystackSessionIDs,
// HaystackDates and HaystackSessions are parallel (sorted by timestamp
// for the _s/_m files, unsorted for oracle); AnswerSessionIDs is the
// session-level gold.
type LongMemEvalQuestion struct {
	QuestionID         string              `json:"question_id"`
	QuestionType       string              `json:"question_type"`
	Question           string              `json:"question"`
	Answer             json.RawMessage     `json:"answer,omitempty"`
	QuestionDate       string              `json:"question_date"`
	HaystackDates      []string            `json:"haystack_dates"`
	HaystackSessionIDs []string            `json:"haystack_session_ids"`
	HaystackSessions   [][]LongMemEvalTurn `json:"haystack_sessions"`
	AnswerSessionIDs   []string            `json:"answer_session_ids"`
}

// IsAbstention reports whether this is one of the 30 abstention
// questions (question_id ends in "_abs"), which are skipped for
// retrieval scoring per the official evaluation script.
func (q *LongMemEvalQuestion) IsAbstention() bool {
	return strings.HasSuffix(q.QuestionID, "_abs")
}

// GoldSessionIDs is the session-level gold evidence (nil for abstention
// questions, which reference non-existent events).
func (q *LongMemEvalQuestion) GoldSessionIDs() []string {
	if q.IsAbstention() {
		return nil
	}
	return q.AnswerSessionIDs
}

// TurnID is the corpus-wide id convention for a turn-granular memory:
// "<session_id>_<turn_index>" with the turn's 0-based index inside its
// session. Ingest (Phase 6) must synthesize turn ids the same way so
// GoldTurnIDs matches retrieved ids.
func TurnID(sessionID string, turn int) string {
	return fmt.Sprintf("%s_%d", sessionID, turn)
}

// GoldTurnIDs is the turn-level gold evidence: the TurnID of every
// has_answer turn across the haystack. Nil for abstention questions.
func (q *LongMemEvalQuestion) GoldTurnIDs() []string {
	if q.IsAbstention() {
		return nil
	}
	var gold []string
	for si, session := range q.HaystackSessions {
		if si >= len(q.HaystackSessionIDs) {
			break
		}
		for ti, turn := range session {
			if turn.HasAnswer {
				gold = append(gold, TurnID(q.HaystackSessionIDs[si], ti))
			}
		}
	}
	return gold
}

// ParseLongMemEval decodes a LongMemEval dataset (a JSON array of
// questions).
func ParseLongMemEval(data []byte) ([]LongMemEvalQuestion, error) {
	var qs []LongMemEvalQuestion
	if err := json.Unmarshal(data, &qs); err != nil {
		return nil, fmt.Errorf("eval: parse longmemeval: %w", err)
	}
	return qs, nil
}

// LoadLongMemEval reads and decodes a LongMemEval JSON file.
func LoadLongMemEval(path string) ([]LongMemEvalQuestion, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("eval: load longmemeval: %w", err)
	}
	return ParseLongMemEval(data)
}
