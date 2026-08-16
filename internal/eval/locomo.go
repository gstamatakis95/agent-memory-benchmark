package eval

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
)

// LoCoMo loader (docs/01-retrieval.md section 2). The file
// (locomo10.json) is a JSON array of samples. Session keys are dynamic
// ("session_1", "session_1_date_time", ...) so the conversation object
// needs a custom decode. The LLM-generated fields (observation,
// session_summary) are deliberately not modeled — using them would
// violate the no-LLM constraint.

// LoCoMoCategoryAdversarial is the category whose questions carry no
// evidence and are excluded from retrieval scoring.
const LoCoMoCategoryAdversarial = 5

// LoCoMoTurn is one dialog turn. Speakers are two named humans
// (speaker_a/speaker_b), not user/assistant.
type LoCoMoTurn struct {
	Speaker     string `json:"speaker"`
	DiaID       string `json:"dia_id"`
	Text        string `json:"text"`
	BlipCaption string `json:"blip_caption,omitempty"`
}

// LoCoMoSession is one dated session; DateTime is the raw free-form
// "session_N_date_time" string (parse with pipeline.ParseTimestamp).
type LoCoMoSession struct {
	Index    int
	DateTime string
	Turns    []LoCoMoTurn
}

// LoCoMoQA is one QA item; Evidence lists the gold dia_ids.
type LoCoMoQA struct {
	Question string          `json:"question"`
	Answer   json.RawMessage `json:"answer,omitempty"`
	Category int             `json:"category"`
	Evidence StringList      `json:"evidence"`
}

// HasEvidence reports whether the item is scorable for retrieval
// (adversarial questions have no evidence).
func (qa LoCoMoQA) HasEvidence() bool { return len(qa.Evidence) > 0 }

// LoCoMoSample is one of the 10 conversations.
type LoCoMoSample struct {
	SampleID string
	SpeakerA string
	SpeakerB string
	Sessions []LoCoMoSession // ascending by Index
	QA       []LoCoMoQA
}

var (
	locomoSessionKey = regexp.MustCompile(`^session_(\d+)$`)
	locomoDateKey    = regexp.MustCompile(`^session_(\d+)_date_time$`)
)

// UnmarshalJSON decodes the dynamic session_N / session_N_date_time keys
// of the conversation object.
func (s *LoCoMoSample) UnmarshalJSON(b []byte) error {
	var raw struct {
		SampleID     string                     `json:"sample_id"`
		Conversation map[string]json.RawMessage `json:"conversation"`
		QA           []LoCoMoQA                 `json:"qa"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	s.SampleID = raw.SampleID
	s.QA = raw.QA

	sessions := make(map[int]*LoCoMoSession)
	get := func(idx int) *LoCoMoSession {
		if sess, ok := sessions[idx]; ok {
			return sess
		}
		sess := &LoCoMoSession{Index: idx}
		sessions[idx] = sess
		return sess
	}
	for key, val := range raw.Conversation {
		switch {
		case key == "speaker_a":
			if err := json.Unmarshal(val, &s.SpeakerA); err != nil {
				return fmt.Errorf("eval: locomo speaker_a: %w", err)
			}
		case key == "speaker_b":
			if err := json.Unmarshal(val, &s.SpeakerB); err != nil {
				return fmt.Errorf("eval: locomo speaker_b: %w", err)
			}
		case locomoSessionKey.MatchString(key):
			idx, _ := strconv.Atoi(locomoSessionKey.FindStringSubmatch(key)[1])
			if err := json.Unmarshal(val, &get(idx).Turns); err != nil {
				return fmt.Errorf("eval: locomo %s: %w", key, err)
			}
		case locomoDateKey.MatchString(key):
			idx, _ := strconv.Atoi(locomoDateKey.FindStringSubmatch(key)[1])
			if err := json.Unmarshal(val, &get(idx).DateTime); err != nil {
				return fmt.Errorf("eval: locomo %s: %w", key, err)
			}
		}
		// observation / session_summary / event_summary keys are ignored.
	}
	s.Sessions = make([]LoCoMoSession, 0, len(sessions))
	for _, sess := range sessions {
		s.Sessions = append(s.Sessions, *sess)
	}
	sort.Slice(s.Sessions, func(i, j int) bool { return s.Sessions[i].Index < s.Sessions[j].Index })
	return nil
}

// ParseLoCoMo decodes a LoCoMo dataset (a JSON array of samples).
func ParseLoCoMo(data []byte) ([]LoCoMoSample, error) {
	var samples []LoCoMoSample
	if err := json.Unmarshal(data, &samples); err != nil {
		return nil, fmt.Errorf("eval: parse locomo: %w", err)
	}
	return samples, nil
}

// LoadLoCoMo reads and decodes a LoCoMo JSON file.
func LoadLoCoMo(path string) ([]LoCoMoSample, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("eval: load locomo: %w", err)
	}
	return ParseLoCoMo(data)
}

// StringList is a []string that also tolerates nested JSON arrays of
// strings (some LoCoMo multi-hop evidence entries are nested), flattening
// them in order.
type StringList []string

func (l *StringList) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	out, err := flattenStrings(v)
	if err != nil {
		return err
	}
	*l = out
	return nil
}

func flattenStrings(v any) ([]string, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case string:
		return []string{x}, nil
	case []any:
		var out []string
		for _, e := range x {
			f, err := flattenStrings(e)
			if err != nil {
				return nil, err
			}
			out = append(out, f...)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("eval: evidence element %T is not a string", v)
	}
}
