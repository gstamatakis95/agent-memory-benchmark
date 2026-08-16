package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"example.com/agentmem/internal/blob"
	"example.com/agentmem/internal/eval"
)

// item is one raw memory to ingest (and, at eval time, the local twin of an
// ingested row: recomputing its blob hash maps server rows back to turn ids).
type item struct {
	TurnID    string // dataset-level id: fixture "t1", LoCoMo dia_id, LME "<sess>_<i>"
	SessionID string
	Speaker   string
	Text      string
	DateTime  string // raw date string, parsed server-side
}

// question is one eval query with its gold evidence turn ids.
type question struct {
	ID           string
	Question     string
	QuestionDate string
	Evidence     []string
	Group        string // LoCoMo category / LME question_type / "fixtures"
}

// conversation is one retrieval scope: LoCoMo sample, LongMemEval question
// haystack, or the single fixtures conversation. Num is the numeric
// conversation id used on the wire (FetchReq/ProgressReq carry int64 ids;
// the ingest order makes the mapping deterministic on both sides).
type conversation struct {
	Num       int64
	Name      string
	Items     []item
	Questions []question
}

// envelopeOf rebuilds the exact blob the server writes for an item, so its
// content hash identifies the row (see internal/blob).
func envelopeOf(convNum int64, it item) blob.Envelope {
	return blob.Envelope{
		ConversationID: strconv.FormatInt(convNum, 10),
		SessionID:      it.SessionID,
		TurnID:         it.TurnID,
		Speaker:        it.Speaker,
		Text:           it.Text,
		DateTime:       it.DateTime,
	}
}

// findDataFile probes the candidate paths in order. run.sh mounts nothing
// into the server image, so fixtures are baked at /app/testdata and real
// datasets must be present under /app/datasets at image build time (see
// scripts/download-dataset.sh and the Dockerfile note).
func findDataFile(explicit string, candidates ...string) (string, error) {
	if explicit != "" {
		candidates = []string{explicit}
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c, nil
		}
	}
	return "", fmt.Errorf("dataset file not found; tried %v", candidates)
}

func datasetsDir() string {
	if d := os.Getenv("DATASETS_DIR"); d != "" {
		return d
	}
	return "/app/datasets"
}

// loadDataset builds the ingest/eval view of a dataset.
func loadDataset(name string) ([]conversation, error) {
	switch name {
	case "fixtures":
		path, err := findDataFile(os.Getenv("FIXTURES_PATH"),
			"/app/testdata/fixtures.json", "testdata/fixtures.json")
		if err != nil {
			return nil, err
		}
		return loadFixtures(path)
	case "locomo":
		path, err := findDataFile("",
			filepath.Join(datasetsDir(), "locomo10.json"),
			"datasets/locomo10.json", "/data/locomo10.json")
		if err != nil {
			return nil, err
		}
		return loadLoCoMo(path)
	case "longmemeval_s":
		path, err := findDataFile("",
			filepath.Join(datasetsDir(), "longmemeval_s.json"),
			"datasets/longmemeval_s.json", "/data/longmemeval_s.json")
		if err != nil {
			return nil, err
		}
		return loadLongMemEval(path)
	}
	return nil, fmt.Errorf("unknown dataset %q (want fixtures|locomo|longmemeval_s)", name)
}

func loadFixtures(path string) ([]conversation, error) {
	f, err := eval.LoadFixtures(path)
	if err != nil {
		return nil, err
	}
	conv := conversation{Num: 1, Name: "fixtures"}
	for _, t := range f.Turns {
		conv.Items = append(conv.Items, item{
			TurnID:    t.ID,
			SessionID: t.SessionID,
			Speaker:   t.Speaker,
			Text:      t.Text,
			DateTime:  t.DateTime,
		})
	}
	for _, q := range f.Questions {
		conv.Questions = append(conv.Questions, question{
			ID:           q.ID,
			Question:     q.Question,
			QuestionDate: q.QuestionDate,
			Evidence:     q.Evidence,
			Group:        "fixtures",
		})
	}
	return []conversation{conv}, nil
}

func loadLoCoMo(path string) ([]conversation, error) {
	samples, err := eval.LoadLoCoMo(path)
	if err != nil {
		return nil, err
	}
	convs := make([]conversation, 0, len(samples))
	for i, s := range samples {
		conv := conversation{Num: int64(i + 1), Name: s.SampleID}
		for _, sess := range s.Sessions {
			sid := fmt.Sprintf("session_%d", sess.Index)
			for _, turn := range sess.Turns {
				conv.Items = append(conv.Items, item{
					TurnID:    turn.DiaID,
					SessionID: sid,
					Speaker:   turn.Speaker,
					Text:      turn.Text,
					DateTime:  sess.DateTime,
				})
			}
		}
		for qi, qa := range s.QA {
			conv.Questions = append(conv.Questions, question{
				ID:       fmt.Sprintf("%s_q%d", s.SampleID, qi),
				Question: qa.Question,
				Evidence: qa.Evidence, // empty for adversarial => skipped by Evaluate
				Group:    "category_" + strconv.Itoa(qa.Category),
			})
		}
		convs = append(convs, conv)
	}
	return convs, nil
}

func loadLongMemEval(path string) ([]conversation, error) {
	qs, err := eval.LoadLongMemEval(path)
	if err != nil {
		return nil, err
	}
	convs := make([]conversation, 0, len(qs))
	for i, q := range qs {
		conv := conversation{Num: int64(i + 1), Name: q.QuestionID}
		for si, sess := range q.HaystackSessions {
			if si >= len(q.HaystackSessionIDs) {
				break
			}
			sid := q.HaystackSessionIDs[si]
			date := ""
			if si < len(q.HaystackDates) {
				date = q.HaystackDates[si]
			}
			for ti, turn := range sess {
				conv.Items = append(conv.Items, item{
					TurnID:    eval.TurnID(sid, ti),
					SessionID: sid,
					Speaker:   turn.Role,
					Text:      turn.Content,
					DateTime:  date,
				})
			}
		}
		conv.Questions = append(conv.Questions, question{
			ID:           q.QuestionID,
			Question:     q.Question,
			QuestionDate: q.QuestionDate,
			Evidence:     q.GoldTurnIDs(), // nil for _abs => skipped by Evaluate
			Group:        q.QuestionType,
		})
		convs = append(convs, conv)
	}
	return convs, nil
}
