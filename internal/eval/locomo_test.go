package eval

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const locomoSample = `[
  {
    "sample_id": "conv-1",
    "conversation": {
      "speaker_a": "Alice",
      "speaker_b": "Bob",
      "session_1_date_time": "1:56 pm on 8 May, 2023",
      "session_1": [
        {"speaker": "Alice", "dia_id": "D1:1", "text": "hi", "blip_caption": "a photo"},
        {"speaker": "Bob", "dia_id": "D1:2", "text": "hello"}
      ],
      "session_2_date_time": "2:10 pm on 9 May, 2023",
      "session_2": [
        {"speaker": "Alice", "dia_id": "D2:1", "text": "news"}
      ],
      "observation": {"llm_generated": "must be ignored"},
      "session_summary": "also ignored"
    },
    "qa": [
      {"question": "single?", "answer": "a", "category": 1, "evidence": ["D1:1"]},
      {"question": "multi?", "answer": 2, "category": 2, "evidence": [["D1:2"], ["D2:1"]]},
      {"question": "adversarial?", "category": 5}
    ]
  }
]`

func TestParseLoCoMoRoundTrip(t *testing.T) {
	samples, err := ParseLoCoMo([]byte(locomoSample))
	require.NoError(t, err)
	require.Len(t, samples, 1)
	s := samples[0]

	assert.Equal(t, "conv-1", s.SampleID)
	assert.Equal(t, "Alice", s.SpeakerA)
	assert.Equal(t, "Bob", s.SpeakerB)

	require.Len(t, s.Sessions, 2)
	assert.Equal(t, 1, s.Sessions[0].Index)
	assert.Equal(t, "1:56 pm on 8 May, 2023", s.Sessions[0].DateTime)
	require.Len(t, s.Sessions[0].Turns, 2)
	assert.Equal(t, "D1:1", s.Sessions[0].Turns[0].DiaID)
	assert.Equal(t, "a photo", s.Sessions[0].Turns[0].BlipCaption)
	assert.Equal(t, 2, s.Sessions[1].Index)
	assert.Equal(t, "D2:1", s.Sessions[1].Turns[0].DiaID)

	require.Len(t, s.QA, 3)
	assert.Equal(t, 1, s.QA[0].Category)
	assert.Equal(t, []string{"D1:1"}, []string(s.QA[0].Evidence))
	assert.True(t, s.QA[0].HasEvidence())

	// Nested evidence arrays are flattened in order.
	assert.Equal(t, []string{"D1:2", "D2:1"}, []string(s.QA[1].Evidence))

	// Adversarial: no evidence, excluded from retrieval scoring.
	assert.Equal(t, LoCoMoCategoryAdversarial, s.QA[2].Category)
	assert.False(t, s.QA[2].HasEvidence())
}

func TestParseLoCoMoRejectsGarbage(t *testing.T) {
	_, err := ParseLoCoMo([]byte(`{"not": "a list"}`))
	assert.Error(t, err)

	_, err = ParseLoCoMo([]byte(`[{"qa": [{"evidence": [42]}]}]`))
	assert.Error(t, err, "non-string evidence leaf must error, not be silently dropped")
}
