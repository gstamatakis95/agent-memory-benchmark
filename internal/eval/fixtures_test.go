package eval

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const fixturesSample = `{
  "turns": [
    {"id": "t1", "session_id": "s1", "speaker": "alice",
     "text": "I visited Paris last spring.", "date_time": "2023-05-30 14:02"},
    {"id": "t2", "session_id": "s2", "speaker": "bob",
     "text": "I adopted a cat."}
  ],
  "questions": [
    {"id": "q1", "question": "Where did alice travel?",
     "evidence": ["t1"], "question_date": "2023-06-10"}
  ]
}`

func TestParseFixturesRoundTrip(t *testing.T) {
	f, err := ParseFixtures([]byte(fixturesSample))
	require.NoError(t, err)

	require.Len(t, f.Turns, 2)
	assert.Equal(t, FixtureTurn{
		ID: "t1", SessionID: "s1", Speaker: "alice",
		Text: "I visited Paris last spring.", DateTime: "2023-05-30 14:02",
	}, f.Turns[0])
	assert.Empty(t, f.Turns[1].DateTime, "date_time is optional")

	require.Len(t, f.Questions, 1)
	assert.Equal(t, FixtureQuestion{
		ID: "q1", Question: "Where did alice travel?",
		Evidence: []string{"t1"}, QuestionDate: "2023-06-10",
	}, f.Questions[0])

	// Marshal -> parse round trip is lossless.
	out, err := json.Marshal(f)
	require.NoError(t, err)
	f2, err := ParseFixtures(out)
	require.NoError(t, err)
	assert.Equal(t, f, f2)
}
