package eval

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const lmeSample = `[
  {
    "question_id": "q1",
    "question_type": "multi-session",
    "question": "What did I order?",
    "answer": "pizza",
    "question_date": "2023/05/20 (Sat) 02:21",
    "haystack_dates": ["2023/05/01 (Mon) 01:00", "2023/05/02 (Tue) 01:00"],
    "haystack_session_ids": ["s1", "s2"],
    "haystack_sessions": [
      [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "you ordered pizza", "has_answer": true}
      ],
      [
        {"role": "user", "content": "unrelated"}
      ]
    ],
    "answer_session_ids": ["s1"]
  },
  {
    "question_id": "q2_abs",
    "question_type": "knowledge-update",
    "question": "never happened?",
    "question_date": "2023/06/01 (Thu) 10:00",
    "haystack_dates": [],
    "haystack_session_ids": [],
    "haystack_sessions": [],
    "answer_session_ids": []
  }
]`

func TestParseLongMemEvalRoundTrip(t *testing.T) {
	qs, err := ParseLongMemEval([]byte(lmeSample))
	require.NoError(t, err)
	require.Len(t, qs, 2)

	q := qs[0]
	assert.Equal(t, "q1", q.QuestionID)
	assert.Equal(t, "multi-session", q.QuestionType)
	assert.Equal(t, "2023/05/20 (Sat) 02:21", q.QuestionDate)
	assert.Equal(t, []string{"s1", "s2"}, q.HaystackSessionIDs)
	assert.Len(t, q.HaystackDates, 2)
	require.Len(t, q.HaystackSessions, 2)
	assert.Equal(t, "assistant", q.HaystackSessions[0][1].Role)
	assert.True(t, q.HaystackSessions[0][1].HasAnswer)
	assert.False(t, q.HaystackSessions[0][0].HasAnswer)

	assert.False(t, q.IsAbstention())
	assert.Equal(t, []string{"s1"}, q.GoldSessionIDs())
	assert.Equal(t, []string{"s1_1"}, q.GoldTurnIDs(), "has_answer turn 1 of session s1")

	abs := qs[1]
	assert.True(t, abs.IsAbstention())
	assert.Nil(t, abs.GoldSessionIDs(), "_abs questions carry no retrieval gold")
	assert.Nil(t, abs.GoldTurnIDs())
}

func TestTurnID(t *testing.T) {
	assert.Equal(t, "s7_0", TurnID("s7", 0))
	assert.Equal(t, "answer_3_12", TurnID("answer_3", 12))
}
