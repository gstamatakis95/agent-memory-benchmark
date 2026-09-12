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

func TestFixturesNormalize(t *testing.T) {
	// Single-conversation form becomes one conversation named by the caller.
	f, err := ParseFixtures([]byte(fixturesSample))
	require.NoError(t, err)
	convs, err := f.Normalize("fixtures")
	require.NoError(t, err)
	require.Len(t, convs, 1)
	assert.Equal(t, "fixtures", convs[0].ID)
	assert.Len(t, convs[0].Turns, 2)

	// Multi-conversation form passes through; questions are optional.
	multi := &Fixtures{Conversations: []FixtureConversation{
		{ID: "a", Turns: []FixtureTurn{{ID: "t1", SessionID: "s", Text: "x"}}},
		{ID: "b", Turns: []FixtureTurn{{ID: "t1", SessionID: "s", Text: "y"}},
			Questions: []FixtureQuestion{{ID: "q", Question: "?", Evidence: []string{"t1"}}}},
	}}
	convs, err = multi.Normalize("ignored")
	require.NoError(t, err)
	require.Len(t, convs, 2)

	bad := []Fixtures{
		{Turns: f.Turns, Conversations: multi.Conversations},                                                                                                // both forms
		{Conversations: []FixtureConversation{{ID: "", Turns: multi.Conversations[0].Turns}}},                                                               // no conv id
		{Conversations: []FixtureConversation{{ID: "a"}}},                                                                                                   // no turns
		{Conversations: []FixtureConversation{{ID: "a", Turns: []FixtureTurn{{ID: "t", Text: "x"}}}}},                                                       // no session
		{Conversations: []FixtureConversation{{ID: "a", Turns: []FixtureTurn{{ID: "t", SessionID: "s", Text: "x"}, {ID: "t", SessionID: "s", Text: "y"}}}}}, // dup turn
		{Conversations: []FixtureConversation{{ID: "a", Turns: []FixtureTurn{{ID: "t", SessionID: "s", Text: "x"}},
			Questions: []FixtureQuestion{{ID: "q", Question: "?", Evidence: []string{"nope"}}}}}}, // dangling evidence
	}
	for i := range bad {
		_, err := bad[i].Normalize("x")
		assert.Error(t, err, "case %d", i)
	}
}
