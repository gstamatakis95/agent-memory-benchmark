package pipeline

import (
	"reflect"
	"testing"
	"time"
)

func turn(session, id, speaker, text string) Turn {
	return Turn{SessionID: session, TurnID: id, Speaker: speaker, Text: text}
}

func roundTurnIDs(rs []Round) [][]string {
	out := make([][]string, len(rs))
	for i, r := range rs {
		out[i] = r.TurnIDs()
	}
	return out
}

func TestAssembleRoundsPairsUserAssistant(t *testing.T) {
	// LongMemEval-style user/assistant alternation.
	rounds := AssembleRounds([]Turn{
		turn("s1", "t1", "user", "hi"),
		turn("s1", "t2", "assistant", "hello"),
		turn("s1", "t3", "user", "bye"),
		turn("s1", "t4", "assistant", "goodbye"),
	})
	want := [][]string{{"t1", "t2"}, {"t3", "t4"}}
	if got := roundTurnIDs(rounds); !reflect.DeepEqual(got, want) {
		t.Fatalf("rounds = %v, want %v", got, want)
	}
}

func TestAssembleRoundsLoCoMoSpeakers(t *testing.T) {
	// LoCoMo is two named human speakers (never assume assistant roles),
	// with consecutive same-speaker turns possible.
	rounds := AssembleRounds([]Turn{
		turn("session_1", "D1:1", "Caroline", "hey!"),
		turn("session_1", "D1:2", "Melanie", "hey, how are you?"),
		turn("session_1", "D1:3", "Caroline", "great news today"),
		turn("session_1", "D1:4", "Caroline", "I got the job"), // same speaker twice
		turn("session_1", "D1:5", "Melanie", "congrats!"),
	})
	want := [][]string{{"D1:1", "D1:2"}, {"D1:3"}, {"D1:4", "D1:5"}}
	if got := roundTurnIDs(rounds); !reflect.DeepEqual(got, want) {
		t.Fatalf("rounds = %v, want %v", got, want)
	}
}

func TestAssembleRoundsSessionBoundary(t *testing.T) {
	rounds := AssembleRounds([]Turn{
		turn("s1", "t1", "user", "a"),
		turn("s2", "t2", "assistant", "b"), // new session: never pairs across
	})
	want := [][]string{{"t1"}, {"t2"}}
	if got := roundTurnIDs(rounds); !reflect.DeepEqual(got, want) {
		t.Fatalf("rounds = %v, want %v", got, want)
	}
	if rounds[0].SessionID != "s1" || rounds[1].SessionID != "s2" {
		t.Fatalf("session ids wrong: %+v", rounds)
	}
}

func TestAssembleRoundsKeepsTurnRowsAndMetadata(t *testing.T) {
	ts := time.Date(2023, 5, 8, 13, 56, 0, 0, time.UTC)
	in := []Turn{
		{SessionID: "s1", TurnID: "t1", Speaker: "user", Text: "where is Paris?", TS: &ts},
		{SessionID: "s1", TurnID: "t2", Speaker: "assistant", Text: "in France", TS: &ts},
	}
	rounds := AssembleRounds(in)
	if len(rounds) != 1 {
		t.Fatalf("want 1 round, got %d", len(rounds))
	}
	r := rounds[0]
	// Individual turns are retained inside the round (turn-level evidence).
	if !reflect.DeepEqual(r.Turns, in) {
		t.Fatalf("round turns = %+v, want %+v", r.Turns, in)
	}
	if r.TS == nil || !r.TS.Equal(ts) {
		t.Fatalf("round TS = %v, want %v", r.TS, ts)
	}
	if r.RoundID == "" {
		t.Fatal("round id empty")
	}
	wantText := "user: where is Paris?\nassistant: in France"
	if r.Text() != wantText {
		t.Fatalf("round text = %q, want %q", r.Text(), wantText)
	}
	// Input slice must be untouched: callers keep the turn rows.
	if in[0].TurnID != "t1" || in[1].TurnID != "t2" {
		t.Fatal("input turns mutated")
	}
}

func TestAssembleRoundsEmpty(t *testing.T) {
	if rounds := AssembleRounds(nil); len(rounds) != 0 {
		t.Fatalf("want no rounds, got %v", rounds)
	}
}
