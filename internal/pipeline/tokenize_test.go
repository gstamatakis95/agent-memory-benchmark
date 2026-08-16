package pipeline

import (
	"reflect"
	"testing"
)

func TestTokenize(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{
			"stopwords and stemming",
			"The quick brown foxes are running!",
			[]string{"quick", "brown", "fox", "run"},
		},
		{
			"punctuation stripped",
			"Hello, world!!! (really?)",
			[]string{"hello", "world", "realli"},
		},
		{
			"contractions",
			"I don't think it's working",
			[]string{"think", "work"},
		},
		{
			"numbers kept",
			"paris trip May 2023",
			[]string{"pari", "trip", "may", "2023"},
		},
		{
			"case folded before stemming",
			"RUNNING Runner runs",
			[]string{"run", "runner", "run"},
		},
		{
			"all stopwords",
			"is it not the same",
			[]string{},
		},
		{
			"empty",
			"",
			[]string{},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Tokenize(c.in)
			if len(got) == 0 && len(c.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("Tokenize(%q) = %v, want %v", c.in, got, c.want)
			}
		})
	}
}
