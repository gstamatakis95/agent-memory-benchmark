package pipeline

import (
	"testing"
	"time"
)

// TestParseTimestampTable is the docs/06-testing.md Tier 0 date-parsing table
// test: LoCoMo free-form formats, LongMemEval haystack_dates, ambiguous
// mm/dd vs dd/mm rejection via ParseStrict, and UTC normalization.
func TestParseTimestampTable(t *testing.T) {
	ok := []struct {
		name, in string
		want     time.Time
	}{
		{
			"locomo pm",
			"1:56 pm on 8 May, 2023",
			time.Date(2023, 5, 8, 13, 56, 0, 0, time.UTC),
		},
		{
			"locomo am",
			"10:05 am on 15 January, 2023",
			time.Date(2023, 1, 15, 10, 5, 0, 0, time.UTC),
		},
		{
			"locomo uppercase PM",
			"1:56 PM on 8 May, 2023",
			time.Date(2023, 5, 8, 13, 56, 0, 0, time.UTC),
		},
		{
			"locomo no comma",
			"7:30 pm on 21 June 2023",
			time.Date(2023, 6, 21, 19, 30, 0, 0, time.UTC),
		},
		{
			"locomo date only",
			"8 May, 2023",
			time.Date(2023, 5, 8, 0, 0, 0, 0, time.UTC),
		},
		{
			"longmemeval haystack_dates with weekday",
			"2023/05/20 (Sat) 02:21",
			time.Date(2023, 5, 20, 2, 21, 0, 0, time.UTC),
		},
		{
			"iso date",
			"2023-05-30",
			time.Date(2023, 5, 30, 0, 0, 0, 0, time.UTC),
		},
		{
			"iso datetime",
			"2023-05-30 14:10:00",
			time.Date(2023, 5, 30, 14, 10, 0, 0, time.UTC),
		},
		{
			"utc normalization of zoned input",
			"2023-05-08T15:56:00+02:00",
			time.Date(2023, 5, 8, 13, 56, 0, 0, time.UTC),
		},
		{
			"year first slashes unambiguous",
			"2023/05/20 02:21",
			time.Date(2023, 5, 20, 2, 21, 0, 0, time.UTC),
		},
	}
	for _, c := range ok {
		t.Run(c.name, func(t *testing.T) {
			got, err := ParseTimestamp(c.in)
			if err != nil {
				t.Fatalf("ParseTimestamp(%q): %v", c.in, err)
			}
			if !got.Equal(c.want) {
				t.Fatalf("ParseTimestamp(%q) = %v, want %v", c.in, got, c.want)
			}
			if got.Location() != time.UTC {
				t.Fatalf("ParseTimestamp(%q) location = %v, want UTC", c.in, got.Location())
			}
		})
	}

	bad := []struct{ name, in string }{
		// ParseStrict must ERROR on ambiguous mm/dd vs dd/mm, never guess.
		{"ambiguous mm/dd vs dd/mm", "03/04/2023"},
		{"ambiguous with time", "03/04/2023 10:00"},
		{"empty", ""},
		{"garbage", "not a date at all"},
	}
	for _, c := range bad {
		t.Run(c.name, func(t *testing.T) {
			if got, err := ParseTimestamp(c.in); err == nil {
				t.Fatalf("ParseTimestamp(%q) = %v, want error", c.in, got)
			}
		})
	}
}

func TestParseRelative(t *testing.T) {
	// Friday 2023-05-12: "last Tuesday" is 2023-05-09.
	base := time.Date(2023, 5, 12, 12, 0, 0, 0, time.UTC)

	got, ok, err := ParseRelative("last Tuesday", base)
	if err != nil || !ok {
		t.Fatalf("ParseRelative: ok=%v err=%v", ok, err)
	}
	if got.Year() != 2023 || got.Month() != time.May || got.Day() != 9 {
		t.Fatalf("last Tuesday from %v = %v, want 2023-05-09", base, got)
	}
	if got.Location() != time.UTC {
		t.Fatalf("relative result not UTC: %v", got.Location())
	}

	if _, ok, err := ParseRelative("no temporal content here", base); err != nil || ok {
		t.Fatalf("expected no match, got ok=%v err=%v", ok, err)
	}
}

func TestExtractQueryTime(t *testing.T) {
	base := time.Date(2023, 5, 12, 12, 0, 0, 0, time.UTC)
	if _, ok := ExtractQueryTime("what happened last Tuesday?", base); !ok {
		t.Fatal("expected relative anchor to be found")
	}
	if _, ok := ExtractQueryTime("what is my favorite color?", base); ok {
		t.Fatal("expected no temporal anchor: must not guess")
	}
}
