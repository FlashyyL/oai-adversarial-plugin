package eval

import (
	"encoding/json"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestModelTraceGoldenParity(t *testing.T) {
	raw, err := os.ReadFile("testdata/modeltrace-golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Outputs  []TraceOutput
		Expected TraceAttribution
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		got, err := AnalyzeModelTrace(tc.Outputs)
		if err != nil {
			t.Fatal(err)
		}
		want := tc.Expected
		if got.Prediction != want.Prediction || got.Used != want.Used || !reflect.DeepEqual(got.Diagnostics, want.Diagnostics) || len(got.Results) != len(want.Results) {
			t.Fatal("attribution mismatch")
		}
		close := func(a, b float64) {
			t.Helper()
			if math.IsNaN(a) || math.Abs(a-b) > 1e-10 {
				t.Fatalf("got %.15g, want %.15g", a, b)
			}
		}
		close(got.Probability, want.Probability)
		close(got.FamilyProbability, want.FamilyProbability)
		for i, result := range got.Results {
			w := want.Results[i]
			if result.Model != w.Model || result.Family != w.Family {
				t.Fatal("ranking mismatch")
			}
			close(result.Probability, w.Probability)
			close(result.Score, w.Score)
			close(result.Similarity, w.Similarity)
		}
		for i, f := range got.Families {
			close(f.Probability, want.Families[i].Probability)
		}
	}
}

func TestModelTracePartialAndChallenges(t *testing.T) {
	if _, err := AnalyzeModelTrace([]TraceOutput{{Text: strings.Repeat("42,", 164), ExpectedCount: 300}}); err == nil {
		t.Fatal("short answer accepted")
	}
	got, err := AnalyzeModelTrace([]TraceOutput{{Text: strings.Repeat("42,", 165), ExpectedCount: 300}, {Text: "refused", ExpectedCount: 300}})
	if err != nil || got.Used != 1 {
		t.Fatal("partial attribution unavailable")
	}
	seen := map[int]bool{}
	for _, c := range TraceChallenges() {
		if c.ExpectedCount < 292 || c.ExpectedCount > 332 || seen[c.ExpectedCount] || !strings.Contains(c.Prompt, "禁止调用") {
			t.Fatal("invalid challenge")
		}
		seen[c.ExpectedCount] = true
	}
}
