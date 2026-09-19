package main

import (
	"strings"
	"testing"
)

func TestRunteamLengthPolicy(t *testing.T) {
	for _, n := range []int{0, 291, 292, 312, 332, 356, 400} {
		want := n == 292 || n == 332
		if isAcceptedStateLength(n) != want {
			t.Fatalf("length %d", n)
		}
		if isHealthyTurnState("gpt-6-astra", "gpt-6-astra", strings.Repeat("x", n)) != want {
			t.Fatalf("healthy %d", n)
		}
		if isHealthyTurnState("gpt-6-astra", "gpt-5.6-luna", strings.Repeat("x", n)) {
			t.Fatalf("mismatch %d", n)
		}
	}
}
