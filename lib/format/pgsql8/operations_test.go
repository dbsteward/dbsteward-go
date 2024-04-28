package pgsql8

import "testing"

func TestNormalizeColumnCheckCondition(t *testing.T) {
	tests := []struct {
		in, out string
	}{
		{in: "j < 5", out: "j < 5"},
		{in: "(j < 5)", out: "j < 5"},
		{in: "((j < 5))", out: "j < 5"},
		{in: "CHECK ((j < 5))", out: "j < 5"},
		{in: "CHECK   (( j < 5 ))", out: "j < 5"},
		{in: " CHECK ((j < 5))", out: "j < 5"},
	}
	for _, test := range tests {
		t.Run(test.in, func(t *testing.T) {
			out := normalizeColumnCheckCondition(test.in)
			if out != test.out {
				t.Fatalf("expectd '%s' but '%s'", test.out, out)
			}
		})
	}
}
