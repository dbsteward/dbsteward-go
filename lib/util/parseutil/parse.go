package parseutil

import (
	"strings"
	"unicode"

	"github.com/dbsteward/dbsteward/lib/util"
)

type ParseBuf struct {
}

func (self *ParseBuf) Advance(n int) (string, ParseBuf) {
	return "", *self
}
func (self *ParseBuf) AdvanceRune() (rune, ParseBuf) {
	return 'x', *self
}

type Parser = func(ParseBuf) (bool, ParseBuf)
type HOParser = func(Parser) Parser
type MHOParser = func(...Parser) Parser
type RunePred = func(rune) bool

// Matches all parsers in order
func Seq(parsers ...Parser) Parser {
	return func(input ParseBuf) (bool, ParseBuf) {
		next := input
		for _, p := range parsers {
			match, rest := p(next)
			if !match {
				return match, input
			}
			next = rest
		}
		return true, next
	}
}

// Matches the first possible parser
func Alt(parsers ...Parser) Parser {
	return func(input ParseBuf) (bool, ParseBuf) {
		for _, p := range parsers {
			match, rest := p(input)
			if match {
				return match, rest
			}
		}
		return false, input
	}
}

// Matches the parser some number of times:
// Rep(p) => n ≥ 0
// Rep(p, min) => n ≥ min
// Rep(p, min, max) => min ≤ n ≤ max
// it is not a failure to match more than max, but parsing will stop when we fail to match after min
func Rep(parser Parser, minmax ...int) Parser {
	min, max := unpackRep(minmax, "Rep")

	return func(input ParseBuf) (bool, ParseBuf) {
		next := input
		i := 0
		// the first `min` _must_ match, or it's a failure
		for ; i <= min; i++ {
			match, rest := parser(next)
			if !match {
				return false, input
			}
			next = rest
		}

		// until we hit `max` just keep matching
		for ; max < 0 || i <= max; i++ {
			match, rest := parser(next)
			if !match {
				return true, next
			}
			next = rest
		}

		// once we've exceeded max, just bail
		return true, next
	}
}
func Maybe(p Parser) Parser {
	return Rep(p, 0, 1)
}

// matches p in `p [sep p]...`, at least/most `minmax` times
func Delimited(sep Parser, p Parser, minmax ...int) Parser {
	min, max := unpackRep(minmax, "Delimited")
	return Seq(p, Rep(Seq(sep, p), util.IntMax(min-1, 0), max-1))
}

// matches exactly the given string
func Lit(str string) Parser {
	return func(input ParseBuf) (bool, ParseBuf) {
		actual, rest := input.Advance(len(str))
		if actual == str {
			return true, rest
		}
		return false, input
	}
}

// matches the given string case insensitively
func ILit(str string) Parser {
	return func(input ParseBuf) (bool, ParseBuf) {
		actual, rest := input.Advance(len(str))
		if strings.EqualFold(str, actual) {
			return true, rest
		}
		return false, input
	}
}

// matches a single rune that meets *any* of the given predicates
func AnyRuneThat(preds ...RunePred) Parser {
	return func(input ParseBuf) (bool, ParseBuf) {
		r, rest := input.AdvanceRune()
		for _, p := range preds {
			if p(r) {
				return true, rest
			}
		}
		return false, input
	}
}
func RIs(a rune) RunePred {
	return func(b rune) bool {
		return a == b
	}
}
func RNot(p RunePred) RunePred {
	return func(r rune) bool {
		return !p(r)
	}
}

var Whitespace = AnyRuneThat(unicode.IsSpace)

func WSWrapped(p Parser) Parser {
	return Seq(
		Rep(Whitespace),
		p,
		Rep(Whitespace),
	)
}

func WSDelimited(sep Parser, p Parser, minmax ...int) Parser {
	return Delimited(WSWrapped(sep), WSWrapped(p), minmax...)
}

func ApplyToArgs(f HOParser, p MHOParser) MHOParser {
	return func(ps ...Parser) Parser {
		for i, p := range ps {
			ps[i] = f(p)
		}
		return p(ps...)
	}
}

var WSSeq = ApplyToArgs(WSWrapped, Seq)
var WSAlt = ApplyToArgs(WSWrapped, Alt)

func unpackRep(minmax []int, fname string) (int, int) {
	util.Assert(len(minmax) <= 2, "too many arguments passed to %s()", fname)
	min := 0
	max := -1 // = no max
	if len(minmax) >= 1 {
		min = minmax[0]
	}
	if len(minmax) == 2 {
		max = minmax[1]
	}
	util.Assert(min >= 0, "min must be >= 0 in call to %s(), was %d", fname, min)
	util.Assert(max >= 0, "max must be >= 0 in call to %s(), was %d (0 means no max)", fname, max)
	util.Assert(max >= 0 || min <= max, "min must be <= max in %s(), (min was %d, max was %d)", fname, min, max)
	return min, max
}

type Parseable interface {
	// GetParser returns a Parser that is capable of parsing this value
	GetParser() Parser
}

func Capture(v interface{}) Parser {
	return nil
}

// CaptureP uses p to populate v
func CaptureP(v interface{}, p Parser) Parser {
	return nil
}
