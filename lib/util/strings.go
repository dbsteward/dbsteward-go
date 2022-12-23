package util

import (
	"regexp"
	"strings"
)

func PrefixLines(str, prefix string) string {
	return prefix + strings.ReplaceAll(str, "\n", "\n"+prefix)
}

// joins the listed strings together with the given separator,
// but only if the string is not empty
// e.g. CondJoin(",", "foo", "", "bar") results in "foo,bar"
// whereas strings.Join([]string{"foo", "", "bar"},",") would result in "foo,,bar"
func CondJoin(sep string, strs ...string) string {
	out := ""
	for _, s := range strs {
		if s != "" {
			if out != "" {
				out += sep
			}
			out += s
		}
	}
	return out
}

func MaybeStr(cond bool, str string) string {
	return ChooseStr(cond, str, "")
}

func ChooseStr(cond bool, trueStr, falseStr string) string {
	if cond {
		return trueStr
	}
	return falseStr
}

// returns the first non-empty string, or the empty string
func CoalesceStr(strs ...string) string {
	for _, s := range strs {
		if len(s) > 0 {
			return s
		}
	}
	return ""
}

func SplitKV(str, sep string) (string, string) {
	kv := strings.Split(str, sep)
	if len(kv) == 1 {
		return kv[0], ""
	}
	return kv[0], kv[1]
}

func CollectKV(kv []string, sep string) map[string]string {
	out := make(map[string]string, len(kv))
	for _, kv := range kv {
		k, v := SplitKV(kv, sep)
		out[k] = v
	}
	return out
}

func ParseKV(str, fieldSep, keySep string) map[string]string {
	return CollectKV(strings.Split(str, fieldSep), keySep)
}

func EncodeKV(kv map[string]string, fieldSep, keySep string) string {
	parts := make([]string, 0, len(kv))
	for k, v := range kv {
		parts = append(parts, k+keySep+v)
	}
	return strings.Join(parts, fieldSep)
}

// returns true if str starts with the given prefix, case insensitively
func IHasPrefix(str, prefix string) bool {
	return IIndex(str, prefix) == 0
}

// like strings.Index, except case insensitive
func IIndex(s string, substr string) int {
	return strings.Index(strings.ToLower(s), strings.ToLower(substr))
}

// matches the pattern against the text, case insensitively, returning a slice containing the whole match and any captures, or nil if there was no match
func IMatch(pat string, text string) []string {
	return regexp.MustCompile("(?i)" + pat).FindStringSubmatch(text)
}

// like strings.ReplaceAll, except case insensitive
func IReplaceAll(s, match, replace string) string {
	// TODO(go,core)
	return s
}

// returns true if the two slices contain the same case-insensitive strings (in any order)
func IStrsEq(a, b []string) bool {
	// this is true if a and b contain the same number of strings,
	// and if the intersection of the two sets contains all the strings from either side
	return len(a) == len(b) && len(IntersectFunc(a, b, strings.EqualFold)) == len(a)
}

func IStrsIndex(list []string, target string) int {
	return IndexOfFunc(list, target, strings.EqualFold)
}

func IStrsContains(list []string, target string) bool {
	return ContainsFunc(list, target, strings.EqualFold)
}

func IIntersectStrs(a, b []string) []string {
	return IntersectFunc(a, b, strings.EqualFold)
}

func IDifferenceStrs(a, b []string) []string {
	return DifferenceFunc(a, b, strings.EqualFold)
}
