package util

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/crypto/ssh/terminal"
)

// Assert that some condition is true. If it is false, a panic will be raised
// This should be used to assert invariants about the code, NOT for validation
// or general error reporting
func Assert(cond bool, msg string, args ...interface{}) {
	if !cond {
		panic("Assertion Failed: " + fmt.Sprintf(msg, args...))
	}
}

// TODO(go,nth) DEPRECATED just use strings.EqualFold instead
func Stricmp(a, b string) bool {
	return strings.EqualFold(a, b)
}

// returns true if the path exists and is a directory,
// false if it does not exist or is a file
func IsDir(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

func WriteFile(content string, file string) error {
	return ioutil.WriteFile(file, []byte(content), 0644)
}

func Basename(file string, ext string) string {
	return strings.TrimSuffix(path.Base(file), ext)
}

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

func ParseKV(str, fieldSep, keySep string) map[string]string {
	out := map[string]string{}
	for _, field := range strings.Split(str, fieldSep) {
		kv := strings.Split(field, keySep)
		if len(kv) == 1 {
			out[kv[0]] = ""
		} else {
			out[kv[0]] = kv[1]
		}
	}
	return out
}

func EncodeKV(kv map[string]string, fieldSep, keySep string) string {
	parts := make([]string, 0, len(kv))
	for k, v := range kv {
		parts = append(parts, k+keySep+v)
	}
	return strings.Join(parts, fieldSep)
}

// TODO(go,nth) DEPRECATED just use IndexOfStr instead
func InArrayStr(target string, list []string) bool {
	return IndexOfStr(target, list) >= 0
}

func IndexOfStr(target string, list []string) int {
	for i, el := range list {
		if el == target {
			return i
		}
	}
	return -1
}

func IIndexOfStr(target string, list []string) int {
	for i, el := range list {
		if strings.EqualFold(el, target) {
			return i
		}
	}
	return -1
}

// prompts user for input on the console, hiding input
func PromptPassword(prompt string) (string, error) {
	fmt.Printf("Password: ")
	d, err := terminal.ReadPassword(0)
	return string(d), err
}

// returns true if str starts with the given prefix, case insensitively
func IHasPrefix(str, prefix string) bool {
	return IIndex(str, prefix) == 0
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

// like strings.Index, except case insensitive
func IIndex(s string, substr string) int {
	return strings.Index(strings.ToLower(s), strings.ToLower(substr))
}

// case insensitively unions two lists of strings
// TODO(go,nth) optimize this using a map implementation
func IUnionStrs(left, right []string) []string {
	// no, this is not the most efficient, but it is the simplest
	out := make([]string, len(left))
	copy(out, left)
	for _, r := range right {
		if IIndexOfStr(r, out) < 0 {
			out = append(out, r)
		}
	}
	return out
}

// case insensitively intersects two lists of strings
// only returns the strings that are present in both lists
// will use the values from the left side, in the case that the values differ in case
// if a string is present multiple times in a list, it will be duplicated in the output
// TODO(go,nth) optimize this using a map implementation
func IIntersectStrs(left, right []string) []string {
	// no, this is not the most efficient, but it is the simplest
	out := []string{}
	for _, l := range left {
		for _, r := range right {
			if strings.EqualFold(l, r) {
				out = append(out, l)
			}
		}
	}
	return out
}

// case insensitively removes any strings from `right` from the `left` list
// TODO(go,nth) optimize this using a map implementation
func IDifferenceStrs(left, right []string) []string {
	out := []string{}
outer:
	for _, l := range left {
		for _, r := range right {
			if strings.EqualFold(l, r) {
				continue outer
			}
		}
		out = append(out, l)
	}
	return out
}

// returns a map containing keys from left that are not in right, by case-insensitive key equality
func IDifferenceStrMapKeys(left, right map[string]string) map[string]string {
	return DifferenceStrMapFunc(left, right, strings.EqualFold)
}

// returns a map containing keys from left that are not in right, using a custom key-equality function
func DifferenceStrMapFunc(left, right map[string]string, equals func(string, string) bool) map[string]string {
	out := map[string]string{}
	for l, lv := range left {
		inRight := false
		for r := range right {
			if equals(l, r) {
				inRight = true
				break
			}
		}
		if !inRight {
			out[l] = lv
		}
	}
	return out
}

// returns keys from left that are also in right, using a custom key-equality function
func IntersectStrMapFunc(left, right map[string]string, equals func(string, string) bool) map[string]string {
	out := map[string]string{}
	for l, lv := range left {
		for r := range right {
			if equals(l, r) {
				out[l] = lv
				break
			}
		}
	}
	return out
}

// returns keys from left and right, using case-insensitive key equality, preferring values from left
// this means union({foo: 1}, {FOO: 2}) => {foo: 1}
func IUnionStrMapKeys(left, right map[string]string) map[string]string {
	return UnionStrMapFunc(left, right, strings.EqualFold)
}

func UnionStrMapFunc(left, right map[string]string, equals func(string, string) bool) map[string]string {
	out := map[string]string{}
	for l, lv := range left {
		out[l] = lv
	}
	for r, rv := range right {
		found := false
		for o := range out {
			if equals(r, o) {
				found = true
				break
			}
		}
		if !found {
			out[r] = rv
		}
	}
	return out
}

func StrMapKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// returns true if the string explicitly represents a "true" value.
// TODO(go,nth) search for cases of testing a value equal to one of these and replace
func IsTruthy(s string) bool {
	switch strings.ToLower(s) {
	case "t", "true", "yes", "1":
		return true
	default:
		return false
	}
}

// returns true if the string explicitly represents a "false" value
// TODO(go,nth) search for cases of testing a value equal to one of these and replace
func IsFalsey(s string) bool {
	switch strings.ToLower(s) {
	case "f", "false", "no", "0":
		return true
	default:
		return false
	}
}

// returns "true" if string explicitly represents a true value
func NormalizeTruthyBoolStr(s string) string {
	if IsTruthy(s) {
		return "true"
	}
	return "false"
}

func IntMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func IntCeil(num, div int) int {
	return int(math.Ceil(float64(num) / float64(div)))
}

func IntFloor(num, div int) int {
	return int(float64(num) / float64(div))
}

func MustParseInt(val string) int {
	v, err := strconv.Atoi(val)
	if err != nil {
		panic(err)
	}
	return v
}

func Intp(val int) *int {
	return &val
}

func NumDigits(x int) int {
	return int(math.Log10(float64(x)) + 1)
}
