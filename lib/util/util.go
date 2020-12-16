package util

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strings"

	"golang.org/x/crypto/ssh/terminal"
)

type QualifiedTable struct {
	Schema string
	Table  string
}

func ParseQualifiedTableName(table string) QualifiedTable {
	if strings.Contains(table, ".") {
		parts := strings.SplitN(table, ".", 2)
		return QualifiedTable{parts[0], parts[1]}
	}
	return QualifiedTable{"public", table}
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

// returns the first non-empty string, or the empty string
func CoalesceStr(strs ...string) string {
	for _, s := range strs {
		if len(s) > 0 {
			return s
		}
	}
	return ""
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
