package util

import (
	"fmt"
	"strings"

	"golang.org/x/crypto/ssh/terminal"
)

// Assert that some condition is true. If it is false, a panic will be raised
// This should be used to assert invariants about the code, NOT for validation
// or general error reporting
func Assert(cond bool, msg string, args ...any) {
	if !cond {
		panic("Assertion Failed: " + fmt.Sprintf(msg, args...))
	}
}

// prompts user for input on the console, hiding input
func PromptPassword(prompt string, args ...any) (string, error) {
	fmt.Printf(prompt, args...)
	d, err := terminal.ReadPassword(0)
	fmt.Println() // ReadPassword does not print a new line after
	return string(d), err
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
