package lib

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
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

// a more familiar name
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

func InArrayStr(target string, list []string) bool {
	for _, el := range list {
		if el == target {
			return true
		}
	}
	return false
}

func IndexOfStr(target string, list []string) int {
	for i, el := range list {
		if el == target {
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
