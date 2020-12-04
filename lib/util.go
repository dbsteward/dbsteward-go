package lib

import (
	"io/ioutil"
	"os"
	"strings"
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
