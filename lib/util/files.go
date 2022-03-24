package util

import (
	"os"
	"path"
	"strings"
)

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
	return os.WriteFile(file, []byte(content), 0644)
}

func Basename(file string, ext string) string {
	return strings.TrimSuffix(path.Base(file), ext)
}
