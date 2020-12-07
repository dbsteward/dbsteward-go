package lib

import (
	"os"
)

type ToSql interface {
	ToSql() string
}

type OutputFileSegmenter interface {
	Write(format string, args ...interface{})
	WriteSql(...ToSql)
}

func NewOutputFileSegmenter(baseFileName string, startingFileSegment uint, filePointer *os.File, currentOutputFile string) OutputFileSegmenter {
	// TODO(go,core)
	return nil
}
