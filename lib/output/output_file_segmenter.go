package output

import (
	"os"
)

type ToSql interface {
	ToSql() string
}

type OutputFileSegmenter interface {
	SetHeader(format string, args ...interface{})
	AppendHeader(format string, args ...interface{})
	AppendFooter(format string, args ...interface{})
	Write(format string, args ...interface{})
	WriteSql(...ToSql)
}

func NewOutputFileSegmenter(baseFileName string, startingFileSegment uint) OutputFileSegmenter {
	// TODO(go,core)
	return nil
}

func NewOutputFileSegmenterToFile(baseFileName string, startingFileSegment uint, filePointer *os.File, currentOutputFile string) OutputFileSegmenter {
	// TODO(go,core)
	return nil
}
