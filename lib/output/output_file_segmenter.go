package output

import (
	"fmt"
	"os"
	"strings"
	"unicode"

	"github.com/dbsteward/dbsteward/lib/util"
)

const CommentLinePrefix = "--"

type ToSql interface {
	ToSql(Quoter) string
}

type Quoter interface {
	QuoteSchema(schema string) string
	QuoteTable(table string) string
	QuoteColumn(column string) string
	QuoteRole(role string) string
	QualifyTable(schema string, table string) string
	QualifyColumn(schema string, table string, column string) string
	LiteralString(str string) string
	LiteralStringEscaped(str string) string
}

type OutputFileSegmenter interface {
	Close()
	SetHeader(format string, args ...interface{})
	AppendHeader(format string, args ...interface{})
	AppendFooter(format string, args ...interface{})
	Write(format string, args ...interface{})
	WriteSql(...ToSql)
}

func NewOutputFileSegmenter(log util.Logger, quoter Quoter, baseFileName string, startingFileSegment uint, statementLimit uint) OutputFileSegmenter {
	return &outputFileSegmenter{
		log:               log,
		quoter:            quoter,
		baseFileName:      baseFileName,
		fileSegment:       startingFileSegment,
		file:              nil,
		currentOutputFile: "",
		statementCount:    0,
		segmentingEnabled: true,
		fixedFilePointer:  false,
		statementLimit:    statementLimit,
	}
}

func NewOutputFileSegmenterToFile(log util.Logger, quoter Quoter, baseFileName string, startingFileSegment uint, file *os.File, currentOutputFile string, statementLimit uint) OutputFileSegmenter {
	log.Notice("[File Segment] Fixed output file: %s", currentOutputFile)
	return &outputFileSegmenter{
		log:               log,
		quoter:            quoter,
		baseFileName:      baseFileName,
		fileSegment:       startingFileSegment,
		file:              file,
		currentOutputFile: currentOutputFile,
		statementCount:    0,
		segmentingEnabled: false,
		fixedFilePointer:  true,
		statementLimit:    statementLimit,
	}
}

type outputFileSegmenter struct {
	log                  util.Logger
	quoter               Quoter
	baseFileName         string
	fileSegment          uint
	file                 *os.File
	currentOutputFile    string
	statementCount       uint
	segmentingEnabled    bool
	fixedFilePointer     bool
	wroteFixedFileHeader bool
	statementLimit       uint
	contentHeader        string
	contentFooter        string
	writeWasCalledEver   bool
}

func (self *outputFileSegmenter) Close() {
	// before we insist on writing the footer
	// if write was never called then the file segmenting has not initialized
	// and will blow up when write_footer() calls write()
	if !self.writeWasCalledEver {
		self.Write("\n")
	}
	self.writeFooter()
	err := self.file.Close()
	self.log.ErrorIfError(err, "[File Segment] While closing file")
}

func (self *outputFileSegmenter) SetHeader(format string, args ...interface{}) {
	self.contentHeader = fmt.Sprintf(format, args...)
}

func (self *outputFileSegmenter) AppendHeader(format string, args ...interface{}) {
	self.contentHeader += fmt.Sprintf(format, args...)
}

func (self *outputFileSegmenter) AppendFooter(format string, args ...interface{}) {
	self.contentFooter += fmt.Sprintf(format, args...)
}

func (self *outputFileSegmenter) WriteSql(stmts ...ToSql) {
	for _, stmt := range stmts {
		self.Write(stmt.ToSql(self.quoter))
	}
}

func (self *outputFileSegmenter) Write(format string, args ...interface{}) {
	// do the next segment if the pointer has not been set
	// this is for first file header setup between set_header() / append_header() and write time
	if self.file == nil {
		self.nextFileSegment()
	}

	// if this segmenter is using a fixed file pointer
	// need to do write_header() because next_file_segment() isn't going to get called
	if self.fixedFilePointer && !self.wroteFixedFileHeader {
		self.wroteFixedFileHeader = true
		self.writeHeader()
	}

	text := fmt.Sprintf(format, args...)
	_, err := self.file.WriteString(text)
	self.log.FatalIfError(err, "[File Segment] Failed to write to file %s, text: %s", self.file.Name(), text)
	self.countStatements(text)
	self.checkStatementCount()
	self.writeWasCalledEver = true
}

func (self *outputFileSegmenter) nextFileSegment() {
	if !self.segmentingEnabled {
		self.log.Fatal("next_file_segment called while segmenting_enabled is false. base_file_name = %s", self.baseFileName)
	}
	if self.file != nil {
		self.writeFooter()
		self.log.ErrorIfError(self.file.Close(), "[File Segment] while closing file")
		self.fileSegment += 1
	}
	self.currentOutputFile = fmt.Sprintf("%s%d.sql", self.baseFileName, self.fileSegment)
	self.log.Notice("[File Segment] Opening output file segment %s", self.currentOutputFile)
	file, err := os.Create(self.currentOutputFile)
	self.log.FatalIfError(err, "[File Segment] while opening file")
	self.file = file
	self.writeHeader()
	self.statementCount = 0
}

func (self *outputFileSegmenter) writeHeader() {
	self.withoutSegmenting(func() {
		self.Write("%s %s\n", CommentLinePrefix, self.currentOutputFile)
		self.Write(self.contentHeader)
	})
}

func (self *outputFileSegmenter) writeFooter() {
	self.withoutSegmenting(func() {
		self.Write(self.contentFooter)
	})
}

func (self *outputFileSegmenter) withoutSegmenting(f func()) {
	se := self.segmentingEnabled
	self.segmentingEnabled = false
	// the defer ensures we're in a good state following a panic in f
	defer func() { self.segmentingEnabled = se }()
	f()
}

func (self *outputFileSegmenter) countStatements(text string) {
	// TODO(feat) is this method adequate?
	for _, line := range strings.Split(text, "\n") {
		// strip comments off end of line
		idx := strings.Index(line, "--")
		if idx >= 0 {
			line = line[0:idx]
		}
		// kill whitespace at end of line
		line = strings.TrimRightFunc(line, unicode.IsSpace)
		// does line end in semicolon?
		if strings.HasSuffix(line, ";") {
			self.statementCount += 1
		}
	}
}

func (self *outputFileSegmenter) checkStatementCount() {
	if !self.segmentingEnabled || self.statementLimit == 0 {
		return
	}
	if self.statementCount >= self.statementLimit {
		self.nextFileSegment()
	}
}
