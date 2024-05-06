package output

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"unicode"
)

func NewOutputFileSegmenter(log *slog.Logger, quoter Quoter, baseFileName string, startingFileSegment uint, statementLimit uint) OutputFileSegmenter {
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

func NewOutputFileSegmenterToFile(log *slog.Logger, quoter Quoter, baseFileName string, startingFileSegment uint, file *os.File, currentOutputFile string, statementLimit uint) OutputFileSegmenter {
	log.Info(fmt.Sprintf("[File Segment] Fixed output file: %s", currentOutputFile))
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
	log                  *slog.Logger
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

func (ofs *outputFileSegmenter) Close() error {
	// before we insist on writing the footer
	// if write was never called then the file segmenting has not initialized
	// and will blow up when write_footer() calls write()
	if !ofs.writeWasCalledEver {
		err := ofs.Write("\n")
		if err != nil {
			return err
		}
	}
	if err := ofs.writeFooter(); err != nil {
		return err
	}
	return ofs.file.Close()
}

func (ofs *outputFileSegmenter) SetHeader(stmt ToSql) error {
	ofs.contentHeader = stmt.ToSql(ofs.quoter)
	return nil
}

func (ofs *outputFileSegmenter) AppendHeader(stmt ToSql) error {
	ofs.contentHeader += stmt.ToSql(ofs.quoter)
	return nil
}

func (ofs *outputFileSegmenter) AppendFooter(stmt ToSql) error {
	ofs.contentFooter += stmt.ToSql(ofs.quoter)
	return nil
}

func (ofs *outputFileSegmenter) WriteSql(stmts ...ToSql) error {
	// TODO(go,nth) implement ALTER TABLE batching. might be tricky though because behavior might change per dialect?
	for _, stmt := range stmts {
		// make sure every sql statement ends with ;\n\n for consistency, and has no other leading/trailing whitespace
		sql := stmt.ToSql(ofs.quoter)
		sql = strings.TrimSpace(sql)
		if sql == "" {
			ofs.log.Warn(fmt.Sprintf("empty SQL string from %T", stmt))
			continue
		}
		sql = strings.TrimSuffix(sql, ";")
		if err := ofs.Write("%s", sql+";\n\n"); err != nil {
			return err
		}
	}
	return nil
}

func (ofs *outputFileSegmenter) MustWriteSql(stmts []ToSql, err error) {
	if err != nil {
		panic(err)
	}
	err = ofs.WriteSql(stmts...)
	if err != nil {
		panic(err)
	}
}

func (ofs *outputFileSegmenter) Write(format string, args ...interface{}) error {
	// do the next segment if the pointer has not been set
	// this is for first file header setup between set_header() / append_header() and write time
	if ofs.file == nil {
		err := ofs.nextFileSegment()
		if err != nil {
			return err
		}
	}

	// if this segmenter is using a fixed file pointer
	// need to do write_header() because next_file_segment() isn't going to get called
	if ofs.fixedFilePointer && !ofs.wroteFixedFileHeader {
		ofs.wroteFixedFileHeader = true
		err := ofs.writeHeader()
		if err != nil {
			return err
		}
	}

	text := fmt.Sprintf(format, args...)
	_, err := ofs.file.WriteString(text)
	if err != nil {
		return fmt.Errorf("[File Segment] Failed to write to file %s, text: %s: %w", ofs.file.Name(), text, err)
	}
	ofs.countStatements(text)
	err = ofs.checkStatementCount()
	if err != nil {
		return err
	}
	ofs.writeWasCalledEver = true
	return nil
}

func (ofs *outputFileSegmenter) nextFileSegment() error {
	if !ofs.segmentingEnabled {
		return fmt.Errorf("next_file_segment called while segmenting_enabled is false. base_file_name = %s", ofs.baseFileName)
	}
	if ofs.file != nil {
		ofs.writeFooter()
		if err := ofs.file.Close(); err != nil {
			return fmt.Errorf("[File Segment] while closing file: %w", err)
		}
		ofs.fileSegment += 1
	}
	ofs.currentOutputFile = fmt.Sprintf("%s%d.sql", ofs.baseFileName, ofs.fileSegment)
	ofs.log.Info(fmt.Sprintf("[File Segment] Opening output file segment %s", ofs.currentOutputFile))
	file, err := os.Create(ofs.currentOutputFile)
	if err != nil {
		return fmt.Errorf("[File Segment] while opening file: %w", err)
	}
	ofs.file = file
	err = ofs.writeHeader()
	if err != nil {
		return err
	}
	ofs.statementCount = 0
	return nil
}

func (ofs *outputFileSegmenter) writeHeader() error {
	return ofs.withoutSegmenting(func() error {
		err := ofs.Write("%s %s\n", CommentLinePrefix, ofs.currentOutputFile)
		if err != nil {
			return err
		}
		err = ofs.Write(ofs.contentHeader)
		if err != nil {
			return err
		}
		return nil
	})
}

func (ofs *outputFileSegmenter) writeFooter() error {
	return ofs.withoutSegmenting(func() error {
		return ofs.Write(ofs.contentFooter)
	})
}

func (ofs *outputFileSegmenter) withoutSegmenting(f func() error) error {
	se := ofs.segmentingEnabled
	ofs.segmentingEnabled = false
	// the defer ensures we're in a good state following a panic in f
	defer func() { ofs.segmentingEnabled = se }()
	return f()
}

func (ofs *outputFileSegmenter) countStatements(text string) {
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
			ofs.statementCount += 1
		}
	}
}

func (ofs *outputFileSegmenter) checkStatementCount() error {
	if !ofs.segmentingEnabled || ofs.statementLimit == 0 {
		return nil
	}
	if ofs.statementCount >= ofs.statementLimit {
		return ofs.nextFileSegment()
	}
	return nil
}
