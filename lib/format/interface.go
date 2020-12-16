package format

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Operations interface {
	GetQuoteChar() string
	IsIllegalIdentifier(string) bool
	IsReservedIdentifier(string) bool
	GetQuotedName(name string, shouldQuote bool) string
	GetQuotedColumnName(name string) string
}

type Schema interface {
	GetCreationSql(*model.Schema) []output.ToSql
	GetDropSql(*model.Schema) []output.ToSql
}

type Diff interface {
	DiffDoc(oldFile, newFile string, oldDoc, newDoc *model.Definition, upgradePrefix string)
	DiffDocWork(stage1, stage2, stage3, stage4 output.OutputFileSegmenter)

	DropOldSchemas(output.OutputFileSegmenter)
	CreateNewSchemas(output.OutputFileSegmenter)
}
