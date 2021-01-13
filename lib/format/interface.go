package format

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Operations interface {
	Build(outputPrefix string, dbDoc *model.Definition)
	BuildUpgrade(
		oldOutputPrefix, oldCompositeFile string, oldDbDoc *model.Definition, oldFiles []string,
		newOutputPrefix, newCompositeFile string, newDbDoc *model.Definition, newFiles []string,
	)
	ExtractSchema(host string, port uint, name, user, pass string) *model.Definition
	CompareDbData(dbDoc *model.Definition, host string, port uint, name, user, pass string) *model.Definition
	SqlDiff(old, new []string, outputFile string)

	SetContextReplicaSetId(*int)

	GetQuoteChar() string
	IsIllegalIdentifier(string) bool
	IsReservedIdentifier(string) bool
	GetQuotedName(name string, shouldQuote bool) string

	output.Quoter
}

type XmlParser interface {
	Process(*model.Definition)
}

type SlonyOperations interface {
	SlonyCompare(file string)
	SlonyDiff(oldFile, newFile string)
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
