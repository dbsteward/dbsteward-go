package format

import (
	"github.com/dbsteward/dbsteward/lib/config"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Operations interface {
	Build(outputPrefix string, dbDoc *ir.Definition)
	BuildUpgrade(
		oldOutputPrefix, oldCompositeFile string, oldDbDoc *ir.Definition, oldFiles []string,
		newOutputPrefix, newCompositeFile string, newDbDoc *ir.Definition, newFiles []string,
	)
	ExtractSchema(host string, port uint, name, user, pass string) *ir.Definition
	CompareDbData(dbDoc *ir.Definition, host string, port uint, name, user, pass string) *ir.Definition
	SqlDiff(old, new []string, outputFile string)

	GetQuoter() output.Quoter
	SetConfig(*config.Args)
}

type SlonyOperations interface {
	SlonyCompare(file string)
	SlonyDiff(oldFile, newFile string)
}

type Schema interface {
	GetCreationSql(*ir.Schema) []output.ToSql
	GetDropSql(*ir.Schema) []output.ToSql
}

type Index interface {
	BuildPrimaryKeyName(string) string
	BuildForeignKeyName(string, string) string
}

type Diff interface {
	DiffDoc(oldFile, newFile string, oldDoc, newDoc *ir.Definition, upgradePrefix string)
	DiffDocWork(stage1, stage2, stage3, stage4 output.OutputFileSegmenter)

	DropOldSchemas(output.OutputFileSegmenter)
	CreateNewSchemas(output.OutputFileSegmenter)
}
