package lib

import (
	"github.com/dbsteward/dbsteward/lib/model"
)

type FormatOperations interface {
	Build(outputPrefix string, dbDoc *model.Definition)
	BuildUpgrade(
		oldOutputPrefix, oldCompositeFile string, oldDbDoc *model.Definition, oldFiles []string,
		newOutputPrefix, newCompositeFile string, newDbDoc *model.Definition, newFiles []string,
	)
	ExtractSchema(host string, port uint, name, user, pass string) *model.Definition
	CompareDbData(dbDoc *model.Definition, host string, port uint, name, user, pass string) *model.Definition
	SqlDiff(old, new []string, outputFile string)
}

type FormatOperationMap = map[model.SqlFormat]FormatOperations

type SlonyOperations interface {
	SlonyCompare(file string)
	SlonyDiff(oldFile, newFile string)
}
