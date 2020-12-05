package format

import (
	"github.com/dbsteward/dbsteward/lib/format/pgsql8"
	"github.com/dbsteward/dbsteward/lib/model"
)

type GeneralOperations interface {
	Build(outputPrefix string, dbDoc *model.Definition)
	BuildUpgrade(
		oldOutputPrefix string, oldCompositeFile string, oldDbDoc *model.Definition, oldFiles []string,
		newOutputPrefix string, newCompositeFile string, newDbDoc *model.Definition, newFiles []string,
	)
	ExtractSchema(host string, port uint, name, user, pass string) *model.Definition
	CompareDbData(dbDoc *model.Definition, host string, port uint, name, user, pass string) *model.Definition
	SqlDiff(old, new, outputFile string)
}

var GlobalGeneralOperations map[SqlFormat]GeneralOperations = map[SqlFormat]GeneralOperations{
	SqlFormatPgsql8: pgsql8.GlobalPgsql8,
}
