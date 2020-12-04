package format

import (
	"github.com/dbsteward/dbsteward/lib/format/pgsql8"
	"github.com/dbsteward/dbsteward/lib/xml"
)

type GeneralOperations interface {
	Build(outputPrefix string, dbDoc xml.DocumentTBD)
	BuildUpgrade(
		oldOutputPrefix string, oldCompositeFile string, oldDbDoc xml.DocumentTBD, oldFiles []string,
		newOutputPrefix string, newCompositeFile string, newDbDoc xml.DocumentTBD, newFiles []string,
	)
	ExtractSchema(host string, port uint, name, user, pass string) xml.DocumentTBD
	CompareDbData(dbDoc xml.DocumentTBD, host string, port uint, name, user, pass string) xml.DocumentTBD
	SqlDiff(old, new, outputFile string)
}

var GlobalGeneralOperations map[SqlFormat]GeneralOperations = map[SqlFormat]GeneralOperations{
	SqlFormatPgsql8: pgsql8.GlobalPgsql8,
}
