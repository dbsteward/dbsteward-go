package pgsql8_test

import (
	"os"
	"testing"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8"
	"github.com/dbsteward/dbsteward/lib/model"
)

func TestMain(m *testing.M) {
	resetGlobalDBSteward()
	os.Exit(m.Run())
}

func setOldNewDocs(old, new *model.Definition) {
	lib.GlobalDBSteward.OldDatabase = old
	lib.GlobalDBSteward.NewDatabase = new
	if old != nil {
		pgsql8.GlobalDiff.OldTableDependency = lib.GlobalDBX.TableDependencyOrder(old)
	}
	if new != nil {
		pgsql8.GlobalDiff.NewTableDependency = lib.GlobalDBX.TableDependencyOrder(new)
	}
}

func resetGlobalDBSteward() {
	lib.GlobalDBSteward = lib.NewDBSteward(format.LookupMap{
		model.SqlFormatPgsql8: pgsql8.GlobalLookup,
	})
	lib.GlobalDBSteward.SqlFormat = model.SqlFormatPgsql8
}
