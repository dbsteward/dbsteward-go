package pgsql8

import (
	"os"
	"testing"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format"
	"github.com/dbsteward/dbsteward/lib/ir"
)

func TestMain(m *testing.M) {
	resetGlobalDBSteward()
	os.Exit(m.Run())
}

func setOldNewDocs(old, new *ir.Definition) {
	lib.GlobalDBSteward.OldDatabase = old
	lib.GlobalDBSteward.NewDatabase = new
	if old != nil {
		GlobalDiff.OldTableDependency = lib.GlobalDBX.TableDependencyOrder(old)
	}
	if new != nil {
		GlobalDiff.NewTableDependency = lib.GlobalDBX.TableDependencyOrder(new)
	}
}

func resetGlobalDBSteward() {
	lib.GlobalDBSteward = lib.NewDBSteward(format.LookupMap{
		ir.SqlFormatPgsql8: GlobalLookup,
	})
	lib.GlobalDBSteward.SqlFormat = ir.SqlFormatPgsql8
}
