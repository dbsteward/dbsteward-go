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

func setOldNewDocs(differ *diff, old, new *ir.Definition) {
	lib.GlobalDBSteward.OldDatabase = old
	lib.GlobalDBSteward.NewDatabase = new
	var err error
	if old != nil {
		differ.OldTableDependency, err = old.TableDependencyOrder()
		if err != nil {
			panic(err)
		}
	}
	if new != nil {
		differ.NewTableDependency, err = new.TableDependencyOrder()
		if err != nil {
			panic(err)
		}
	}
}

func resetGlobalDBSteward() {
	lib.GlobalDBSteward = lib.NewDBSteward(format.LookupMap{
		ir.SqlFormatPgsql8: GlobalLookup,
	})
	lib.GlobalDBSteward.SqlFormat = ir.SqlFormatPgsql8
}
