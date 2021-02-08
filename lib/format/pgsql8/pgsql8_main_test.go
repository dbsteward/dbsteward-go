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
	lib.GlobalDBSteward = lib.NewDBSteward(format.LookupMap{
		model.SqlFormatPgsql8: pgsql8.GlobalLookup,
	})
	lib.GlobalDBSteward.SqlFormat = model.SqlFormatPgsql8
	os.Exit(m.Run())
}
