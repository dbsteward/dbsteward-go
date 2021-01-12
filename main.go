package main

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8"
	"github.com/dbsteward/dbsteward/lib/model"
)

func main() {
	// correlates to bin/dbsteward
	lib.GlobalDBSteward = lib.NewDBSteward(format.LookupMap{
		model.SqlFormatPgsql8: pgsql8.GlobalLookup,
	})
	lib.GlobalDBSteward.ArgParse()
	lib.GlobalDBSteward.Notice("Done")
}
