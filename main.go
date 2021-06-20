package main

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format"
	"github.com/dbsteward/dbsteward/lib/format/mssql10"
	"github.com/dbsteward/dbsteward/lib/format/mysql5"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8"
	"github.com/dbsteward/dbsteward/lib/model"
)

func main() {
	// correlates to bin/dbsteward
	lib.GlobalDBSteward = lib.NewDBSteward(format.LookupMap{
		model.SqlFormatPgsql8:  pgsql8.GlobalLookup,
		model.SqlFormatMysql5:  mysql5.GlobalLookup,
		model.SqlFormatMssql10: mssql10.GlobalLookup,
	})
	lib.GlobalDBSteward.ArgParse()
	lib.GlobalDBSteward.Notice("Done")
}
