package main

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format"
	"github.com/dbsteward/dbsteward/lib/format/mssql10"
	"github.com/dbsteward/dbsteward/lib/format/mysql5"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8"
	"github.com/dbsteward/dbsteward/lib/ir"
)

func main() {
	// correlates to bin/dbsteward
	lib.GlobalDBSteward = lib.NewDBSteward(format.LookupMap{
		ir.SqlFormatPgsql8:  pgsql8.GlobalLookup,
		ir.SqlFormatMysql5:  mysql5.GlobalLookup,
		ir.SqlFormatMssql10: mssql10.GlobalLookup,
	})
	lib.GlobalDBSteward.ArgParse()
	lib.GlobalDBSteward.Notice("Done")
}
