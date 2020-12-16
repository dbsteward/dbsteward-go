package main

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8"
)

func main() {
	// correlates to bin/dbsteward
	lib.GlobalDBSteward = lib.NewDBSteward(map[format.SqlFormat]lib.FormatOperations{
		format.SqlFormatPgsql8: pgsql8.GlobalOperations,
	})
	lib.GlobalDBSteward.ArgParse()
}
