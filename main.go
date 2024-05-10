package main

import (
	"github.com/dbsteward/dbsteward/lib"
	_ "github.com/dbsteward/dbsteward/lib/format/pgsql8"
)

func main() {
	dbsteward := lib.NewDBSteward()
	dbsteward.ArgParse()
	dbsteward.Info("Done")
}
