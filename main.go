package main

import (
	"github.com/dbsteward/dbsteward/lib"
)

func main() {
	// correlates to bin/dbsteward
	dbsteward := lib.GlobalDBSteward
	dbsteward.ArgParse()
}
