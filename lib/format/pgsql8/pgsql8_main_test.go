package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/ir"
)

func setOldNewDocs(conf lib.Config, differ *diff, old, new *ir.Definition) lib.Config {
	conf.OldDatabase = old
	conf.NewDatabase = new
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
	return conf
}
