package format

import "github.com/dbsteward/dbsteward/lib/ir"

type LookupMap map[ir.SqlFormat]*Lookup

type Lookup struct {
	Operations Operations
	Schema     Schema
}
