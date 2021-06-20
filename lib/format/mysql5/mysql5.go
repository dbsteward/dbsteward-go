package mysql5

import "github.com/dbsteward/dbsteward/lib/format"

var GlobalOperations = NewOperations()
var GlobalLookup = &format.Lookup{
	Operations: GlobalOperations,
}
