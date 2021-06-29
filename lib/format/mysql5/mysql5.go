package mysql5

import "github.com/dbsteward/dbsteward/lib/format"

var GlobalOperations = NewOperations()
var GlobalSchema = NewSchema()
var GlobalDataType = NewDataType()
var GlobalFunction = NewFunction()
var GlobalTable = NewTable()
var GlobalSequence = NewSequence()
var GlobalTrigger = NewTrigger()
var GlobalView = NewView()
var GlobalDiffConstraints = NewDiffConstraints()
var GlobalDiffViews = NewDiffViews()

var GlobalLookup = &format.Lookup{
	Operations: GlobalOperations,
}
