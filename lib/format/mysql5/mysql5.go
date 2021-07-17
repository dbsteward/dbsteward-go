package mysql5

import "github.com/dbsteward/dbsteward/lib/format"

var GlobalOperations = NewOperations()
var GlobalSchema = NewSchema()
var GlobalDataType = NewDataType()
var GlobalFunction = NewFunction()
var GlobalTable = NewTable()
var GlobalColumn = NewColumn()
var GlobalSequence = NewSequence()
var GlobalTrigger = NewTrigger()
var GlobalView = NewView()
var GlobalDiff = NewDiff()
var GlobalDiffConstraints = NewDiffConstraints()
var GlobalDiffFunctions = NewDiffFunctions()
var GlobalDiffSequences = NewDiffSequences()
var GlobalDiffTables = NewDiffTables()
var GlobalDiffTriggers = NewDiffTriggers()
var GlobalDiffTypes = NewDiffTypes()
var GlobalDiffViews = NewDiffViews()

var GlobalLookup = &format.Lookup{
	Operations: GlobalOperations,
}
