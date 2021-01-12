package pgsql8

import "github.com/dbsteward/dbsteward/lib/format"

var GlobalOperations = NewOperations()
var GlobalColumn = NewColumn()
var GlobalConstraint = NewConstraint()
var GlobalFunction = NewFunction()
var GlobalIndex = NewIndex()
var GlobalLanguage = NewLanguage()
var GlobalPermission = NewPermission()
var GlobalSchema = NewSchema()
var GlobalSequence = NewSequence()
var GlobalTable = NewTable()
var GlobalTrigger = NewTrigger()
var GlobalDataType = NewDataType()
var GlobalView = NewView()
var GlobalDiffConstraints = NewDiffConstraints()
var GlobalDiffFunctions = NewDiffFunctions()
var GlobalDiffIndexes = NewDiffIndexes()
var GlobalDiffLanguages = NewDiffLanguages()
var GlobalDiffSequences = NewDiffSequences()
var GlobalDiffTables = NewDiffTables()
var GlobalDiffTriggers = NewDiffTriggers()
var GlobalDiffTypes = NewDiffTypes()
var GlobalDiffViews = NewDiffViews()
var GlobalDiff = NewDiff()

var GlobalLookup = &format.Lookup{
	Operations: GlobalOperations,
}
