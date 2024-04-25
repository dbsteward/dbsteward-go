package pgsql8

import "github.com/dbsteward/dbsteward/lib/format"

var GlobalOperations = NewOperations()
var GlobalSchema = NewSchema()
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
var GlobalXmlParser = NewXmlParser()

var GlobalLookup = &format.Lookup{
	Operations: GlobalOperations,
	Schema:     GlobalSchema,
	XmlParser:  GlobalXmlParser,
}
