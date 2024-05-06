package pgsql8

import "github.com/dbsteward/dbsteward/lib/format"

var GlobalSchema = NewSchema()
var GlobalXmlParser = NewXmlParser()

var GlobalLookup = &format.Lookup{
	Schema:                GlobalSchema,
	OperationsConstructor: NewOperations,
}
