package pgsql8

import "github.com/dbsteward/dbsteward/lib/format"

var GlobalOperations = NewOperations()
var GlobalSchema = NewSchema()
var differ = newDiff()
var GlobalXmlParser = NewXmlParser()

var GlobalLookup = &format.Lookup{
	Operations: GlobalOperations,
	Schema:     GlobalSchema,
	XmlParser:  GlobalXmlParser,
}
