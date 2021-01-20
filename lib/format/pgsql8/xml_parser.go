package pgsql8

import (
	"strconv"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/util"
)

type XmlParser struct {
}

type slonyRange struct {
	first int
	last  int
}

func tryNewSlonyRange(firstStr, lastStr string, parts int) *slonyRange {
	if firstStr == "" {
		if lastStr == "" {
			return nil
		}
		lib.GlobalDBSteward.Fatal("tablePartitionOption 'lastSlonyId' was provided but not 'firstSlonyId'")
	}

	first, err := strconv.Atoi(firstStr)
	lib.GlobalDBSteward.FatalIfError(err, "tablePartitionOption 'firstSlonyId' must be a number")

	last := first + parts - 1
	if lastStr != "" {
		lastTmp, err := strconv.Atoi(lastStr)
		lib.GlobalDBSteward.FatalIfError(err, "tablePartitionOption 'lastSlonyId' must be a number")
		allocated := lastTmp - first + 1
		if allocated != parts {
			lib.GlobalDBSteward.Fatal("Requested %d partitions but provided %d slony IDs", parts, allocated)
		}
		last = lastTmp
	}

	return &slonyRange{first, last}
}

func NewXmlParser() *XmlParser {
	return &XmlParser{}
}

func (self *XmlParser) Process(doc *model.Definition) {
	for _, schema := range doc.Schemas {
		for _, table := range schema.Tables {
			if table.Partitioning != nil {
				self.expandPartitionedTable(doc, schema, table)
			}
		}
	}
}

func (self *XmlParser) expandPartitionedTable(doc *model.Definition, schema *model.Schema, table *model.Table) {
	util.Assert(table.Partitioning != nil, "Table.Partitioning must not be nil")
	// TODO(feat) hash partitions
	// TODO(feat) native partitioning in recent postgres

	if table.Partitioning.Type.Equals(model.TablePartitionTypeModulo) {
		self.expandModuloParitionedTable(doc, schema, table)
		return
	}

	lib.GlobalDBSteward.Fatal("Invalid partition type: %s", table.Partitioning.Type)
}

func (self *XmlParser) CheckPartitionChange(oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) {
	util.Assert(oldTable.Partitioning != nil, "oldTable.Partitioning must not be nil")
	util.Assert(newTable.Partitioning != nil, "newTable.Partitioning must not be nil")

	if !oldTable.Partitioning.Type.Equals(newTable.Partitioning.Type) {
		lib.GlobalDBSteward.Fatal(
			"Changing partitioning types (%s -> %s) on table %s.%s is not supported",
			oldTable.Partitioning.Type, newTable.Partitioning.Type,
			newSchema.Name, newTable.Name,
		)
	}

	if newTable.Partitioning.Type.Equals(model.TablePartitionTypeModulo) {
		self.checkModuloPartitionChange(oldSchema, oldTable, newSchema, newTable)
		return
	}

	lib.GlobalDBSteward.Fatal("Invalid partition type: %s", newTable.Partitioning.Type)
}
