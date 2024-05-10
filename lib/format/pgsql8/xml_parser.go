package pgsql8

import (
	"fmt"
	"log/slog"
	"strconv"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
	"github.com/pkg/errors"
)

type XmlParser struct {
	quoter output.Quoter
}

type slonyRange struct {
	first int
	last  int
}

func tryNewSlonyRange(firstStr, lastStr string, parts int) (*slonyRange, error) {
	if firstStr == "" {
		if lastStr == "" {
			return nil, nil
		}
		return nil, fmt.Errorf("tablePartitionOption 'lastSlonyId' was provided but not 'firstSlonyId'")
	}

	first, err := strconv.Atoi(firstStr)
	if err != nil {
		return nil, fmt.Errorf("tablePartitionOption 'firstSlonyId' must be a number: %w", err)
	}

	last := first + parts - 1
	if lastStr != "" {
		lastTmp, err := strconv.Atoi(lastStr)
		if err != nil {
			return nil, fmt.Errorf("tablePartitionOption 'lastSlonyId' must be a number: %w", err)
		}
		allocated := lastTmp - first + 1
		if allocated != parts {
			return nil, fmt.Errorf("requested %d partitions but provided %d slony IDs", parts, allocated)
		}
		last = lastTmp
	}

	return &slonyRange{first, last}, nil
}

func NewXmlParser(quoter output.Quoter) *XmlParser {
	return &XmlParser{quoter: quoter}
}

func (parser *XmlParser) Process(l *slog.Logger, doc *ir.Definition) error {
	for _, schema := range doc.Schemas {
		for _, table := range schema.Tables {
			if table.Partitioning != nil {
				l.Warn(fmt.Sprintf("Table %s.%s definies partition which is only partially supported at this time", schema.Name, table.Name))
				return parser.expandPartitionedTable(doc, schema, table)
			}
		}
	}
	return nil
}

func (parser *XmlParser) expandPartitionedTable(doc *ir.Definition, schema *ir.Schema, table *ir.Table) error {
	util.Assert(table.Partitioning != nil, "Table.Partitioning must not be nil")
	// TODO(feat) hash partitions
	// TODO(feat) native partitioning in recent postgres

	if table.Partitioning.Type.Equals(ir.TablePartitionTypeModulo) {
		return parser.expandModuloParitionedTable(doc, schema, table)
	}

	return fmt.Errorf("invalid partition type: %s", table.Partitioning.Type)
}

func (parser *XmlParser) CheckPartitionChange(oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) error {
	util.Assert(oldTable.Partitioning != nil, "oldTable.Partitioning must not be nil")
	util.Assert(newTable.Partitioning != nil, "newTable.Partitioning must not be nil")

	if !oldTable.Partitioning.Type.Equals(newTable.Partitioning.Type) {
		return errors.Errorf(
			"Changing partitioning types (%s -> %s) on table %s.%s is not supported",
			oldTable.Partitioning.Type, newTable.Partitioning.Type,
			newSchema.Name, newTable.Name,
		)
	}

	if newTable.Partitioning.Type.Equals(ir.TablePartitionTypeModulo) {
		return parser.checkModuloPartitionChange(oldSchema, oldTable, newSchema, newTable)
	}

	return errors.Errorf("Invalid partition type: %s", newTable.Partitioning.Type)
}
