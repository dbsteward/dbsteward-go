package pgsql8

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

var GlobalIndex = NewIndex()

type Index struct {
}

func NewIndex() *Index {
	return &Index{}
}

func (self *Index) GetCreateSql(schema *model.Schema, table *model.Table, index *model.Index) []output.ToSql {
	// TODO(go,pgsql)
	return nil
}

func (self *Index) GetDropSql(schema *model.Schema, table *model.Table, index *model.Index) []output.ToSql {
	// TODO(go,pgsql)
	return nil
}

func (self *Index) GetTableIndexes(table *model.Table) []*model.Index {
	if table == nil {
		return nil
	}
	out := []*model.Index{}
	copy(out, table.Indexes)

	// add column unique indexes to the list
	for _, column := range table.Columns {
		if column.Unique {
			out = append(out, &model.Index{
				Name:   self.BuildSecondaryKeyName(table.Name, column.Name),
				Unique: true,
				Using:  model.IndexTypeBtree,
				Dimensions: []*model.IndexDim{{
					Name:  column.Name + "_unq",
					Value: column.Name,
				}},
			})
		}
	}

	return out
}

func (self *Index) TryGetTableIndexNamed(table *model.Table, name string) *model.Index {
	for _, index := range self.GetTableIndexes(table) {
		if strings.EqualFold(index.Name, name) {
			return index
		}
	}
	return nil
}

func (self *Index) BuildPrimaryKeyName(table string) string {
	// primary key name does not use a column
	return self.buildIndexName(table, "", "pkey")
}

func (self *Index) BuildSecondaryKeyName(table, column string) string {
	return self.buildIndexName(table, column, "key")
}

func (self *Index) BuildForeignKeyName(table, column string) string {
	return self.buildIndexName(table, column, "fkey")
}

func (self *Index) buildIndexName(table, column, suffix string) string {
	// TODO(feat) what happens with compound indexes?
	// TODO(go,nth) can we merge this with Operations.buildIdentifierName?
	tableLen := len(table)
	columnLen := len(column)
	suffixLen := len(suffix)

	// figure out how to build "table_column_suffix"

	// reserve space for the suffix, at least one underscore
	maxlen := MAX_IDENT_LENGTH - suffixLen - 1
	if columnLen > 0 {
		// if there's a column, add another underscore
		maxlen -= 1
	}

	tableMax := util.IntCeil(maxlen, 2)
	columnMax := util.IntFloor(maxlen, 2)

	if tableLen > tableMax && columnLen < columnMax {
		// table is longer than max, but column is shorter
		// give table the extra room from column
		tableMax += columnMax - columnLen
	} else if tableLen < tableMax && columnLen > columnMax {
		// table is shorter than max but column is longer
		// give column the extra room from table
		columnMax += tableMax - tableLen
	}

	table = table[0:util.IntMin(tableMax, tableLen)]
	column = column[0:util.IntMin(columnMax, columnLen)]

	if columnLen > 0 {
		return fmt.Sprintf("%s_%s_%s", table, column, suffix)
	}
	return fmt.Sprintf("%s_%s", table, suffix)
}
