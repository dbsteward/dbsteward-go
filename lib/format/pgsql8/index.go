package pgsql8

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"

	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

type Index struct {
}

func NewIndex() *Index {
	return &Index{}
}

func (self *Index) GetCreateSql(schema *model.Schema, table *model.Table, index *model.Index) []output.ToSql {
	dims := make([]sql.Quotable, len(index.Dimensions))
	for i, dim := range index.Dimensions {
		if dim.Sql {
			dims[i] = &sql.DoNotQuote{dim.Value}
		} else {
			dims[i] = &sql.QuoteObject{dim.Value}
		}
	}
	condStr := ""
	if cond := index.TryGetCondition(model.SqlFormatPgsql8); cond != nil {
		condStr = cond.Condition
	}
	return []output.ToSql{
		&sql.IndexCreate{
			Table:        sql.TableRef{schema.Name, table.Name},
			Index:        index.Name,
			Unique:       index.Unique,
			Concurrently: index.Concurrently,
			Using:        string(index.Using),
			Dimensions:   dims,
			Where:        condStr,
		},
	}
}

func (self *Index) GetDropSql(schema *model.Schema, table *model.Table, index *model.Index) []output.ToSql {
	return []output.ToSql{
		&sql.IndexDrop{
			Index: sql.IndexRef{schema.Name, table.Name},
		},
	}
}

func (self *Index) GetTableIndexes(schema *model.Schema, table *model.Table) ([]*model.Index, error) {
	if table == nil {
		return nil, nil
	}
	out := make([]*model.Index, len(table.Indexes))
	copy(out, table.Indexes)

	// add column unique indexes to the list
	for _, column := range table.Columns {
		if column.Unique {
			out = append(out, &model.Index{
				Name:   self.BuildSecondaryKeyName(table.Name, column.Name),
				Unique: true,
				Using:  model.IndexTypeBtree, // TODO(feat) can these support other types?
				Dimensions: []*model.IndexDim{{
					Name:  column.Name + "_unq",
					Value: column.Name,
				}},
			})
		}
	}

	// validate that there are no duplicate index names
	// TODO(go,3) move this validation elsewhere
	names := util.NewSet(util.IdentityId[string])
	for _, index := range out {
		if names.Has(index.Name) {
			return out, fmt.Errorf("Duplicate index name %s on table %s.%s", index.Name, schema.Name, table.Name)
		} else {
			names.Add(index.Name)
		}
	}

	return out, nil
}

func (self *Index) TryGetTableIndexNamed(schema *model.Schema, table *model.Table, name string) (*model.Index, error) {
	indexes, err := self.GetTableIndexes(schema, table)
	if err != nil {
		return nil, err
	}
	for _, index := range indexes {
		if strings.EqualFold(index.Name, name) {
			return index, nil
		}
	}
	return nil, nil
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

	table = table[0:util.Min(tableMax, tableLen)]
	column = column[0:util.Min(columnMax, columnLen)]

	if columnLen > 0 {
		return fmt.Sprintf("%s_%s_%s", table, column, suffix)
	}
	return fmt.Sprintf("%s_%s", table, suffix)
}
