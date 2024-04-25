package pgsql8

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

func getCreateIndexSql(schema *ir.Schema, table *ir.Table, index *ir.Index) []output.ToSql {
	dims := make([]sql.Quotable, len(index.Dimensions))
	for i, dim := range index.Dimensions {
		if dim.Sql {
			dims[i] = &sql.DoNotQuote{Text: dim.Value}
		} else {
			dims[i] = &sql.QuoteObject{Ident: dim.Value}
		}
	}
	condStr := ""
	if cond := index.TryGetCondition(ir.SqlFormatPgsql8); cond != nil {
		condStr = cond.Condition
	}
	return []output.ToSql{
		&sql.IndexCreate{
			Table:        sql.TableRef{Schema: schema.Name, Table: table.Name},
			Index:        index.Name,
			Unique:       index.Unique,
			Concurrently: index.Concurrently,
			Using:        string(index.Using),
			Dimensions:   dims,
			Where:        condStr,
		},
	}
}

func getDropIndexSql(schema *ir.Schema, index *ir.Index) []output.ToSql {
	return []output.ToSql{
		&sql.IndexDrop{
			Index: sql.IndexRef{Schema: schema.Name, Index: index.Name},
		},
	}
}

func getTableIndexes(schema *ir.Schema, table *ir.Table) ([]*ir.Index, error) {
	if table == nil {
		return nil, nil
	}
	out := make([]*ir.Index, len(table.Indexes))
	copy(out, table.Indexes)

	// add column unique indexes to the list
	for _, column := range table.Columns {
		if column.Unique {
			out = append(out, &ir.Index{
				Name:   buildSecondaryKeyName(table.Name, column.Name),
				Unique: true,
				Using:  ir.IndexTypeBtree, // TODO(feat) can these support other types?
				Dimensions: []*ir.IndexDim{{
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
			return out, fmt.Errorf("duplicate index name %s on table %s.%s", index.Name, schema.Name, table.Name)
		} else {
			names.Add(index.Name)
		}
	}

	return out, nil
}

func tryGetTableIndexNamed(schema *ir.Schema, table *ir.Table, name string) (*ir.Index, error) {
	indexes, err := getTableIndexes(schema, table)
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

func buildPrimaryKeyName(table string) string {
	// primary key name does not use a column
	return buildIndexName(table, "", "pkey")
}

func buildSecondaryKeyName(table, column string) string {
	return buildIndexName(table, column, "key")
}

func buildForeignKeyName(table, column string) string {
	return buildIndexName(table, column, "fkey")
}

func buildIndexName(table, column, suffix string) string {
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
