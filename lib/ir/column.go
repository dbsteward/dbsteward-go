package ir

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/util"
)

type Column struct {
	Name             string
	Type             string
	Nullable         bool
	Default          string
	Description      string
	Unique           bool
	Check            string
	SerialStart      *int
	OldColumnName    string
	ConvertUsing     string
	ForeignSchema    string
	ForeignTable     string
	ForeignColumn    string
	ForeignKeyName   string
	ForeignIndexName string
	ForeignOnUpdate  ForeignKeyAction
	ForeignOnDelete  ForeignKeyAction
	Statistics       *int
	BeforeAddStage1  string
	AfterAddStage1   string
	BeforeAddStage2  string
	AfterAddStage2   string
	BeforeAddStage3  string
	AfterAddStage3   string

	// These are DEPRECATED, replaced by Before/AfterAddStageN. see ConvertStageDirectives
	AfterAddPreStage1  string
	AfterAddPostStage1 string
	AfterAddPreStage2  string
	AfterAddPostStage2 string
	AfterAddPreStage3  string
	AfterAddPostStage3 string
}

func (col *Column) ConvertStageDirectives() {
	col.BeforeAddStage1 = util.CoalesceStr(col.BeforeAddStage1, col.AfterAddPreStage1)
	col.AfterAddStage1 = util.CoalesceStr(col.AfterAddStage1, col.AfterAddPostStage1)
	col.BeforeAddStage2 = util.CoalesceStr(col.BeforeAddStage2, col.AfterAddPreStage2)
	col.AfterAddStage2 = util.CoalesceStr(col.AfterAddStage2, col.AfterAddPostStage2)
	col.BeforeAddStage3 = util.CoalesceStr(col.BeforeAddStage3, col.AfterAddPreStage3)
	col.AfterAddStage3 = util.CoalesceStr(col.AfterAddStage3, col.AfterAddPostStage3)

	col.AfterAddPreStage1 = ""
	col.AfterAddPostStage1 = ""
	col.AfterAddPreStage2 = ""
	col.AfterAddPostStage2 = ""
	col.AfterAddPreStage3 = ""
	col.AfterAddPostStage3 = ""
}

func (col *Column) HasForeignKey() bool {
	return col.ForeignTable != ""
}

func (col *Column) TryGetReferencedKey() *KeyNames {
	if !col.HasForeignKey() {
		return nil
	}

	key := col.GetReferencedKey()
	return &key
}

func (col *Column) GetReferencedKey() KeyNames {
	util.Assert(col.HasForeignKey(), "GetReferencedKey without checking HasForeignKey")
	return KeyNames{
		Schema:  col.ForeignSchema,
		Table:   col.ForeignTable,
		Columns: []string{col.ForeignColumn},
		KeyName: col.ForeignKeyName,
	}
}

func (col *Column) Merge(overlay *Column) {
	// TODO(go,core) slony, migration sql
	col.Type = overlay.Type
	col.Nullable = overlay.Nullable
	col.Default = overlay.Default
	col.Description = overlay.Description
	col.SerialStart = overlay.SerialStart
	col.ForeignSchema = overlay.ForeignSchema
	col.ForeignTable = overlay.ForeignTable
	col.ForeignKeyName = overlay.ForeignKeyName
	col.ForeignOnUpdate = overlay.ForeignOnUpdate
	col.ForeignOnDelete = overlay.ForeignOnDelete
	col.Statistics = overlay.Statistics
}

func (col *Column) Validate(_ *Definition, s *Schema, t *Table) []error {
	var errs []error
	if col.Name == "" {
		errs = append(errs, fmt.Errorf("column in %s.%s has empty name", s.Name, t.Name))
	}
	// TODO(go,3) validate values
	// TODO(go,3) validate foreign references, remove other codepaths
	// TODO(go,3) validate oldname references, remove other codepaths
	return errs
}

func (col *Column) IdentityMatches(other *Column) bool {
	if col == nil || other == nil {
		return false
	}
	// TODO(feat) case sensitivity
	return strings.EqualFold(col.Name, other.Name)
}

// Returns true if this column appears to match the other for inheritance
// Very similar to normal Equals, but with a few exceptions:
// - Foreign key names may be different
// - Descriptions may be different
func (col *Column) EqualsInherited(other *Column) bool {
	if col == nil || other == nil {
		return false
	}
	return strings.EqualFold(col.Name, other.Name) &&
		strings.EqualFold(col.Type, other.Type) &&
		col.Nullable == other.Nullable &&
		col.Default == other.Default &&
		col.SerialStart == other.SerialStart &&
		strings.EqualFold(col.ForeignSchema, other.ForeignSchema) &&
		strings.EqualFold(col.ForeignTable, other.ForeignTable) &&
		col.ForeignOnUpdate.Equals(other.ForeignOnUpdate) &&
		col.ForeignOnDelete.Equals(other.ForeignOnDelete) &&
		util.PtrEq(col.Statistics, other.Statistics)
}

type ColumnRef struct {
	Schema *Schema
	Table  *Table
	Column *Column
}

func (colref ColumnRef) String() string {
	schema := "<nil>"
	if colref.Schema != nil {
		schema = colref.Schema.Name
	}
	table := "<nil>"
	if colref.Table != nil {
		table = colref.Table.Name
	}
	column := "<nil>"
	if colref.Column != nil {
		column = colref.Column.Name
	}
	return fmt.Sprintf("%s.%s.%s", schema, table, column)
}
