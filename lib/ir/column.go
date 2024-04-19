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

func (self *Column) ConvertStageDirectives() {
	self.BeforeAddStage1 = util.CoalesceStr(self.BeforeAddStage1, self.AfterAddPreStage1)
	self.AfterAddStage1 = util.CoalesceStr(self.AfterAddStage1, self.AfterAddPostStage1)
	self.BeforeAddStage2 = util.CoalesceStr(self.BeforeAddStage2, self.AfterAddPreStage2)
	self.AfterAddStage2 = util.CoalesceStr(self.AfterAddStage2, self.AfterAddPostStage2)
	self.BeforeAddStage3 = util.CoalesceStr(self.BeforeAddStage3, self.AfterAddPreStage3)
	self.AfterAddStage3 = util.CoalesceStr(self.AfterAddStage3, self.AfterAddPostStage3)

	self.AfterAddPreStage1 = ""
	self.AfterAddPostStage1 = ""
	self.AfterAddPreStage2 = ""
	self.AfterAddPostStage2 = ""
	self.AfterAddPreStage3 = ""
	self.AfterAddPostStage3 = ""
}

func (self *Column) HasForeignKey() bool {
	return self.ForeignTable != ""
}

func (self *Column) TryGetReferencedKey() *KeyNames {
	if !self.HasForeignKey() {
		return nil
	}

	key := self.GetReferencedKey()
	return &key
}

func (self *Column) GetReferencedKey() KeyNames {
	util.Assert(self.HasForeignKey(), "GetReferencedKey without checking HasForeignKey")
	return KeyNames{
		Schema:  self.ForeignSchema,
		Table:   self.ForeignTable,
		Columns: []string{self.ForeignColumn},
		KeyName: self.ForeignKeyName,
	}
}

func (self *Column) Merge(overlay *Column) {
	// TODO(go,core) slony, migration sql
	self.Type = overlay.Type
	self.Nullable = overlay.Nullable
	self.Default = overlay.Default
	self.Description = overlay.Description
	self.SerialStart = overlay.SerialStart
	self.ForeignSchema = overlay.ForeignSchema
	self.ForeignTable = overlay.ForeignTable
	self.ForeignKeyName = overlay.ForeignKeyName
	self.ForeignOnUpdate = overlay.ForeignOnUpdate
	self.ForeignOnDelete = overlay.ForeignOnDelete
	self.Statistics = overlay.Statistics
}

func (self *Column) Validate(*Definition, *Schema, *Table) []error {
	// TODO(go,3) validate values
	// TODO(go,3) validate foreign references, remove other codepaths
	// TODO(go,3) validate oldname references, remove other codepaths
	return nil
}

func (self *Column) IdentityMatches(other *Column) bool {
	if self == nil || other == nil {
		return false
	}
	// TODO(feat) case sensitivity
	return strings.EqualFold(self.Name, other.Name)
}

// Returns true if this column appears to match the other for inheritance
// Very similar to normal Equals, but with a few exceptions:
// - Foreign key names may be different
// - Descriptions may be different
func (self *Column) EqualsInherited(other *Column) bool {
	if self == nil || other == nil {
		return false
	}
	return strings.EqualFold(self.Name, other.Name) &&
		strings.EqualFold(self.Type, other.Type) &&
		self.Nullable == other.Nullable &&
		self.Default == other.Default &&
		self.SerialStart == other.SerialStart &&
		strings.EqualFold(self.ForeignSchema, other.ForeignSchema) &&
		strings.EqualFold(self.ForeignTable, other.ForeignTable) &&
		self.ForeignOnUpdate.Equals(other.ForeignOnUpdate) &&
		self.ForeignOnDelete.Equals(other.ForeignOnDelete) &&
		util.PtrEq(self.Statistics, other.Statistics)
}

type ColumnRef struct {
	Schema *Schema
	Table  *Table
	Column *Column
}

func (self ColumnRef) String() string {
	schema := "<nil>"
	if self.Schema != nil {
		schema = self.Schema.Name
	}
	table := "<nil>"
	if self.Table != nil {
		table = self.Table.Name
	}
	column := "<nil>"
	if self.Column != nil {
		column = self.Column.Name
	}
	return fmt.Sprintf("%s.%s.%s", schema, table, column)
}
