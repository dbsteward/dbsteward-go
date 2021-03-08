package model

import (
	"encoding/xml"
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/util"
)

type Column struct {
	Name             string           `xml:"name,attr,omitempty"`
	Type             string           `xml:"type,attr,omitempty"`
	Nullable         bool             `xml:"null,attr"` // TODO(go,nth) find a way to omit this when true
	Default          string           `xml:"default,attr,omitempty"`
	Description      string           `xml:"description,attr,omitempty"`
	Unique           bool             `xml:"unique,attr,omitempty"`
	Check            string           `xml:"check,attr,omitempty"`
	SerialStart      *int             `xml:"serialStart,attr,omitempty"`
	OldColumnName    string           `xml:"oldColumnName,attr,omitempty"`
	ConvertUsing     string           `xml:"convertUsing,attr,omitempty"`
	ForeignSchema    string           `xml:"foreignSchema,attr,omitempty"`
	ForeignTable     string           `xml:"foreignTable,attr,omitempty"`
	ForeignColumn    string           `xml:"foreignColumn,attr,omitempty"`
	ForeignKeyName   string           `xml:"foreignKeyName,attr,omitempty"`
	ForeignIndexName string           `xml:"foreignIndexName,attr,omitempty"`
	ForeignOnUpdate  ForeignKeyAction `xml:"foreignOnUpdate,attr,omitempty"`
	ForeignOnDelete  ForeignKeyAction `xml:"foreignOnDelete,attr,omitempty"`
	Statistics       *int             `xml:"statistics,attr,omitempty"` // TODO(feat) this doesn't show up in the DTD
	BeforeAddStage1  string           `xml:"beforeAddStage1,attr,omitempty"`
	AfterAddStage1   string           `xml:"afterAddStage1,attr,omitempty"`
	BeforeAddStage2  string           `xml:"beforeAddStage2,attr,omitempty"`
	AfterAddStage2   string           `xml:"afterAddStage2,attr,omitempty"`
	BeforeAddStage3  string           `xml:"beforeAddStage3,attr,omitempty"`
	AfterAddStage3   string           `xml:"afterAddStage3,attr,omitempty"`

	// These are DEPRECATED, replaced by Before/AfterAddStageN. see ConvertStageDirectives
	AfterAddPreStage1  string `xml:"afterAddPreStage1,attr,omitempty"`
	AfterAddPostStage1 string `xml:"afterAddPostStage1,attr,omitempty"`
	AfterAddPreStage2  string `xml:"afterAddPreStage2,attr,omitempty"`
	AfterAddPostStage2 string `xml:"afterAddPostStage2,attr,omitempty"`
	AfterAddPreStage3  string `xml:"afterAddPreStage3,attr,omitempty"`
	AfterAddPostStage3 string `xml:"afterAddPostStage3,attr,omitempty"`
}

// Implement some custom unmarshalling behavior
func (self *Column) UnmarshalXML(decoder *xml.Decoder, start xml.StartElement) error {
	type colAlias Column // prevents recursion while decoding, as type aliases have no methods
	// set defaults
	col := &colAlias{
		Nullable: true, // as in SQL NULL
	}
	err := decoder.DecodeElement(col, &start)
	if err != nil {
		return err
	}
	*self = Column(*col)
	return nil
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
// Very similar to normal Equals, but foreign key names are allowed to be different
func (self *Column) EqualsInherited(other *Column) bool {
	if self == nil || other == nil {
		return false
	}
	return strings.EqualFold(self.Name, other.Name) &&
		strings.EqualFold(self.Type, other.Type) &&
		self.Nullable == other.Nullable &&
		self.Default == other.Default &&
		self.Description == other.Description &&
		self.SerialStart == other.SerialStart &&
		strings.EqualFold(self.ForeignSchema, other.ForeignSchema) &&
		strings.EqualFold(self.ForeignTable, other.ForeignTable) &&
		self.ForeignOnUpdate.Equals(other.ForeignOnUpdate) &&
		self.ForeignOnDelete.Equals(other.ForeignOnDelete) &&
		util.IntpEq(self.Statistics, other.Statistics)
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
