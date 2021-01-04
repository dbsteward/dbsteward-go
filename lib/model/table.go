package model

import (
	"fmt"
	"strings"
)

type Table struct {
	Name           string         `xml:"name,attr"`
	Description    string         `xml:"description,attr"`
	Owner          string         `xml:"owner,attr"`
	PrimaryKey     DelimitedList  `xml:"primaryKey,attr"`
	PrimaryKeyName string         `xml:"primaryKeyName,attr"`
	SlonySetId     int            `xml:"slonySetId,attr"`
	TableOptions   []*TableOption `xml:"tableOption"`
	Columns        []*Column      `xml:"column"`
	ForeignKeys    []*ForeignKey  `xml:"foreignKey"`
	Indexes        []*Index       `xml:"index"`
	Constraints    []*Constraint  `xml:"constraint"`
	Grants         []*Grant       `xml:"grant"`
	Rows           *DataRows      `xml:"rows"`
}

type Column struct {
	Name            string `xml:"name,attr"`
	Type            string `xml:"type,attr"`
	Nullable        bool   `xml:"null,attr"` // TODO(go,core) this means it will default to being NOT NULL, need to validate usages!
	Default         string `xml:"default,attr"`
	Description     string `xml:"description,attr"`
	SerialStart     string `xml:"serialStart,attr"`
	ForeignSchema   string `xml:"foreignSchema,attr"`
	ForeignTable    string `xml:"foreignTable,attr"`
	ForeignColumn   string `xml:"foreignColumn,attr"`
	ForeignKeyName  string `xml:"foreignKeyName,attr"`
	ForeignOnUpdate string `xml:"foreignOnUpdate,attr"`
	ForeignOnDelete string `xml:"foreignOnDelete,attr"`
	Statistics      *int   `xml:"statistics,attr"` // TODO(feat) this doesn't show up in the DTD
}

type TableOption struct {
	SqlFormat SqlFormat `xml:"sqlFormat,attr"`
	Name      string    `xml:"name"`
	Value     string    `xml:"value"`
}

type Index struct {
	Name       string      `xml:"name,attr"`
	Using      string      `xml:"using,attr"`
	Unique     bool        `xml:"unique,attr"`
	Dimensions []*IndexDim `xml:"indexDimension"`
}

type IndexDim struct {
	Name  string `xml:"name,attr"`
	Value string `xml:",chardata"`
}

type Constraint struct {
	Name       string `xml:"name,attr"`
	Type       string `xml:"type,attr"`
	Definition string `xml:"definition,attr"`
}

type ForeignKey struct {
	Columns        DelimitedList `xml:"columns,attr"`
	ForeignSchema  string        `xml:"foreignSchema,attr"`
	ForeignTable   string        `xml:"foreignTable,attr"`
	ForeignColumns DelimitedList `xml:"foreignColumns,attr"`
	ConstraintName string        `xml:"constraintName,attr"`
	IndexName      string        `xml:"indexName,attr"`
	OnUpdate       string        `xml:"onUpdate,attr"`
	OnDelete       string        `xml:"onDelete,attr"`
}

func (self *Table) HasDefaultNextVal() bool {
	// TODO(go,core)
	return false
}

func (self *Table) TryGetTableOptionMatching(target *TableOption) *TableOption {
	for _, opt := range self.TableOptions {
		if opt.IdentityMatches(target) {
			return opt
		}
	}
	return nil
}

func (self *Table) SetTableOption(sqlFormat SqlFormat, name, value string) {
	// TODO(feat) sanity check
	self.AddTableOption(&TableOption{
		SqlFormat: sqlFormat,
		Name:      name,
		Value:     value,
	})
}

func (self *Table) AddTableOption(opt *TableOption) {
	// TODO(feat) sanity check
	self.TableOptions = append(self.TableOptions, opt)
}

func (self *Table) GetGrantsForRole(role string) []*Grant {
	out := []*Grant{}
	for _, grant := range self.Grants {
		if grant.Role == role {
			out = append(out, grant)
		}
	}
	return out
}

func (self *Table) AddGrant(grant *Grant) {
	// TODO(feat) sanity check
	self.Grants = append(self.Grants, grant)
}

func (self *Table) TryGetColumnNamed(name string) *Column {
	for _, column := range self.Columns {
		// TODO(feat) case insensitivity?
		if column.Name == name {
			return column
		}
	}
	return nil
}

func (self *Table) AddColumn(col *Column) {
	// TODO(feat) sanity check
	self.Columns = append(self.Columns, col)
}
func (self *Table) TryGetIndexMatching(target *Index) *Index {
	for _, index := range self.Indexes {
		if index.IdentityMatches(target) {
			return index
		}
	}
	return nil
}
func (self *Table) AddIndex(index *Index) {
	// TODO(feat) sanity check
	self.Indexes = append(self.Indexes, index)
}
func (self *Table) AddForeignKey(col *ForeignKey) {
	// TODO(feat) sanity check
	self.ForeignKeys = append(self.ForeignKeys, col)
}

func (self *Index) AddDimensionNamed(name, value string) {
	// TODO(feat) sanity check
	self.Dimensions = append(self.Dimensions, &IndexDim{
		Name:  name,
		Value: value,
	})
}

func (self *Index) AddDimension(value string) {
	self.AddDimensionNamed(
		fmt.Sprintf("%s_%d", self.Name, len(self.Dimensions)+1),
		value,
	)
}

func (self *Table) TryGetConstraintMatching(target *Constraint) *Constraint {
	for _, constraint := range self.Constraints {
		if constraint.IdentityMatches(target) {
			return constraint
		}
	}
	return nil
}

func (self *Table) AddConstraint(constraint *Constraint) {
	// TODO(feat) sanity check
	self.Constraints = append(self.Constraints, constraint)
}

func (self *Table) Merge(overlay *Table) {
	if overlay == nil {
		return
	}

	self.Description = overlay.Description
	self.Owner = overlay.Owner
	self.PrimaryKey = overlay.PrimaryKey
	self.PrimaryKeyName = overlay.PrimaryKeyName
	self.SlonySetId = overlay.SlonySetId

	for _, overlayOpt := range overlay.TableOptions {
		if baseOpt := self.TryGetTableOptionMatching(overlayOpt); baseOpt != nil {
			baseOpt.Merge(overlayOpt)
		} else {
			self.AddTableOption(overlayOpt)
		}
	}

	for _, overlayCol := range overlay.Columns {
		if baseCol := self.TryGetColumnNamed(overlayCol.Name); baseCol != nil {
			baseCol.Merge(overlayCol)
		} else {
			self.AddColumn(overlayCol)
		}
	}

	// TODO(go,core) this (I think) differs from the OG algorithm, need to thoroughly test
	// just replace foreignkeys outright, because there's so many different ways we could possibly identify/merge them
	self.ForeignKeys = overlay.ForeignKeys

	for _, overlayIndex := range overlay.Indexes {
		if baseIndex := self.TryGetIndexMatching(overlayIndex); baseIndex != nil {
			baseIndex.Merge(overlayIndex)
		} else {
			self.AddIndex(overlayIndex)
		}
	}

	for _, overlayConstraint := range overlay.Constraints {
		if baseConstraint := self.TryGetConstraintMatching(overlayConstraint); baseConstraint != nil {
			baseConstraint.Merge(overlayConstraint)
		} else {
			self.AddConstraint(overlayConstraint)
		}
	}

	for _, overlayGrant := range overlay.Grants {
		self.AddGrant(overlayGrant)
	}

	self.MergeDataRows(overlay.Rows)
}

func (self *Table) MergeDataRows(overlay *DataRows) {
	// TODO(go,core) data addendum stuff
	// TODO(go,core) impl from xml_parser::data_rows_overlay()
}

func (self *TableOption) IdentityMatches(other *TableOption) bool {
	if other == nil {
		return false
	}
	return self.SqlFormat.Equals(other.SqlFormat) && strings.EqualFold(self.Name, other.Name)
}

func (self *TableOption) Merge(overlay *TableOption) {
	// TODO(feat) does this need to be more sophisticated given that sometimes we set name=with,value=<lots of things>?
	self.Value = overlay.Value
}

func (self *Column) Merge(overlay *Column) {
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

func (self *Index) IdentityMatches(other *Index) bool {
	if other == nil {
		return false
	}
	return strings.EqualFold(self.Name, other.Name)
}

func (self *Index) Merge(overlay *Index) {
	if overlay == nil {
		return
	}
	self.Using = overlay.Using
	self.Unique = overlay.Unique
	self.Dimensions = overlay.Dimensions
}

func (self *Constraint) IdentityMatches(other *Constraint) bool {
	if other == nil {
		return false
	}
	return strings.EqualFold(self.Name, other.Name)
}

func (self *Constraint) Merge(overlay *Constraint) {
	if overlay == nil {
		return
	}
	self.Type = overlay.Type
	self.Definition = overlay.Definition
}
