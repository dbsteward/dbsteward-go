package model

import (
	"fmt"
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
	Nullable        bool   `xml:"null,attr"`
	Default         string `xml:"default,attr"`
	Description     string `xml:"description,attr"`
	SerialStart     string `xml:"serialStart,attr"`
	ForeignSchema   string `xml:"foreignSchema,attr"`
	ForeignTable    string `xml:"foreignTable,attr"`
	ForeignColumn   string `xml:"foreignColumn,attr"`
	ForeignKeyName  string `xml:"foreignKeyName,attr"`
	ForeignOnUpdate string `xml:"foreignOnUpdate,attr"`
	ForeignOnDelete string `xml:"foreignOnDelete,attr"`
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
	OnUpdate       string        `xml:"onUpdate,attr"`
	OnDelete       string        `xml:"onDelete,attr"`
}

func (self *Table) HasDefaultNextVal() bool {
	// TODO(go,core)
	return false
}

func (self *Table) SetTableOption(sqlFormat SqlFormat, name, value string) {
	// TODO(feat) sanity check
	self.TableOptions = append(self.TableOptions, &TableOption{
		SqlFormat: sqlFormat,
		Name:      name,
		Value:     value,
	})
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

func (self *Table) AddConstraint(constraint *Constraint) {
	// TODO(feat) sanity check
	self.Constraints = append(self.Constraints, constraint)
}
