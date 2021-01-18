package model

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/dbsteward/dbsteward/lib/util"
)

type Table struct {
	Name           string          `xml:"name,attr"`
	Description    string          `xml:"description,attr,omitempty"`
	Owner          string          `xml:"owner,attr,omitempty"`
	PrimaryKey     DelimitedList   `xml:"primaryKey,attr,omitempty"`
	PrimaryKeyName string          `xml:"primaryKeyName,attr,omitempty"`
	InheritsTable  string          `xml:"inheritsTable,attr,omitempty"`
	InheritsSchema string          `xml:"inheritsSchema,attr,omitempty"`
	OldTableName   string          `xml:"oldTableName,attr,omitempty"`
	OldSchemaName  string          `xml:"oldSchemaName,attr,omitempty"`
	SlonySetId     *int            `xml:"slonySetId,attr,omitempty"`
	SlonyId        *int            `xml:"slonyId,attr,omitempty"`
	TableOptions   []*TableOption  `xml:"tableOption"`
	Partitioning   *TablePartition `xml:"tablePartition"`
	Columns        []*Column       `xml:"column"`
	ForeignKeys    []*ForeignKey   `xml:"foreignKey"`
	Indexes        []*Index        `xml:"index"`
	Constraints    []*Constraint   `xml:"constraint"`
	Grants         []*Grant        `xml:"grant"`
	Rows           *DataRows       `xml:"rows"`
}

type TableOption struct {
	SqlFormat SqlFormat `xml:"sqlFormat,attr"`
	Name      string    `xml:"name"`
	Value     string    `xml:"value"`
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

func (self *Table) GetTableOptions(sqlFormat SqlFormat) []*TableOption {
	out := make([]*TableOption, 0, len(self.TableOptions))
	for _, opt := range self.TableOptions {
		if opt.SqlFormat.Equals(sqlFormat) {
			out = append(out, opt)
		}
	}
	return out
}

func (self *Table) GetTableOptionStrMap(sqlFormat SqlFormat) map[string]string {
	out := map[string]string{}
	for _, opt := range self.GetTableOptions(sqlFormat) {
		out[opt.Name] = opt.Value
	}
	return out
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
		if util.IIndexOfStr(role, grant.Roles) >= 0 {
			out = append(out, grant)
		}
	}
	return out
}

func (self *Table) GetGrants() []*Grant {
	return self.Grants
}

func (self *Table) AddGrant(grant *Grant) {
	// TODO(feat) sanity check
	self.Grants = append(self.Grants, grant)
}

func (self *Table) GetColumnNamed(name string) (*Column, error) {
	var found *Column
	for _, col := range self.Columns {
		// TODO(feat) case insensitivity?
		if col.Name == name {
			if found == nil {
				found = col
			} else {
				return found, errors.Errorf("Found a second column named %s", name)
			}
		}
	}
	if found == nil {
		return nil, errors.Errorf("Found no columns named %s", name)
	}
	return found, nil
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

func (self *Table) TryGetColumnOldNamed(oldName string) *Column {
	for _, column := range self.Columns {
		if strings.EqualFold(column.OldColumnName, oldName) {
			return column
		}
	}
	return nil
}

func (self *Table) TryGetColumnsNamed(names []string) ([]*Column, bool) {
	out := make([]*Column, len(names))
	ok := true
	for i, name := range names {
		out[i] = self.TryGetColumnNamed(name)
		if out[i] == nil {
			ok = false
		}
	}
	return out, ok
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
	// TODO(go,core) impl from xml_parser::data_rows_overlay(); should this maybe go in XmlParser instead?
}

func (self *TableOption) IdentityMatches(other *TableOption) bool {
	if self == nil || other == nil {
		return false
	}
	return self.SqlFormat.Equals(other.SqlFormat) && strings.EqualFold(self.Name, other.Name)
}

func (self *TableOption) Equals(other *TableOption) bool {
	return self.IdentityMatches(other) && strings.EqualFold(self.Value, other.Value)
}

func (self *TableOption) Merge(overlay *TableOption) {
	// TODO(feat) does this need to be more sophisticated given that sometimes we set name=with,value=<lots of things>?
	self.Value = overlay.Value
}

type TableRef struct {
	Schema *Schema
	Table  *Table
}

func (self TableRef) String() string {
	return fmt.Sprintf("%s.%s", self.Schema.Name, self.Table.Name)
}
