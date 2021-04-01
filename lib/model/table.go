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
	ClusterIndex   string          `xml:"clusterIndex,attr,omitempty"`
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

func (self *Table) GetOwner() string {
	return self.Owner
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

func (self *Table) GetTableOptionStrMap(sqlFormat SqlFormat) *util.OrderedMap {
	opts := self.GetTableOptions(sqlFormat)
	out := util.NewOrderedMapOfSize(len(opts))
	for _, opt := range opts {
		out.Insert(opt.Name, opt.Value)
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
	// TODO(feat) case insensitivity?
	// TODO(go,3) case sensitivity & quoting
	return self.TryGetColumnNamedCase(name, false)
}

func (self *Table) TryGetColumnNamedCase(name string, caseSensitive bool) *Column {
	eq := strings.EqualFold
	if caseSensitive {
		eq = func(a, b string) bool { return a == b }
	}
	for _, column := range self.Columns {
		if eq(column.Name, name) {
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

func (self *Table) RemoveColumn(target *Column) {
	newCols := make([]*Column, 0, len(self.Columns)-1)
	for _, col := range self.Columns {
		if col != target {
			newCols = append(newCols, col)
		}
	}
	self.Columns = newCols
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

// TODO(go,nth) replace other table name matches with IdentityMatches where possible
// TODO(go,nth) replace schema.TryGetTableNamed with TryGetTableMatching where possible
func (self *Table) IdentityMatches(other *Table) bool {
	if self == nil || other == nil {
		return false
	}

	// TODO(feat) case sensitivity based on engine+quotedness
	// TODO(feat) take schema into account
	return strings.EqualFold(self.Name, other.Name)
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

func (self *Table) Validate(doc *Definition, schema *Schema) []error {
	// TODO(go,3) check owner, remove from other codepaths
	// TODO(go,3) validate grants, remove from other codepaths
	// TODO(go,3) validate primary key, remove from other codepaths
	// TODO(go,3) validate data rows, remove from other codepaths
	// TODO(go,3) validate oldname references, remove from other codepaths
	// TODO(go,3) validate inheritance references, remove from other codepaths

	out := []error{}

	// no two objects should have same identity (also, validate sub-objects)
	for i, tableOption := range self.TableOptions {
		out = append(out, tableOption.Validate(doc, schema, self)...)
		for _, other := range self.TableOptions[i+1:] {
			if tableOption.IdentityMatches(other) {
				out = append(out, fmt.Errorf("found two tableOptions in table %s.%s with name %q", schema.Name, self.Name, tableOption.Name))
			}
		}
	}
	for i, column := range self.Columns {
		out = append(out, column.Validate(doc, schema, self)...)
		for _, other := range self.Columns[i+1:] {
			if column.IdentityMatches(other) {
				out = append(out, fmt.Errorf("found two columns in table %s.%s with name %q", schema.Name, self.Name, column.Name))
			}
		}
	}
	for i, foreignKey := range self.ForeignKeys {
		out = append(out, foreignKey.Validate(doc, schema, self)...)
		for _, other := range self.ForeignKeys[i+1:] {
			if foreignKey.IdentityMatches(other) {
				out = append(out, fmt.Errorf("found two foreignKeys in table %s.%s with constraint name %q", schema.Name, self.Name, foreignKey.ConstraintName))
			}
		}
	}
	for i, index := range self.Indexes {
		out = append(out, index.Validate(doc, schema, self)...)
		for _, other := range self.Indexes[i+1:] {
			if index.IdentityMatches(other) {
				out = append(out, fmt.Errorf("found two indexes in table %s.%s with name %q", schema.Name, self.Name, index.Name))
			}
		}
	}
	for i, constraint := range self.Constraints {
		out = append(out, constraint.Validate(doc, schema, self)...)
		for _, other := range self.Constraints[i+1:] {
			if constraint.IdentityMatches(other) {
				out = append(out, fmt.Errorf("found two constraints in table %s.%s with name %q", schema.Name, self.Name, constraint.Name))
			}
		}
	}

	return out
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

func (self *TableOption) Validate(*Definition, *Schema, *Table) []error {
	// TODO(go,3) validate values
	return nil
}

type TableRef struct {
	Schema *Schema
	Table  *Table
}

func (self TableRef) String() string {
	schema := "<nil>"
	if self.Schema != nil {
		schema = self.Schema.Name
	}
	table := "<nil>"
	if self.Table != nil {
		table = self.Table.Name
	}
	return fmt.Sprintf("%s.%s", schema, table)
}

func (self *TableRef) ToColumnRef(column *Column) *ColumnRef {
	return &ColumnRef{self.Schema, self.Table, column}
}
