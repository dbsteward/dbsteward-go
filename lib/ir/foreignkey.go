package ir

import (
	"fmt"
	"strings"
)

type ForeignKeyAction string

const (
	ForeignKeyActionNoAction   ForeignKeyAction = "NO_ACTION"
	ForeignKeyActionRestrict   ForeignKeyAction = "RESTRICT"
	ForeignKeyActionCascade    ForeignKeyAction = "CASCADE"
	ForeignKeyActionSetNull    ForeignKeyAction = "SET_NULL"
	ForeignKeyActionSetDefault ForeignKeyAction = "SET_DEFAULT"
)

func NewForeignKeyAction(s string) (ForeignKeyAction, error) {
	if s == "" {
		return ForeignKeyActionNoAction, nil
	}
	fka := ForeignKeyAction(s)
	if fka.Equals(ForeignKeyActionNoAction) {
		return ForeignKeyActionNoAction, nil
	}
	if fka.Equals(ForeignKeyActionRestrict) {
		return ForeignKeyActionRestrict, nil
	}
	if fka.Equals(ForeignKeyActionCascade) {
		return ForeignKeyActionCascade, nil
	}
	if fka.Equals(ForeignKeyActionSetNull) {
		return ForeignKeyActionSetNull, nil
	}
	if fka.Equals(ForeignKeyActionSetDefault) {
		return ForeignKeyActionSetDefault, nil
	}
	return "", fmt.Errorf("invalid Foreign Key Action: '%s'", s)
}

func (fka ForeignKeyAction) Equals(other ForeignKeyAction) bool {
	return strings.EqualFold(string(fka), string(other))
}

type ForeignKey struct {
	Columns        []string
	ForeignSchema  string
	ForeignTable   string
	ForeignColumns []string
	ConstraintName string
	IndexName      string
	OnUpdate       ForeignKeyAction
	OnDelete       ForeignKeyAction
}

func (self *ForeignKey) GetReferencedKey() KeyNames {
	cols := self.ForeignColumns
	if len(cols) == 0 {
		cols = self.Columns
	}
	return KeyNames{
		Schema:  self.ForeignSchema,
		Table:   self.ForeignTable,
		Columns: cols,
		KeyName: self.ConstraintName,
	}
}

func (self *ForeignKey) IdentityMatches(other *ForeignKey) bool {
	if self == nil || other == nil {
		return false
	}
	// TODO(go,core) validate this constraint/index name matching behavior
	// TODO(feat) case sensitivity
	return strings.EqualFold(self.ConstraintName, other.ConstraintName)
}

func (self *ForeignKey) Validate(doc *Definition, schema *Schema, table *Table) []error {
	out := []error{}
	if self.ConstraintName == "" {
		out = append(out, fmt.Errorf("foreign key in table %s.%s must have a constraint name", schema.Name, table.Name))
	}
	// TODO(go,3) validate reference, remove other codepaths
	return out
}
