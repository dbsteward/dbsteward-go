package sql99

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib/ir"
)

type ConstraintType uint

const (
	ConstraintTypePrimaryKey ConstraintType = 1 << iota
	ConstraintTypeForeign
	ConstraintTypeOther

	ConstraintTypeConstraint ConstraintType = ConstraintTypeOther | ConstraintTypeForeign
	ConstraintTypeAll        ConstraintType = ConstraintTypeConstraint | ConstraintTypePrimaryKey
)

// Returns true if this constraint type is or includes the given type
func (self ConstraintType) Includes(sub ConstraintType) bool {
	return self&sub != 0
}

func (self ConstraintType) Equals(other ConstraintType) bool {
	return self == other
}

type TableConstraint struct {
	Schema           *ir.Schema
	Table            *ir.Table
	Columns          []*ir.Column
	Name             string
	Type             ConstraintType
	UnderlyingType   ir.ConstraintType
	TextDefinition   string
	ForeignSchema    *ir.Schema
	ForeignTable     *ir.Table
	ForeignCols      []*ir.Column
	ForeignIndexName string
	ForeignOnUpdate  ir.ForeignKeyAction
	ForeignOnDelete  ir.ForeignKeyAction
}

func (self *TableConstraint) Equals(other *TableConstraint) bool {
	// TODO(go,core) this definition is slightly different than php, double check and test it
	if self == nil || other == nil {
		return false
	}

	if !strings.EqualFold(self.Name, other.Name) {
		return false
	}

	if !self.Type.Equals(other.Type) {
		return false
	}

	if !strings.EqualFold(string(self.UnderlyingType), string(other.UnderlyingType)) {
		return false
	}

	if self.TextDefinition != other.TextDefinition {
		return false
	}

	if self.ForeignOnDelete != other.ForeignOnDelete {
		return false
	}

	if self.ForeignOnUpdate != other.ForeignOnUpdate {
		return false
	}

	if len(self.ForeignCols) != len(other.ForeignCols) {
		return false
	}

	for i, col := range self.Columns {
		if !strings.EqualFold(col.Name, other.Columns[i].Name) {
			return false
		}

		// TODO(feat) We should double check this: does changing a column type invalidate a constraint over it?
		// if !strings.EqualFold(col.Type, other.Columns[i].Type) {
		// 	return false
		// }

		if len(self.ForeignCols) > 0 && !strings.EqualFold(self.ForeignCols[i].Type, other.ForeignCols[i].Type) {
			return false
		}
	}

	return true
}
