package sql

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

// TODO(go,nth) implement these in terms of TableAlterParts

type ConstraintDrop struct {
	Table      TableRef
	Constraint string
}

func (self *ConstraintDrop) ToSql(q output.Quoter) string {
	return fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;", self.Table.Qualified(q), q.QuoteObject(self.Constraint))
}

type ConstraintCreateRaw struct {
	Table          TableRef
	Constraint     string
	ConstraintType model.ConstraintType
	Definition     string
}

func (self *ConstraintCreateRaw) ToSql(q output.Quoter) string {
	util.Assert(self.Table.Schema != "", "Empty schema name")
	util.Assert(self.Table.Table != "", "Empty table name")
	util.Assert(self.Constraint != "", "Empty constraint name")
	util.Assert(string(self.ConstraintType) != "", "Empty constraint type")
	util.Assert(self.Definition != "", "Empty constraint defintion")

	return fmt.Sprintf(
		"ALTER TABLE %s\n  ADD CONSTRAINT %s %s %s;",
		self.Table.Qualified(q),
		q.QuoteObject(self.Constraint),
		string(self.ConstraintType),
		self.Definition,
	)
}

type ConstraintCreatePrimaryKey struct {
	Table      TableRef
	Constraint string
	Columns    []string
}

func (self *ConstraintCreatePrimaryKey) ToSql(q output.Quoter) string {
	cols := make([]string, len(self.Columns))
	for i, col := range self.Columns {
		cols[i] = q.QuoteColumn(col)
	}
	return (&ConstraintCreateRaw{
		self.Table,
		self.Constraint,
		model.ConstraintType("PRIMARY KEY"), // note that it's invalid for this to exist in the xml so we have to make our own constant
		fmt.Sprintf("(%s)", strings.Join(cols, ", ")),
	}).ToSql(q)
}

type ConstraintCreateForeignKey struct {
	Table          TableRef
	Constraint     string
	LocalColumns   []string
	ForeignTable   TableRef
	ForeignColumns []string
	OnUpdate       model.ForeignKeyAction
	OnDelete       model.ForeignKeyAction
}

func (self *ConstraintCreateForeignKey) ToSql(q output.Quoter) string {
	localCols := make([]string, len(self.LocalColumns))
	for i, col := range self.LocalColumns {
		localCols[i] = q.QuoteColumn(col)
	}
	foreignCols := make([]string, len(self.ForeignColumns))
	for i, col := range self.ForeignColumns {
		foreignCols[i] = q.QuoteColumn(col)
	}

	onUpdate := ""
	if self.OnUpdate != "" {
		onUpdate = "ON UPDATE " + strings.ReplaceAll(string(self.OnUpdate), "_", " ")
	}
	onDelete := ""
	if self.OnDelete != "" {
		onDelete = "ON DELETE " + strings.ReplaceAll(string(self.OnDelete), "_", " ")
	}

	return (&ConstraintCreateRaw{
		self.Table,
		self.Constraint,
		model.ConstraintTypeForeign,
		util.CondJoin(" ",
			fmt.Sprintf(
				"(%s) REFERENCES %s (%s)",
				strings.Join(localCols, ", "),
				self.ForeignTable.Qualified(q),
				strings.Join(foreignCols, ", "),
			),
			onUpdate,
			onDelete,
		),
	}).ToSql(q)
}
