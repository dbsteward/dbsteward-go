package pgsql8

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

type ConstraintType string

const (
	ConstraintTypeAll        ConstraintType = "all"
	ConstraintTypePrimaryKey ConstraintType = "primaryKey"
	ConstraintTypeConstraint ConstraintType = "constraint"
	ConstraintTypeForeign    ConstraintType = "foreignKey"
)

// Returns true if this constraint type is or includes the given type
func (self ConstraintType) Includes(sub ConstraintType) bool {
	if self == ConstraintTypeAll || sub == ConstraintTypeAll {
		// all includes everything else, all is included by everything else
		return true
	}
	switch self {
	case ConstraintTypeConstraint:
		return sub != ConstraintTypePrimaryKey
	case ConstraintTypePrimaryKey:
		return sub == ConstraintTypePrimaryKey
	case ConstraintTypeForeign:
		return sub == ConstraintTypeForeign
	}
	util.Assert(false, "Unknown constraint type %s", self)
	return false
}

type TableConstraint struct {
	Schema           *model.Schema
	Table            *model.Table
	Columns          []*model.Column
	Name             string
	Type             ConstraintType
	UnderlyingType   model.ConstraintType
	TextDefinition   string
	ForeignSchema    *model.Schema
	ForeignTable     *model.Table
	ForeignCols      []*model.Column
	ForeignIndexName string
	ForeignOnUpdate  model.ForeignKeyAction
	ForeignOnDelete  model.ForeignKeyAction
}

type Constraint struct {
}

func NewConstraint() *Constraint {
	return &Constraint{}
}

// TODO(go,pgsql) make sure this is tested _thoroughly_
// TODO(go,core) lift this to sql99
// ConstraintTypeAll includes PrimaryKey,Constraint,Foreign
// ConstraintType
func (self *Constraint) GetTableConstraints(doc *model.Definition, schema *model.Schema, table *model.Table, ct ConstraintType) []*TableConstraint {
	if table == nil {
		return nil
	}
	// TODO(go,4) manifest explicit object node at compositing/expansion step instead of "hallucinating" them here
	constraints := []*TableConstraint{}

	// look for <table primaryKey> constraints
	if ct.Includes(ConstraintTypePrimaryKey) {
		// TODO(go,3) move validation elsewhere
		if len(table.PrimaryKey) == 0 {
			lib.GlobalDBSteward.Fatal("Table %s.%s does not have a primaryKey", schema.Name, table.Name)
		}

		cols, ok := table.TryGetColumnsNamed(table.PrimaryKey)
		if !ok {
			lib.GlobalDBSteward.Fatal("Table %s.%s does not have all named primary keys %v", schema.Name, table.Name, table.PrimaryKey)
		}
		constraints = append(constraints, &TableConstraint{
			Name:    util.CoalesceStr(table.PrimaryKeyName, GlobalIndex.BuildPrimaryKeyName(table.Name)),
			Schema:  schema,
			Table:   table,
			Columns: cols,
			Type:    ConstraintTypePrimaryKey,
		})
	}

	// look for <constraint> constraints
	if ct.Includes(ConstraintTypeConstraint) {
		// TODO(go,3) move validation elsewhere
		for _, constraint := range table.Constraints {
			if ct.Includes(ConstraintTypeForeign) && constraint.Type.Equals(model.ConstraintTypeForeign) {
				var fSchema *model.Schema
				if constraint.ForeignSchema != "" {
					fSchema = doc.TryGetSchemaNamed(constraint.ForeignSchema)
					if fSchema == nil {
						lib.GlobalDBSteward.Fatal(
							"Table constraint %s.%s.%s references foreignSchema %s but definition does not contain that schema",
							schema.Name,
							table.Name,
							constraint.Name,
							constraint.ForeignSchema,
						)
					}
				}
				var fTable *model.Table
				if constraint.ForeignTable != "" {
					if fSchema == nil {
						fSchema = schema
					}
					fTable = fSchema.TryGetTableNamed(constraint.ForeignTable)

					if fTable == nil {
						lib.GlobalDBSteward.Fatal(
							"Table constraint %s.%s.%s references foreignTable %s but schema %s does not contain that table",
							schema.Name,
							table.Name,
							constraint.Name,
							fSchema.Name,
							constraint.ForeignTable,
						)
					}
				}

				constraints = append(constraints, &TableConstraint{
					Name:             constraint.Name,
					Schema:           schema,
					Table:            table,
					Type:             ConstraintTypeForeign,
					UnderlyingType:   constraint.Type,
					TextDefinition:   constraint.Definition,
					ForeignIndexName: constraint.ForeignIndexName,
					ForeignSchema:    fSchema,
					ForeignTable:     fTable,
				})
			} else if ct.Includes(ConstraintTypeConstraint) {
				constraints = append(constraints, &TableConstraint{
					Name:           constraint.Name,
					Schema:         schema,
					Table:          table,
					Type:           ConstraintTypeConstraint,
					UnderlyingType: constraint.Type,
					TextDefinition: constraint.Definition,
				})
			}
		}
	}

	// look for explicit <foreignKey> constraints
	if ct.Includes(ConstraintTypeForeign) {
		for _, fk := range table.ForeignKeys {
			localCols, ok := table.TryGetColumnsNamed(fk.Columns)
			if !ok {
				lib.GlobalDBSteward.Fatal(
					"foreignKey %s on %s.%s references local columns %v that don't exist",
					fk.ConstraintName,
					schema.Name,
					table.Name,
					fk.Columns,
				)
			}

			localKey := model.Key{
				Schema:  schema,
				Table:   table,
				Columns: localCols,
			}

			ref := lib.GlobalDBX.ResolveForeignKey(doc, localKey, fk.GetReferencedKey())
			constraints = append(constraints, &TableConstraint{
				Name:             fk.ConstraintName,
				Schema:           schema,
				Table:            table,
				Type:             ConstraintTypeForeign,
				ForeignIndexName: fk.IndexName,
				ForeignSchema:    ref.Schema,
				ForeignTable:     ref.Table,
				ForeignCols:      ref.Columns,
				ForeignOnUpdate:  fk.OnUpdate,
				ForeignOnDelete:  fk.OnDelete,
			})
		}
	}

	// look for constraints in columns
	if ct.Includes(ConstraintTypeConstraint) {
		for _, column := range table.Columns {
			if ct.Includes(ConstraintTypeForeign) && column.HasForeignKey() {
				foreign := column.GetReferencedKey()
				local := model.Key{
					Schema:  schema,
					Table:   table,
					Columns: []*model.Column{column},
				}
				ref := lib.GlobalDBX.ResolveForeignKey(doc, local, foreign)
				constraints = append(constraints, &TableConstraint{
					Name:             util.CoalesceStr(column.ForeignKeyName, GlobalIndex.BuildForeignKeyName(table.Name, column.Name)),
					Schema:           schema,
					Table:            table,
					Columns:          local.Columns,
					Type:             ConstraintTypeForeign,
					ForeignIndexName: column.ForeignIndexName,
					ForeignSchema:    ref.Schema,
					ForeignTable:     ref.Table,
					ForeignCols:      ref.Columns,
					ForeignOnUpdate:  column.ForeignOnUpdate,
					ForeignOnDelete:  column.ForeignOnDelete,
				})
			}

			if column.Check != "" {
				constraints = append(constraints, &TableConstraint{
					Name:           column.Name + "_check", // TODO(feat) is this correct?
					Schema:         schema,
					Table:          table,
					Columns:        []*model.Column{column},
					Type:           ConstraintTypeConstraint,
					UnderlyingType: model.ConstraintTypeCheck,
					TextDefinition: column.Check,
				})
			}

			// TODO(feat) should we be incorporating unique constraints in here? or are those handled by indexes? is that true in every dialect?
		}
	}

	return constraints
}

func (self *Constraint) TryGetTableConstraintNamed(doc *model.Definition, schema *model.Schema, table *model.Table, name string, constraintType ConstraintType) *TableConstraint {
	// TODO(feat) can make this a little more performant if we pass constraint type in
	for _, constraint := range self.GetTableConstraints(doc, schema, table, constraintType) {
		if strings.EqualFold(constraint.Name, name) {
			return constraint
		}
	}
	return nil
}

func (self *Constraint) GetDropSql(constraint *TableConstraint) []output.ToSql {
	return []output.ToSql{
		&sql.ConstraintDrop{
			Table:      sql.TableRef{constraint.Schema.Name, constraint.Table.Name},
			Constraint: constraint.Name,
		},
	}
}

func (self *Constraint) GetCreationSql(constraint *TableConstraint) []output.ToSql {
	table := sql.TableRef{constraint.Schema.Name, constraint.Table.Name}

	// if there's a text definition, prefer that; it should have come verbatim from the xml
	if constraint.TextDefinition != "" {
		util.Assert(constraint.UnderlyingType != "", "TableConstraint should not have a TextDefinition but no UnderlyingType")
		return []output.ToSql{
			&sql.ConstraintCreateRaw{
				Table:          table,
				Constraint:     constraint.Name,
				ConstraintType: constraint.UnderlyingType,
				Definition:     constraint.TextDefinition,
			},
		}
	}

	if constraint.Type == ConstraintTypePrimaryKey {
		cols := make([]string, len(constraint.Columns))
		for i, col := range constraint.Columns {
			cols[i] = col.Name
		}
		return []output.ToSql{
			&sql.ConstraintCreatePrimaryKey{
				Table:      table,
				Constraint: constraint.Name,
				Columns:    cols,
			},
		}
	}

	if constraint.Type == ConstraintTypeForeign {
		localCols := make([]string, len(constraint.Columns))
		for i, col := range constraint.Columns {
			localCols[i] = col.Name
		}
		foreignCols := make([]string, len(constraint.Columns))
		for i, col := range constraint.ForeignCols {
			foreignCols[i] = col.Name
		}
		return []output.ToSql{
			&sql.ConstraintCreateForeignKey{
				Table:          table,
				Constraint:     constraint.Name,
				LocalColumns:   localCols,
				ForeignTable:   sql.TableRef{constraint.ForeignSchema.Name, constraint.ForeignTable.Name},
				ForeignColumns: foreignCols,
				OnUpdate:       constraint.ForeignOnUpdate,
				OnDelete:       constraint.ForeignOnDelete,
			},
		}
	}

	util.Assert(false, "This should be unreachable, check that TableConstraint is constructed correctly or Constraint.GetTableConstraints is correct: %#v", constraint)
	return nil
}

func (self *TableConstraint) Equals(other *TableConstraint) bool {
	// TODO(go,core) this definition is slightly different than php, double check and test it
	if self == nil || other == nil {
		return false
	}

	if !strings.EqualFold(self.Name, other.Name) {
		return false
	}

	if !strings.EqualFold(string(self.Type), string(other.Type)) {
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

	for i, col := range self.Columns {
		if !strings.EqualFold(col.Type, other.Columns[i].Type) {
			return false
		}
	}

	return true
}

func (self *TableConstraint) DependsOnRenamedTable(doc *model.Definition) bool {
	if lib.GlobalDBSteward.IgnoreOldNames {
		return false
	}

	refSchema := self.ForeignSchema
	refTable := self.ForeignTable
	if refTable == nil && self.TextDefinition != "" && self.Type == ConstraintTypeForeign {
		if matches := util.IMatch(`^.+\s+REFERENCES\s+\"?(\w+)\"?\.\"?(\w+)\"?\s*\(\s*\"?(.*)\"?\s*\)$`, self.TextDefinition); len(matches) > 0 {
			refSchema = doc.TryGetSchemaNamed(matches[1])
			if refSchema == nil {
				lib.GlobalDBSteward.Fatal("Constraint %s.%s.%s references schema %s but could not find it", self.Schema.Name, self.Table.Name, self.Name, matches[1])
			}

			refTable = refSchema.TryGetTableNamed(matches[2])
			if refTable == nil {
				lib.GlobalDBSteward.Fatal("Constraint %s.%s.%s references table %s.%s but could not find it", self.Schema.Name, self.Table.Name, self.Name, matches[1], matches[2])
			}
		} else {
			lib.GlobalDBSteward.Fatal("Failed to parse REFERENCES definition for constraint %s.%s.%s: %s", self.Schema.Name, self.Table.Name, self.Name, self.TextDefinition)
		}
	}

	if refTable == nil {
		return false
	}

	if GlobalDiffTables.IsRenamedTable(refSchema, refTable) {
		lib.GlobalDBSteward.Notice("Constraint %s.%s.%s references renamed table %s.%s", self.Schema.Name, self.Table.Name, self.Name, refSchema.Name, refTable.Name)
		return true
	}
	return false
}
