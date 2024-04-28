package pgsql8

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/format/sql99"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

// TODO(go,pgsql) make sure this is tested _thoroughly_
// TODO(go,core) lift this to sql99
// ConstraintTypeAll includes PrimaryKey,Constraint,Foreign
// sql99.ConstraintType
func getTableConstraints(doc *ir.Definition, schema *ir.Schema, table *ir.Table, ct sql99.ConstraintType) []*sql99.TableConstraint {
	if table == nil {
		return nil
	}
	// TODO(go,4) manifest explicit object node at compositing/expansion step instead of "hallucinating" them here
	constraints := []*sql99.TableConstraint{}

	// look for <table primaryKey> constraints
	if ct.Includes(sql99.ConstraintTypePrimaryKey) {
		// TODO(go,3) move validation elsewhere
		if len(table.PrimaryKey) == 0 {
			lib.GlobalDBSteward.Fatal("Table %s.%s does not have a primaryKey", schema.Name, table.Name)
		}

		cols, ok := lib.GlobalDBX.TryInheritanceGetColumns(doc, schema, table, table.PrimaryKey)
		if !ok {
			lib.GlobalDBSteward.Fatal("Table %s.%s does not have all named primary keys %v", schema.Name, table.Name, table.PrimaryKey)
		}
		constraints = append(constraints, &sql99.TableConstraint{
			Name:    util.CoalesceStr(table.PrimaryKeyName, buildPrimaryKeyName(table.Name)),
			Type:    sql99.ConstraintTypePrimaryKey,
			Schema:  schema,
			Table:   table,
			Columns: cols,
		})
	}

	// look for <constraint> constraints
	if ct.Includes(sql99.ConstraintTypeConstraint) {
		// TODO(go,3) move validation elsewhere
		for _, constraint := range table.Constraints {
			if ct.Includes(sql99.ConstraintTypeForeign) && constraint.Type.Equals(ir.ConstraintTypeForeign) {
				var fSchema *ir.Schema
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
				var fTable *ir.Table
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

				constraints = append(constraints, &sql99.TableConstraint{
					Name:             constraint.Name,
					Type:             sql99.ConstraintTypeForeign,
					Schema:           schema,
					Table:            table,
					UnderlyingType:   constraint.Type,
					TextDefinition:   constraint.Definition,
					ForeignIndexName: constraint.ForeignIndexName,
					ForeignSchema:    fSchema,
					ForeignTable:     fTable,
				})
			} else if ct.Includes(sql99.ConstraintTypeConstraint) {
				constraints = append(constraints, &sql99.TableConstraint{
					Name:           constraint.Name,
					Type:           sql99.ConstraintTypeConstraint,
					Schema:         schema,
					Table:          table,
					UnderlyingType: constraint.Type,
					TextDefinition: constraint.Definition,
				})
			}
		}
	}

	// look for explicit <foreignKey> constraints
	if ct.Includes(sql99.ConstraintTypeForeign) {
		for _, fk := range table.ForeignKeys {
			if fk.ConstraintName == "" {
				// TODO(go,3) remove this restriction, generate a name
				lib.GlobalDBSteward.Fatal("foreignKey on %s.%s requires a constraintName", schema.Name, table.Name)
			}

			localCols, ok := lib.GlobalDBX.TryInheritanceGetColumns(doc, schema, table, fk.Columns)
			if !ok {
				lib.GlobalDBSteward.Fatal(
					"foreignKey %s on %s.%s references local columns %v that don't exist",
					fk.ConstraintName,
					schema.Name,
					table.Name,
					fk.Columns,
				)
			}

			localKey := ir.Key{
				Schema:  schema,
				Table:   table,
				Columns: localCols,
			}

			ref := lib.GlobalDBX.ResolveForeignKey(doc, localKey, fk.GetReferencedKey())
			constraints = append(constraints, &sql99.TableConstraint{
				Name:             fk.ConstraintName,
				Type:             sql99.ConstraintTypeForeign,
				Schema:           schema,
				Table:            table,
				Columns:          localCols,
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
	if ct.Includes(sql99.ConstraintTypeConstraint) {
		for _, column := range table.Columns {
			if ct.Includes(sql99.ConstraintTypeForeign) && column.HasForeignKey() {
				ref := lib.GlobalDBX.ResolveForeignKeyColumn(doc, schema, table, column)
				constraints = append(constraints, &sql99.TableConstraint{
					Name:             util.CoalesceStr(column.ForeignKeyName, buildForeignKeyName(table.Name, column.Name)),
					Type:             sql99.ConstraintTypeForeign,
					Schema:           schema,
					Table:            table,
					Columns:          []*ir.Column{column},
					ForeignIndexName: column.ForeignIndexName,
					ForeignSchema:    ref.Schema,
					ForeignTable:     ref.Table,
					ForeignCols:      ref.Columns,
					ForeignOnUpdate:  column.ForeignOnUpdate,
					ForeignOnDelete:  column.ForeignOnDelete,
				})
			}

			if column.Check != "" {
				constraints = append(constraints, &sql99.TableConstraint{
					Name:           column.Name + "_check", // TODO(feat) is this correct?
					Type:           sql99.ConstraintTypeConstraint,
					Schema:         schema,
					Table:          table,
					Columns:        []*ir.Column{column},
					UnderlyingType: ir.ConstraintTypeCheck,
					TextDefinition: column.Check,
				})
			}

			// TODO(feat) should we be incorporating unique constraints in here? or are those handled by indexes? is that true in every dialect?
		}
	}

	return constraints
}

func tryGetTableConstraintNamed(doc *ir.Definition, schema *ir.Schema, table *ir.Table, name string, constraintType sql99.ConstraintType) *sql99.TableConstraint {
	// TODO(feat) can make this a little more performant if we pass constraint type in
	for _, constraint := range getTableConstraints(doc, schema, table, constraintType) {
		if strings.EqualFold(constraint.Name, name) {
			return constraint
		}
	}
	return nil
}

func getTableConstraintDropSql(constraint *sql99.TableConstraint) []output.ToSql {
	return []output.ToSql{
		&sql.ConstraintDrop{
			Table:      sql.TableRef{Schema: constraint.Schema.Name, Table: constraint.Table.Name},
			Constraint: constraint.Name,
		},
	}
}

func getTableContraintCreationSql(constraint *sql99.TableConstraint) []output.ToSql {
	table := sql.TableRef{Schema: constraint.Schema.Name, Table: constraint.Table.Name}

	// if there's a text definition, prefer that; it should have come verbatim from the xml
	if constraint.TextDefinition != "" {
		util.Assert(constraint.UnderlyingType != "", "sql99.TableConstraint should not have a TextDefinition but no UnderlyingType")
		return []output.ToSql{
			&sql.ConstraintCreateRaw{
				Table:          table,
				Constraint:     constraint.Name,
				ConstraintType: constraint.UnderlyingType,
				Definition:     "(" + normalizeColumnCheckCondition(constraint.TextDefinition) + ")",
			},
		}
	}

	if constraint.Type == sql99.ConstraintTypePrimaryKey {
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

	if constraint.Type == sql99.ConstraintTypeForeign {
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
				ForeignTable:   sql.TableRef{Schema: constraint.ForeignSchema.Name, Table: constraint.ForeignTable.Name},
				ForeignColumns: foreignCols,
				OnUpdate:       constraint.ForeignOnUpdate,
				OnDelete:       constraint.ForeignOnDelete,
			},
		}
	}

	util.Assert(false, "This should be unreachable, check that sql99.TableConstraint is constructed correctly or Constraint.GetTableConstraints is correct: %#v", constraint)
	return nil
}

func constraintDependsOnRenamedTable(doc *ir.Definition, constraint *sql99.TableConstraint) bool {
	if lib.GlobalDBSteward.IgnoreOldNames {
		return false
	}

	refSchema := constraint.ForeignSchema
	refTable := constraint.ForeignTable
	if refTable == nil && constraint.TextDefinition != "" && constraint.Type == sql99.ConstraintTypeForeign {
		if matches := util.IMatch(`^.+\s+REFERENCES\s+\"?(\w+)\"?\.\"?(\w+)\"?\s*\(\s*\"?(.*)\"?\s*\)$`, constraint.TextDefinition); len(matches) > 0 {
			refSchema = doc.TryGetSchemaNamed(matches[1])
			if refSchema == nil {
				lib.GlobalDBSteward.Fatal("Constraint %s.%s.%s references schema %s but could not find it", constraint.Schema.Name, constraint.Table.Name, constraint.Name, matches[1])
			}

			refTable = refSchema.TryGetTableNamed(matches[2])
			if refTable == nil {
				lib.GlobalDBSteward.Fatal("Constraint %s.%s.%s references table %s.%s but could not find it", constraint.Schema.Name, constraint.Table.Name, constraint.Name, matches[1], matches[2])
			}
		} else {
			lib.GlobalDBSteward.Fatal("Failed to parse REFERENCES definition for constraint %s.%s.%s: %s", constraint.Schema.Name, constraint.Table.Name, constraint.Name, constraint.TextDefinition)
		}
	}

	if refTable == nil {
		return false
	}

	isRenamed, err := lib.GlobalDBX.IsRenamedTable(refSchema, refTable)
	lib.GlobalDBSteward.FatalIfError(err, "while checking if constraint depends on renamed table")
	if isRenamed {
		lib.GlobalDBSteward.Notice("Constraint %s.%s.%s references renamed table %s.%s", constraint.Schema.Name, constraint.Table.Name, constraint.Name, refSchema.Name, refTable.Name)
		return true
	}
	return false
}
