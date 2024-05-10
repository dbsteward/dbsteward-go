package pgsql8

import (
	"fmt"
	"log"
	"strconv"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/util"
	"github.com/pkg/errors"
)

type moduloPartition struct {
	parts      int
	column     string
	slonyRange *slonyRange
}

func newModuloPartition(schema *ir.Schema, table *ir.Table) (*moduloPartition, error) {
	partNumberStr := table.Partitioning.TryGetOptionValueNamed("number")
	if partNumberStr == "" {
		return nil, fmt.Errorf("tablePartitionOption 'number' must be specificed for table %s.%s", schema.Name, table.Name)
	}
	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil {
		return nil, fmt.Errorf("tablePartitionOption 'number' for table %s.%s could not be parsed as an int", schema.Name, table.Name)
	}

	partColumn := table.Partitioning.TryGetOptionValueNamed("column")
	if partColumn == "" {
		return nil, fmt.Errorf("tablePartitionOption 'column' must be specificed for table %s.%s", schema.Name, table.Name)
	}

	firstSlonyIdStr := table.Partitioning.TryGetOptionValueNamed("firstSlonyId")
	lastSlonyIdStr := table.Partitioning.TryGetOptionValueNamed("lastSlonyId")
	slonyIds, err := tryNewSlonyRange(firstSlonyIdStr, lastSlonyIdStr, partNumber)
	if err != nil {
		return nil, err
	}

	return &moduloPartition{
		parts:      partNumber,
		column:     partColumn,
		slonyRange: slonyIds,
	}, nil
}

func (mp *moduloPartition) tableName(i int) string {
	return fmt.Sprintf("partition_%0*d", util.NumDigits(mp.parts), i)
}

func (p *XmlParser) expandModuloParitionedTable(doc *ir.Definition, schema *ir.Schema, table *ir.Table) error {
	util.Assert(table.Partitioning != nil, "Table.Partitioning must not be nil")
	util.Assert(table.Partitioning.Type.Equals(ir.TablePartitionTypeModulo), "must be modulo type")

	opts, err := newModuloPartition(schema, table)
	if err != nil {
		return err
	}

	// create the schema node for parititions
	partSchema := &ir.Schema{}
	doc.AddSchema(partSchema)
	p.createModuloPartitionSchema(schema, table, partSchema, opts)
	p.createModuloPartitionTables(schema, table, partSchema, opts)

	// add trigger and function to the main table
	p.createModuloPartitionTrigger(schema, table, partSchema, opts)
	return nil
}

func (p *XmlParser) createModuloPartitionSchema(schema *ir.Schema, table *ir.Table, partSchema *ir.Schema, opts *moduloPartition) {
	partSchema.Name = fmt.Sprintf("_p_%s_%s", schema.Name, table.Name)
	for _, grant := range schema.Grants {
		grantCopy := *grant
		partSchema.AddGrant(&grantCopy)
	}
}

func (p *XmlParser) createModuloPartitionTables(schema *ir.Schema, table *ir.Table, partSchema *ir.Schema, opts *moduloPartition) {
	for i := 0; i < opts.parts; i++ {
		partDigits := util.NumDigits(opts.parts)
		partNum := fmt.Sprintf("%0*d", partDigits, i)

		partTable := &ir.Table{
			Name:           opts.tableName(i),
			Owner:          table.Owner,
			PrimaryKey:     table.PrimaryKey,
			InheritsTable:  table.Name,
			InheritsSchema: schema.Name,
		}
		partSchema.AddTable(partTable)

		partTable.AddConstraint(&ir.Constraint{
			Name: fmt.Sprintf("%s_p_%s_chk", table.Name, partNum),
			Type: ir.ConstraintTypeCheck,
			// TODO(go,3) use higher level rep instead of xml rep here to resolve need for string-level quoting at this point
			Definition: fmt.Sprintf("((%s %% %d) = %d)", p.quoter.QuoteColumn(opts.column), opts.parts, i),
		})

		for _, index := range table.Indexes {
			if len(index.Dimensions) == 0 {
				log.Panicf("Index %s has no dimensions", index.Name)
			}
			indexCopy := *index
			indexCopy.Name = simpleBuildIdentifier("", index.Name, "_p"+partNum)
			partTable.AddIndex(&indexCopy)
		}

		for _, constraint := range table.Constraints {
			constraintCopy := *constraint
			constraintCopy.Name = simpleBuildIdentifier("p"+partNum+"_", constraint.Name, "")
			partTable.AddConstraint(&constraintCopy)
		}

		for _, foreignKey := range table.ForeignKeys {
			foreignKeyCopy := *foreignKey
			foreignKeyCopy.ConstraintName = simpleBuildIdentifier("p"+partNum+"_", foreignKey.ConstraintName, "")
			foreignKeyCopy.IndexName = util.MaybeStr(foreignKey.IndexName != "", simpleBuildIdentifier("", foreignKey.IndexName, "_p"+partNum))
			partTable.AddForeignKey(&foreignKeyCopy)
		}

		// we can't copy columns, but if there are any foreign keys defined, we need to mirror those
		for _, column := range table.Columns {
			if column.HasForeignKey() {
				partTable.AddForeignKey(&ir.ForeignKey{
					ConstraintName: simpleBuildIdentifier("p"+partNum+"_", column.Name, "_fk"),
					Columns:        []string{column.Name},
					ForeignSchema:  util.CoalesceStr(column.ForeignSchema, schema.Name),
					ForeignTable:   column.ForeignTable,
					ForeignColumns: []string{util.CoalesceStr(column.ForeignColumn, column.Name)},
					OnUpdate:       column.ForeignOnUpdate,
					OnDelete:       column.ForeignOnDelete,
				})
			}
		}

		for _, grant := range table.Grants {
			grantCopy := *grant
			partTable.AddGrant(&grantCopy)
		}
	}

	// remove attributes from the main table that move to the partitions
	table.Indexes = nil
}

func (p *XmlParser) createModuloPartitionTrigger(schema *ir.Schema, table *ir.Table, partSchema *ir.Schema, opts *moduloPartition) {
	funcDef := fmt.Sprintf("DECLARE\n\tmod_result INT;\nBEGIN\n\tmod_result := NEW.%s %% %d;\n",
		quoter.QuoteColumn(opts.column), opts.parts)
	for i := 0; i < opts.parts; i++ {
		funcDef += "\t"
		if i != 0 {
			funcDef += "ELSE"
		}
		funcDef += fmt.Sprintf("IF (mod_result = %d) THEN\n\t\tINSERT INTO %s VALUES (NEW.*);\n",
			i, quoter.QualifyTable(partSchema.Name, opts.tableName(i)))
	}
	funcDef += "\tEND IF;\n\tRETURN NULL;\nEND;"

	partSchema.AddFunction(&ir.Function{
		Name:        "insert_trigger",
		Returns:     "TRIGGER",
		Owner:       table.Owner,
		Description: fmt.Sprintf("DBSteward auto-generated for table partition of %s.%s", schema.Name, table.Name),
		Definitions: []*ir.FunctionDefinition{{
			Language:  "plpgsql",
			SqlFormat: ir.SqlFormatPgsql8,
			Text:      funcDef,
		}},
		Grants: []*ir.Grant{{
			Roles:       []string{ir.RoleApplication},
			Permissions: []string{ir.PermissionExecute},
		}},
	})

	// add trigger to the main schema/table
	schema.AddTrigger(&ir.Trigger{
		Name:      table.Name + "_part_trg",
		SqlFormat: ir.SqlFormatPgsql8,
		Events:    []string{"INSERT"},
		Timing:    ir.TriggerTimingBefore,
		Table:     table.Name,
		ForEach:   ir.TriggerForEachRow,
		Function:  partSchema.Name + ".insert_trigger()",
	})
}

func (p *XmlParser) checkModuloPartitionChange(oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) error {
	oldOpts, err := newModuloPartition(oldSchema, oldTable)
	if err != nil {
		return err
	}
	newOpts, err := newModuloPartition(newSchema, newTable)
	if err != nil {
		return err
	}
	if oldOpts.parts != newOpts.parts {
		return errors.Errorf("Changing the number of partitions in a table will lead to data loss: %s.%s", newSchema.Name, newTable.Name)
	}
	if oldOpts.column != newOpts.column {
		return errors.Errorf("Changing the paritioning column in a table will lead to data loss: %s.%s", newSchema.Name, newTable.Name)
	}
	return nil
}

func simpleBuildIdentifier(prefix, name, suffix string) string {
	remaining := MAX_IDENT_LENGTH - len(prefix) - len(suffix)
	return prefix + name[0:util.Min(remaining, len(name))] + suffix
}
