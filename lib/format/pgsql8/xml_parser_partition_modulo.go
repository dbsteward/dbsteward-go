package pgsql8

import (
	"fmt"
	"strconv"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/util"
	"github.com/pkg/errors"
)

type moduloPartition struct {
	parts      int
	column     string
	slonyRange *slonyRange
}

func newModuloPartition(schema *model.Schema, table *model.Table) *moduloPartition {
	partNumberStr := table.Partitioning.TryGetOptionValueNamed("number")
	if partNumberStr == "" {
		lib.GlobalDBSteward.Fatal("tablePartitionOption 'number' must be specificed for table %s.%s", schema.Name, table.Name)
	}
	partNumber, err := strconv.Atoi(partNumberStr)
	lib.GlobalDBSteward.FatalIfError(err, "tablePartitionOption 'number' for table %s.%s could not be parsed as an int", schema.Name, table.Name)

	partColumn := table.Partitioning.TryGetOptionValueNamed("column")
	if partColumn == "" {
		lib.GlobalDBSteward.Fatal("tablePartitionOption 'column' must be specificed for table %s.%s", schema.Name, table.Name)
	}

	firstSlonyIdStr := table.Partitioning.TryGetOptionValueNamed("firstSlonyId")
	lastSlonyIdStr := table.Partitioning.TryGetOptionValueNamed("lastSlonyId")
	slonyIds := tryNewSlonyRange(firstSlonyIdStr, lastSlonyIdStr, partNumber)

	return &moduloPartition{
		parts:      partNumber,
		column:     partColumn,
		slonyRange: slonyIds,
	}
}

func (self *moduloPartition) tableName(i int) string {
	return fmt.Sprintf("partition_%0*d", util.NumDigits(self.parts), i)
}
func (self *moduloPartition) slonyId(i int) *int {
	if self.slonyRange == nil {
		return nil
	}
	return util.Intp(self.slonyRange.first + i)
}

func (self *XmlParser) expandModuloParitionedTable(doc *model.Definition, schema *model.Schema, table *model.Table) {
	util.Assert(table.Partitioning != nil, "Table.Partitioning must not be nil")
	util.Assert(table.Partitioning.Type.Equals(model.TablePartitionTypeModulo), "must be modulo type")

	opts := newModuloPartition(schema, table)

	// create the schema node for parititions
	partSchema := &model.Schema{}
	doc.AddSchema(partSchema)
	self.createModuloPartitionSchema(schema, table, partSchema, opts)
	self.createModuloPartitionTables(schema, table, partSchema, opts)

	// add trigger and function to the main table
	self.createModuloPartitionTrigger(schema, table, partSchema, opts)
}

func (self *XmlParser) createModuloPartitionSchema(schema *model.Schema, table *model.Table, partSchema *model.Schema, opts *moduloPartition) {
	partSchema.Name = fmt.Sprintf("_p_%s_%s", schema.Name, table.Name)
	for _, grant := range schema.Grants {
		grantCopy := *grant
		partSchema.AddGrant(&grantCopy)
	}
}

func (self *XmlParser) createModuloPartitionTables(schema *model.Schema, table *model.Table, partSchema *model.Schema, opts *moduloPartition) {
	for i := 0; i < opts.parts; i++ {
		partDigits := util.NumDigits(opts.parts)
		partNum := fmt.Sprintf("%0*d", partDigits, i)

		partTable := &model.Table{
			Name:           opts.tableName(i),
			Owner:          table.Owner,
			PrimaryKey:     table.PrimaryKey,
			InheritsTable:  table.Name,
			InheritsSchema: schema.Name,
			SlonySetId:     table.SlonySetId,
			SlonyId:        opts.slonyId(i),
		}
		partSchema.AddTable(partTable)

		partTable.AddConstraint(&model.Constraint{
			Name:       fmt.Sprintf("%s_p_%s_chk", table.Name, partNum),
			Type:       model.ConstraintTypeCheck,
			Definition: fmt.Sprintf("((%s %% %d) = %d)", GlobalOperations.QuoteColumn(opts.column), opts.parts, i),
		})

		for _, index := range table.Indexes {
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
				partTable.AddForeignKey(&model.ForeignKey{
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

func (self *XmlParser) createModuloPartitionTrigger(schema *model.Schema, table *model.Table, partSchema *model.Schema, opts *moduloPartition) {
	funcDef := fmt.Sprintf("DECLARE\n\tmod_result INT;\nBEGIN\n\tmod_result := NEW.%s %% %d;\n",
		GlobalOperations.QuoteColumn(opts.column), opts.parts)
	for i := 0; i < opts.parts; i++ {
		funcDef += "\t"
		if i != 0 {
			funcDef += "ELSE"
		}
		funcDef += fmt.Sprintf("IF (mod_result = %d) THEN\n\t\tINSERT INTO %s VALUES (NEW.*);\n",
			i, GlobalOperations.QualifyTable(partSchema.Name, opts.tableName(i)))
	}
	funcDef += "\tEND IF;\n\tRETURN NULL;\nEND;"

	partSchema.AddFunction(&model.Function{
		Name:        "insert_trigger",
		Returns:     "TRIGGER",
		Owner:       table.Owner,
		Description: fmt.Sprintf("DBSteward auto-generated for table partition of %s.%s", schema.Name, table.Name),
		Definitions: []*model.FunctionDefinition{{
			Language:  "plpgsql",
			SqlFormat: model.SqlFormatPgsql8,
			Text:      funcDef,
		}},
		Grants: []*model.Grant{{
			Roles:       []string{model.RoleApplication},
			Permissions: []string{model.PermissionExecute},
		}},
	})

	// add trigger to the main schema/table
	schema.AddTrigger(&model.Trigger{
		Name:      table.Name + "_part_trg",
		SqlFormat: model.SqlFormatPgsql8,
		Events:    []string{"INSERT"},
		Timing:    model.TriggerTimingBefore,
		Table:     table.Name,
		ForEach:   model.TriggerForEachRow,
		Function:  partSchema.Name + ".insert_trigger()",
	})
}

func (self *XmlParser) checkModuloPartitionChange(oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) error {
	oldOpts := newModuloPartition(oldSchema, oldTable)
	newOpts := newModuloPartition(newSchema, newTable)
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
	return prefix + name[0:util.IntMin(remaining, len(name))] + suffix
}
