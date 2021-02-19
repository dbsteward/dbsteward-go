package pgsql8

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
	"github.com/pkg/errors"
)

type DiffTables struct {
}

func NewDiffTables() *DiffTables {
	return &DiffTables{}
}

// TODO(go,core) lift much of this up to sql99

// applies transformations to tables that exist in both old and new
func (self *DiffTables) DiffTables(stage1, stage3 output.OutputFileSegmenter, oldSchema, newSchema *model.Schema) error {
	// note: old dbsteward called create_tables here, but because we split out DiffTable, we can't call it both places,
	// so callers were updated to call CreateTables or CreateTable just before calling DiffTables or DiffTable, respectively

	if oldSchema == nil {
		return nil
	}
	for _, newTable := range newSchema.Tables {
		oldTable := oldSchema.TryGetTableNamed(newTable.Name)
		oldSchema, oldTable = lib.GlobalDBX.RenamedTableCheckPointer(oldSchema, oldTable, newSchema, newTable)
		err := self.DiffTable(stage1, stage3, oldSchema, oldTable, newSchema, newTable)
		if err != nil {
			return errors.Wrapf(err, "while diffing table %s.%s", newSchema.Name, newTable.Name)
		}
	}
	return nil
}

func (self *DiffTables) DiffTable(stage1, stage3 output.OutputFileSegmenter, oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) error {
	if oldTable == nil || newTable == nil {
		// create and drop are handled elsewhere
		return nil
	}

	err := self.updateTableOptions(stage1, oldSchema, oldTable, newSchema, newTable)
	if err != nil {
		return errors.Wrap(err, "while diffing table options")
	}
	err = self.updateTableColumns(stage1, stage3, oldTable, newSchema, newTable)
	if err != nil {
		return errors.Wrap(err, "while diffing table columns")
	}
	err = self.checkPartition(oldSchema, oldTable, newSchema, newTable)
	if err != nil {
		return errors.Wrap(err, "while diffing table partitions")
	}
	err = self.checkInherits(stage1, oldTable, newSchema, newTable)
	if err != nil {
		return errors.Wrap(err, "while diffing table inheritance")
	}
	err = self.addAlterStatistics(stage1, oldTable, newSchema, newTable)
	if err != nil {
		return errors.Wrap(err, "while diffing table statistics")
	}

	return nil
}

func (self *DiffTables) updateTableOptions(stage1 output.OutputFileSegmenter, oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) error {
	util.Assert(oldTable != nil, "expect oldTable to not be nil")
	util.Assert(newTable != nil, "expect newTable to not be nil")

	oldOpts := oldTable.GetTableOptionStrMap(model.SqlFormatPgsql8)
	newOpts := newTable.GetTableOptionStrMap(model.SqlFormatPgsql8)

	// dropped options are those present in old table but not new
	deleteOpts := util.IDifferenceStrMapKeys(oldOpts, newOpts)

	// added options are those present in new table but not old
	createOpts := util.IDifferenceStrMapKeys(newOpts, oldOpts)

	// changed options are those present in both tables but with different values
	updateOpts := util.IntersectStrMapFunc(newOpts, oldOpts, func(newKey, oldKey string) bool {
		return strings.EqualFold(newKey, oldKey) && !strings.EqualFold(newOpts[newKey], oldOpts[oldKey])
	})

	return self.applyTableOptionsDiff(stage1, newSchema, newTable, updateOpts, createOpts, deleteOpts)
}

func (self *DiffTables) applyTableOptionsDiff(stage1 output.OutputFileSegmenter, schema *model.Schema, table *model.Table, updateOpts, createOpts, deleteOpts map[string]string) error {
	alters := []sql.TableAlterPart{}
	ref := sql.TableRef{schema.Name, table.Name}

	// in pgsql create and alter have the same syntax
	for name, value := range util.IUnionStrMapKeys(createOpts, updateOpts) {
		if strings.EqualFold(name, "with") {
			// ALTER TABLE ... SET (params) doesn't accept oids=true/false unlike CREATE TABLE
			// only WITH OIDS or WITHOUT OIDS
			params := GlobalTable.ParseStorageParams(value)
			if oids, ok := params["oids"]; ok {
				delete(params, "oids")
				if util.IsTruthy(oids) {
					alters = append(alters, &sql.TableAlterPartWithOids{})
				} else {
					alters = append(alters, &sql.TableAlterPartWithoutOids{})
				}
			} else {
				// we might have gotten rid of the oids param
				alters = append(alters, &sql.TableAlterPartWithoutOids{})
			}

			// set rest of params normally
			alters = append(alters, &sql.TableAlterPartSetStorageParams{params})
		} else if strings.EqualFold(name, "tablespace") {
			alters = append(alters, &sql.TableAlterPartSetTablespace{value})
			// TODO(go,3) MoveTablespaceIndexes generates a whole function that just walks indexes and issues ALTER INDEXes. can we move that to this side?
			stage1.WriteSql(&sql.TableMoveTablespaceIndexes{
				Table:      ref,
				Tablespace: value,
			})
		} else {
			lib.GlobalDBSteward.Warning("Ignoring create/update of unknown table option %s on table %s.%s", name, schema.Name, table.Name)
		}
	}

	for name, value := range deleteOpts {
		if strings.EqualFold(name, "with") {
			params := GlobalTable.ParseStorageParams(value)
			// handle oids separately since pgsql doesn't recognize it as a storage parameter in an ALTER TABLE
			if _, ok := params["oids"]; ok {
				delete(params, "oids")
				alters = append(alters, &sql.TableAlterPartWithoutOids{})
			}
			// handle rest normally
			alters = append(alters, &sql.TableAlterPartResetStorageParams{util.StrMapKeys(params)})
		} else if strings.EqualFold(name, "tablespace") {
			stage1.WriteSql(&sql.TableResetTablespace{
				Table: ref,
			})
		} else {
			lib.GlobalDBSteward.Warning("Ignoring removal of unknown table option %s on table %s.%s", name, schema.Name, table.Name)
		}
	}

	if len(alters) > 0 {
		stage1.WriteSql(&sql.TableAlterParts{
			Table: ref,
			Parts: alters,
		})
	}

	return nil
}

type updateTableColumnsAgg struct {
	before1 []output.ToSql
	before3 []output.ToSql
	stage1  []sql.TableAlterPart
	stage3  []sql.TableAlterPart
	after1  []output.ToSql
	after3  []output.ToSql
}

func (self *DiffTables) updateTableColumns(stage1, stage3 output.OutputFileSegmenter, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) error {
	agg := &updateTableColumnsAgg{}

	// TODO(go,pgsql) old dbsteward interleaved commands into a single list, and output in the same order
	// meaning that a BEFORE3 could be output before a BEFORE1 in a single-stage upgrade. in this implementation,
	// _all_ BEFORE1s are printed before BEFORE3s. Double check that this doesn't break anything.

	err := self.addDropTableColumns(agg, oldTable, newTable)
	if err != nil {
		return err
	}
	err = self.addCreateTableColumns(agg, oldTable, newSchema, newTable)
	if err != nil {
		return err
	}
	err = self.addModifyTableColumns(agg, oldTable, newSchema, newTable)
	if err != nil {
		return err
	}

	// Note: in the case of single stage upgrades, stage1==stage3, so do all the Before's before all of the stages, and do them in stage order
	stage1.WriteSql(agg.before1...)
	stage3.WriteSql(agg.before3...)

	ref := sql.TableRef{newSchema.Name, newTable.Name}
	useReplicationOwner := false
	ownRole := newTable.Owner
	if ownRole == "" {
		ownRole = lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, model.RoleOwner)
	}
	if newTable.SlonyId != nil {
		// slony will make the alter table statement changes as its super user
		// which if the db owner is different,
		// implicit sequence creation will fail with:
		// ERROR:  55000: sequence must have same owner as table it is linked to
		// so if the alter statement contains a new serial column,
		// change the user to the slony user for the alter, then (see similar block below)
		for _, part := range agg.stage1 {
			// unwrap annotations
			if annot, ok := part.(*sql.TableAlterPartAnnotation); ok {
				part = annot.Wrapped
			}

			// inspect the alter table parts for indications that we're creating a serial column
			switch pt := part.(type) {
			case *sql.TableAlterPartColumnCreate:
				if GlobalDataType.IsSerialType(pt.ColumnDef.Type.Type) {
					useReplicationOwner = true
				}
			case *sql.TableAlterPartColumnChangeType:
				if GlobalDataType.IsSerialType(pt.Type.Type) {
					useReplicationOwner = true
				}
			}

			if useReplicationOwner {
				break
			}
		}

		repRole := lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, model.RoleReplication)
		if repRole == "" || repRole == ownRole {
			useReplicationOwner = false
		}

		if useReplicationOwner {
			stage1.WriteSql(&sql.Annotated{
				Annotation: "postgres needs to be appeased by making the owner the user we are executing as when pushing DDL through slony",
				Wrapped: &sql.TableAlterOwner{
					Table: ref,
					Role:  repRole,
				},
			})
		}
	}
	if len(agg.stage1) > 0 {
		stage1.WriteSql(&sql.TableAlterParts{
			Table: ref,
			Parts: agg.stage1,
		})
	}
	if useReplicationOwner {
		// replicated table? put ownership back
		stage1.WriteSql(&sql.Annotated{
			Annotation: "postgresql has been appeased, see above",
			Wrapped: &sql.TableAlterOwner{
				Table: ref,
				Role:  ownRole,
			},
		})
	}
	if len(agg.stage3) > 0 {
		stage3.WriteSql(&sql.TableAlterParts{
			Table: ref,
			Parts: agg.stage3,
		})
	}

	stage1.WriteSql(agg.after1...)
	stage3.WriteSql(agg.after3...)

	return nil
}

func (self *DiffTables) addDropTableColumns(agg *updateTableColumnsAgg, oldTable, newTable *model.Table) error {
	for _, oldColumn := range oldTable.Columns {
		if newTable.TryGetColumnNamed(oldColumn.Name) != nil {
			// new column exists, not dropping it
			continue
		}

		renamedColumn := newTable.TryGetColumnOldNamed(oldColumn.Name)
		if !lib.GlobalDBSteward.IgnoreOldNames && renamedColumn != nil {
			agg.after3 = append(agg.after3, sql.NewComment(
				"%s DROP COLUMN %s omitted: new column %s indicates it is the replacement for %s",
				oldTable.Name, oldColumn.Name, renamedColumn.Name, oldColumn.Name,
			))
		} else {
			agg.stage3 = append(agg.stage3, &sql.TableAlterPartColumnDrop{oldColumn.Name})
		}
	}
	return nil
}

func (self *DiffTables) addCreateTableColumns(agg *updateTableColumnsAgg, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) error {
	// note that postgres treats identifiers as case-sensitive when quoted
	// TODO(go,3) find a way to generalize/streamline this
	caseSensitive := lib.GlobalDBSteward.QuoteAllNames || lib.GlobalDBSteward.QuoteColumnNames

	for _, newColumn := range newTable.Columns {
		if oldTable.TryGetColumnNamedCase(newColumn.Name, caseSensitive) != nil {
			// old column exists, nothing to create
			continue
		}

		isRenamed, err := self.IsRenamedColumn(oldTable, newTable, newColumn)
		if err != nil {
			return errors.Wrapf(err, "while adding new table columns")
		}
		if isRenamed {
			agg.after1 = append(agg.after1, &sql.Annotated{
				Annotation: "column rename from oldColumnName specification",
				Wrapped: &sql.ColumnRename{
					Column:  sql.ColumnRef{newSchema.Name, newTable.Name, newColumn.OldColumnName},
					NewName: newColumn.Name,
				},
			})
			continue
		}

		// notice $include_null_definition is false
		// this is because ADD COLUMNs with NOT NULL will fail when there are existing rows
		agg.stage1 = append(agg.stage1, &sql.TableAlterPartColumnCreate{
			// TODO(go,nth) clean up this call, get rid of booleans and global flag
			ColumnDef: GlobalColumn.GetFullDefinition(lib.GlobalDBSteward.NewDatabase, newSchema, newTable, newColumn, false, true),
		})

		// instead we put the NOT NULL defintion in stage3 schema changes once data has been updated in stage2 data
		if !newColumn.Nullable {
			agg.stage3 = append(agg.stage3, &sql.TableAlterPartColumnSetNull{
				Column:   newColumn.Name,
				Nullable: false,
			})
			// also, if it's defined, default the column in stage 1 so the SET NULL will actually pass in stage 3
			if newColumn.Default != "" {
				agg.after1 = append(agg.after1, &sql.DataUpdate{
					Table:          sql.TableRef{newSchema.Name, newTable.Name},
					UpdatedColumns: []string{newColumn.Name},
					UpdatedValues:  []sql.ToSqlValue{sql.ValueDefault},
					KeyColumns:     []string{newColumn.Name},
					KeyValues:      []sql.ToSqlValue{sql.ValueNull},
				})
			}
		}

		// FS#15997 - dbsteward - replica inconsistency on added new columns with default now()
		// slony replicas that add columns via DDL that have a default of NOW() will be out of sync
		// because the data in those columns is being placed in as a default by the local db server
		// to compensate, add UPDATE statements to make the these column's values NOW() from the master
		if GlobalColumn.HasDefaultNow(newTable, newColumn) {
			agg.after1 = append(agg.after1, &sql.Annotated{
				Annotation: "has_default_now: this statement is to make sure new columns are in sync on replicas",
				Wrapped: &sql.DataUpdate{
					Table:          sql.TableRef{newSchema.Name, newTable.Name},
					UpdatedColumns: []string{newColumn.Name},
					UpdatedValues:  []sql.ToSqlValue{sql.RawSql(newColumn.Default)},
				},
			})
		}

		// some columns need to be filled with values before any new constraints can be applied
		// this is accomplished by defining arbitrary SQL in the column element afterAddPre/PostStageX attribute
		// TODO(go,nth) original code re-traverses doc->schema->table->column, and I'm not sure why; need to make sure this is well tested and reviewed
		if newColumn.BeforeAddStage1 != "" {
			agg.before1 = append(agg.before1, &sql.Annotated{
				Annotation: fmt.Sprintf("from %s.%s.%s beforeAddStage1 definition", newSchema.Name, newTable.Name, newColumn.Name),
				Wrapped:    sql.RawSql(newColumn.BeforeAddStage1),
			})
		}
		if newColumn.AfterAddStage1 != "" {
			agg.after1 = append(agg.after1, &sql.Annotated{
				Annotation: fmt.Sprintf("from %s.%s.%s afterAddStage1 definition", newSchema.Name, newTable.Name, newColumn.Name),
				Wrapped:    sql.RawSql(newColumn.AfterAddStage1),
			})
		}
		if newColumn.BeforeAddStage3 != "" {
			agg.before1 = append(agg.before1, &sql.Annotated{
				Annotation: fmt.Sprintf("from %s.%s.%s beforeAddStage3 definition", newSchema.Name, newTable.Name, newColumn.Name),
				Wrapped:    sql.RawSql(newColumn.BeforeAddStage3),
			})
		}
		if newColumn.AfterAddStage3 != "" {
			agg.after1 = append(agg.after1, &sql.Annotated{
				Annotation: fmt.Sprintf("from %s.%s.%s afterAddStage3 definition", newSchema.Name, newTable.Name, newColumn.Name),
				Wrapped:    sql.RawSql(newColumn.AfterAddStage3),
			})
		}
	}

	return nil
}

func (self *DiffTables) addModifyTableColumns(agg *updateTableColumnsAgg, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) error {
	dbsteward := lib.GlobalDBSteward

	// note that postgres treats identifiers as case-sensitive when quoted
	// TODO(go,3) find a way to generalize/streamline this
	caseSensitive := dbsteward.QuoteAllNames || dbsteward.QuoteColumnNames

	for _, newColumn := range newTable.Columns {
		oldColumn := oldTable.TryGetColumnNamedCase(newColumn.Name, caseSensitive)
		if oldColumn == nil {
			// old table does not contain column, CREATE handled by addCreateTableColumns
			continue
		}
		isRenamed, err := self.IsRenamedColumn(oldTable, newTable, newColumn)
		if err != nil {
			return errors.Wrapf(err, "while diffing table columns")
		}
		if isRenamed {
			// column is renamed, RENAME is handled by addCreateTableColumns
			// TODO(feat) doens't this mean the ONLY change to a renamed column is the RENAME? That doesn't seem right, could lead to bad data
			continue
		}

		// TODO(go,pgsql) orig code calls (oldDB, *newSchema*, oldTable, oldColumn) but that seems wrong, need to validate this
		oldType := GlobalColumn.GetColumnType(dbsteward.OldDatabase, newSchema, oldTable, oldColumn)
		newType := GlobalColumn.GetColumnType(dbsteward.NewDatabase, newSchema, newTable, newColumn)

		if !GlobalDataType.IsLinkedTableType(oldType) && GlobalDataType.IsLinkedTableType(newType) {
			// TODO(feat) can we remove this restriction? or is this a postgres thing?
			return errors.Errorf(
				"Column %s.%s.%s has linked type %s. Column types cannot be altered to serial. If this column cannot be recreated as part of database change control, a user defined serial should be created, and corresponding nextval() defined as the default for the column.",
				newSchema.Name, newTable.Name, newColumn.Name, newType,
			)
		}

		// TODO(feat) should this be case-insensitive?
		if oldType != newType {
			// ALTER TYPE ... USING support by looking up the new type in the xml definition
			alterType := &sql.TableAlterPartColumnChangeType{
				Column: newColumn.Name,
				Type:   sql.ParseTypeRef(newType),
			}
			if newColumn.ConvertUsing != "" {
				expr := sql.ExpressionValue(newColumn.ConvertUsing)
				alterType.Using = &expr
			}
			agg.stage1 = append(agg.stage1, &sql.TableAlterPartAnnotation{
				Annotation: "changing from type " + oldType,
				Wrapped:    alterType,
			})
		}

		if oldColumn.Default != newColumn.Default {
			if newColumn.Default == "" {
				agg.stage1 = append(agg.stage1, &sql.TableAlterPartColumnDropDefault{newColumn.Name})
			} else {
				agg.stage1 = append(agg.stage1, &sql.TableAlterPartColumnSetDefault{newColumn.Name, sql.RawSql(newColumn.Default)})
			}
		}

		if oldColumn.Nullable != newColumn.Nullable {
			if newColumn.Nullable {
				agg.stage1 = append(agg.stage1, &sql.TableAlterPartColumnSetNull{newColumn.Name, true})
			} else {
				// if the default value is defined in the dbsteward XML
				// set the value of the column to the default in end of stage 1 so that NOT NULL can be applied in stage 3
				// this way custom <sql> tags can be avoided for upgrade generation if defaults are specified
				if newColumn.Default != "" {
					agg.after1 = append(agg.after1, &sql.Annotated{
						Annotation: "make modified column that is null the default value before NOT NULL hits",
						Wrapped: &sql.DataUpdate{
							Table:          sql.TableRef{newSchema.Name, newTable.Name},
							UpdatedColumns: []string{newColumn.Name},
							UpdatedValues:  []sql.ToSqlValue{sql.RawSql(newColumn.Default)},
							KeyColumns:     []string{newColumn.Name},
							KeyValues:      []sql.ToSqlValue{sql.ValueNull},
						},
					})
				}

				agg.stage3 = append(agg.stage3, &sql.TableAlterPartColumnSetNull{newColumn.Name, false})
			}
		}

		// drop sequence and default if converting from serial to int
		if GlobalDataType.IsSerialType(oldColumn.Type) && GlobalDataType.IsIntType(newColumn.Type) {
			agg.before3 = append(agg.before3, &sql.SequenceDrop{
				Sequence: sql.SequenceRef{
					Schema:   newSchema.Name,
					Sequence: GlobalOperations.BuildSequenceName(newSchema.Name, newTable.Name, newColumn.Name),
				},
			})
			agg.stage1 = append(agg.stage1, &sql.TableAlterPartColumnDropDefault{newColumn.Name})
		}
	}

	return nil
}

func (self *DiffTables) checkPartition(oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) error {
	if oldTable.Partitioning == nil && newTable.Partitioning == nil {
		return nil
	}
	if oldTable.Partitioning == nil || newTable.Partitioning == nil {
		// TODO(go,3) can we make this happen?
		return errors.Errorf("Changing partition status of a table may lead to data loss: %s.%s", oldSchema.Name, oldTable.Name)
	}
	if newTable.OldTableName != "" {
		// TODO(go,3) can it be?
		return errors.Errorf("Changing a parititioned table's name is not supported: %s.%s", oldSchema.Name, oldTable.Name)
	}
	// XmlParser has the rest of this knowledge
	return GlobalXmlParser.CheckPartitionChange(oldSchema, oldTable, newSchema, newTable)
}

func (self *DiffTables) checkInherits(stage1 output.OutputFileSegmenter, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) error {
	if oldTable.InheritsSchema == "" && oldTable.InheritsTable == "" && newTable.InheritsSchema == "" && newTable.InheritsTable == "" {
		return nil
	}

	if (oldTable.InheritsSchema == "" && oldTable.InheritsTable == "") != (newTable.InheritsSchema == "" && newTable.InheritsTable == "") {
		return errors.Errorf("Changing table inheritance is not supported in %s.%s", newSchema.Name, newTable.Name)
	}

	return nil
}

func (self *DiffTables) addAlterStatistics(stage1 output.OutputFileSegmenter, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) error {
	for _, newColumn := range newTable.Columns {
		oldColumn := oldTable.TryGetColumnNamed(newColumn.Name)
		if oldColumn == nil {
			continue
		}

		if newColumn.Statistics != nil && (oldColumn.Statistics == nil || *oldColumn.Statistics != *newColumn.Statistics) {
			stage1.WriteSql(&sql.ColumnAlterStatistics{
				Column:     sql.ColumnRef{newSchema.Name, newTable.Name, newColumn.Name},
				Statistics: *newColumn.Statistics,
			})
		} else if oldColumn.Statistics != nil && newColumn.Statistics == nil {
			stage1.WriteSql(&sql.ColumnAlterStatistics{
				Column:     sql.ColumnRef{newSchema.Name, newTable.Name, newColumn.Name},
				Statistics: -1,
			})
		}
	}
	return nil
}

func (self *DiffTables) IsRenamedTable(schema *model.Schema, table *model.Table) (bool, error) {
	if lib.GlobalDBSteward.IgnoreOldNames {
		return false, nil
	}
	if table.OldTableName == "" {
		return false, nil
	}
	if schema.TryGetTableNamed(table.OldTableName) != nil {
		// TODO(feat) what if the table moves schemas?
		// TODO(feat) what if we move a table and replace it with a table of the same name?
		return true, errors.Errorf("oldTableName panic - new schema %s still contains table named %s", schema.Name, table.OldTableName)
	}

	oldSchema := GlobalTable.GetOldTableSchema(schema, table)
	if oldSchema != nil {
		if oldSchema.TryGetTableNamed(table.OldTableName) == nil {
			return true, errors.Errorf("oldTableName panic - old schema %s does not contain table named %s", oldSchema.Name, table.OldTableName)
		}
	}

	// it is a new old named table rename if:
	// table.OldTableName exists in old schema
	// table.OldTableName does not exist in new schema
	if oldSchema.TryGetTableNamed(table.OldTableName) != nil && schema.TryGetTableNamed(table.OldTableName) == nil {
		lib.GlobalDBSteward.Info("Table %s used to be called %s", table.Name, table.OldTableName)
		return true, nil
	}
	return false, nil
}

func (self *DiffTables) IsRenamedColumn(oldTable, newTable *model.Table, newColumn *model.Column) (bool, error) {
	dbsteward := lib.GlobalDBSteward
	if dbsteward.IgnoreOldNames {
		return false, nil
	}

	caseSensitive := false
	if dbsteward.QuoteColumnNames || dbsteward.QuoteAllNames || dbsteward.SqlFormat.Equals(model.SqlFormatMysql5) {
		for _, oldColumn := range oldTable.Columns {
			if strings.EqualFold(oldColumn.Name, newColumn.Name) {
				if oldColumn.Name != newColumn.Name && newColumn.OldColumnName == "" {
					return true, errors.Errorf(
						"Ambiguous operation! It looks like column name case changed between old_column %s.%s and new_column %s.%s",
						oldTable.Name, oldColumn.Name, newTable.Name, newColumn.Name,
					)
				}
				break
			}
		}
		caseSensitive = true
	}
	if newColumn.OldColumnName == "" {
		return false, nil
	}
	if newTable.TryGetColumnNamedCase(newColumn.OldColumnName, caseSensitive) != nil {
		// TODO(feat) what if we are both renaming the old column and creating a new one with the old name?
		return true, errors.Errorf("oldColumnName panic - new table %s still contains column named %s", newTable.Name, newColumn.OldColumnName)
	}
	if oldTable.TryGetColumnNamedCase(newColumn.OldColumnName, caseSensitive) == nil {
		return true, errors.Errorf("oldColumnName panic - old table %s does not contain column named %s", oldTable.Name, newColumn.OldColumnName)
	}

	// it is a new old named table rename if:
	// newColumn.OldColumnName exists in old schema
	// newColumn.OldColumnName does not exist in new schema
	if oldTable.TryGetColumnNamedCase(newColumn.OldColumnName, caseSensitive) != nil && newTable.TryGetColumnNamedCase(newColumn.OldColumnName, caseSensitive) == nil {
		dbsteward.Info("Column %s.%s used to be called %s", newTable.Name, newColumn.Name, newColumn.OldColumnName)
		return true, nil
	}
	return false, nil
}

func (self *DiffTables) CreateTables(ofs output.OutputFileSegmenter, oldSchema, newSchema *model.Schema) error {
	if newSchema == nil {
		// if the new schema is nil, there's no tables to create
		return nil
	}
	for _, newTable := range newSchema.Tables {
		err := self.CreateTable(ofs, oldSchema, newSchema, newTable)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *DiffTables) CreateTable(ofs output.OutputFileSegmenter, oldSchema, newSchema *model.Schema, newTable *model.Table) error {
	if newTable == nil {
		// TODO(go,nth) we shouldn't be here? should this be an Assert?
		return nil
	}
	if oldSchema.TryGetTableNamed(newTable.Name) != nil {
		// old table exists, alters or drops will be handled by other code
		return nil
	}

	isRenamed, err := self.IsRenamedTable(newSchema, newTable)
	if err != nil {
		return err
	}
	if isRenamed {
		// this is a renamed table, so rename it instead of creating a new one
		oldTableSchema := GlobalTable.GetOldTableSchema(newSchema, newTable)
		oldTable := GlobalTable.GetOldTable(newSchema, newTable)

		// ALTER TABLE ... RENAME TO does not accept schema qualifiers ...
		oldRef := sql.TableRef{oldTableSchema.Name, oldTable.Name}
		ofs.WriteSql(&sql.Annotated{
			Annotation: "table rename from oldTableName specification",
			Wrapped: &sql.TableAlterRename{
				Table:   oldRef,
				NewName: newTable.Name,
			},
		})
		// ... so if the schema changes issue a SET SCHEMA
		if !strings.EqualFold(oldTableSchema.Name, newSchema.Name) {
			ofs.WriteSql(&sql.Annotated{
				Annotation: "table reschema from oldSchemaName specification",
				Wrapped: &sql.TableAlterSetSchema{
					Table:     oldRef,
					NewSchema: newSchema.Name,
				},
			})
		}
	} else {
		ofs.WriteSql(GlobalTable.GetCreationSql(newSchema, newTable)...)
		ofs.WriteSql(GlobalTable.DefineTableColumnDefaults(newSchema, newTable)...)
	}
	return nil
}

func (self *DiffTables) DropTables(ofs output.OutputFileSegmenter, oldSchema, newSchema *model.Schema) {
	// if newSchema is nil, we'll have already dropped all the tables in it
	if oldSchema != nil && newSchema != nil {
		for _, oldTable := range oldSchema.Tables {
			self.DropTable(ofs, oldSchema, oldTable, newSchema)
		}
	}
}

func (self *DiffTables) DropTable(ofs output.OutputFileSegmenter, oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema) {
	newTable := newSchema.TryGetTableNamed(oldTable.Name)
	if newTable != nil {
		// table exists, nothing to do
		return
	}
	if !lib.GlobalDBSteward.IgnoreOldNames {
		renamedRef := lib.GlobalDBX.TryGetTableFormerlyKnownAs(lib.GlobalDBSteward.NewDatabase, oldSchema, oldTable)
		if renamedRef != nil {
			ofs.Write("-- DROP TABLE %s.%s omitted: new table %s indicates it is her replacement", oldSchema.Name, oldTable.Name, renamedRef)
			return
		}
	}

	ofs.WriteSql(GlobalTable.GetDropSql(oldSchema, oldTable)...)
}

func (self *DiffTables) DiffClusters(ofs output.OutputFileSegmenter, oldSchema, newSchema *model.Schema) {
	for _, newTable := range newSchema.Tables {
		oldTable := oldSchema.TryGetTableNamed(newTable.Name)
		self.DiffClustersTable(ofs, oldSchema, oldTable, newSchema, newTable)
	}
}

func (self *DiffTables) DiffClustersTable(ofs output.OutputFileSegmenter, oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) {
	if (oldTable == nil && newTable.ClusterIndex != "") || (oldTable != nil && oldTable.ClusterIndex != newTable.ClusterIndex) {
		ofs.WriteSql(&sql.TableAlterClusterOn{
			Table: sql.TableRef{newSchema.Name, newTable.Name},
			Index: newTable.ClusterIndex,
		})
	}
}

func (self *DiffTables) DiffData(ofs output.OutputFileSegmenter, oldSchema, newSchema *model.Schema) {
	for _, newTable := range newSchema.Tables {
		isRenamed, err := self.IsRenamedTable(newSchema, newTable)
		lib.GlobalDBSteward.FatalIfError(err, "while diffing data")
		if isRenamed {
			// if the table was renamed, get old definition pointers, diff that
			oldSchema := GlobalTable.GetOldTableSchema(newSchema, newTable)
			oldTable := GlobalTable.GetOldTable(newSchema, newTable)
			ofs.WriteSql(self.GetCreateDataSql(oldSchema, oldTable, newSchema, newTable)...)
		} else {
			oldTable := oldSchema.TryGetTableNamed(newTable.Name)
			ofs.WriteSql(self.GetCreateDataSql(oldSchema, oldTable, newSchema, newTable)...)
		}
	}
}

func (self *DiffTables) GetCreateDataSql(oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) []output.ToSql {
	newRows, updatedRows := self.getNewAndChangedRows(oldTable, newTable)
	// cut back on allocations - we know that there's going to be _at least_ one statement for every new and updated row, and likely 1 for the serial start
	out := make([]output.ToSql, 0, len(newRows)+len(updatedRows)+1)

	for _, updatedRow := range updatedRows {
		out = append(out, self.buildDataUpdate(newSchema, newTable, updatedRow))
	}
	for _, newRow := range newRows {
		// TODO(go,3) batch inserts
		out = append(out, self.buildDataInsert(newSchema, newTable, newRow))
	}

	if oldTable == nil {
		// if this is a fresh build, make sure serial starts are issued _after_ the hardcoded data inserts
		out = append(out, GlobalTable.GetSerialStartDml(newSchema, newTable)...)
		return out
	}

	return out
}

func (self *DiffTables) GetDeleteDataSql(oldSchema *model.Schema, oldTable *model.Table, newSchema *model.Schema, newTable *model.Table) []output.ToSql {
	oldRows := self.getOldRows(oldTable, newTable)
	out := make([]output.ToSql, len(oldRows))
	for i, oldRow := range oldRows {
		out[i] = self.buildDataDelete(oldSchema, oldTable, oldRow)
	}
	return out
}

// TODO(go,3) all these row diffing functions feel awkward and too involved, let's see if we can't revisit these

// returns the rows in newTable which are new or updated, respectively, relative to oldTable
// TODO(go,3) move this to model
type changedRow struct {
	oldCols []string
	oldRow  *model.DataRow
	newRow  *model.DataRow
}

func (self *DiffTables) getNewAndChangedRows(oldTable, newTable *model.Table) ([]*model.DataRow, []*changedRow) {
	if newTable == nil || newTable.Rows == nil || len(newTable.Rows.Rows) == 0 || len(newTable.Rows.Columns) == 0 {
		// there are no new rows at all, so nothing is new or changed
		return nil, nil
	}

	if oldTable == nil || oldTable.Rows == nil || len(oldTable.Rows.Rows) == 0 || len(oldTable.Rows.Columns) == 0 {
		// there are no old rows at all, so everything is new, nothing is changed
		newRows := make([]*model.DataRow, len(newTable.Rows.Rows))
		copy(newRows, newTable.Rows.Rows)
		return newRows, nil
	}

	newRows := []*model.DataRow{}
	updatedRows := []*changedRow{}
	for _, newRow := range newTable.Rows.Rows {
		if newRow.Delete {
			// if the new row marked for deletion, it is neither new nor updated
			continue
		}
		oldRow := oldTable.Rows.TryGetRowMatchingKeyCols(newRow, newTable.PrimaryKey)
		if oldRow == nil {
			newRows = append(newRows, newRow)
		} else if !newTable.Rows.RowEquals(newRow, oldRow, oldTable.Rows.Columns) {
			updatedRows = append(updatedRows, &changedRow{
				oldCols: oldTable.Rows.Columns,
				oldRow:  oldRow,
				newRow:  newRow,
			})
		}
	}
	return newRows, updatedRows
}

// returns the rows in oldTable that are no longer in newTable
// TODO(go,3) move this to model?
func (self *DiffTables) getOldRows(oldTable, newTable *model.Table) []*model.DataRow {
	if oldTable == nil || oldTable.Rows == nil || len(oldTable.Rows.Rows) == 0 || len(oldTable.Rows.Columns) == 0 {
		// there are no old rows at all
		return nil
	}
	if newTable == nil || newTable.Rows == nil || len(newTable.Rows.Rows) == 0 || len(newTable.Rows.Columns) == 0 {
		// there are no new rows at all, so everything is old
		oldRows := make([]*model.DataRow, len(oldTable.Rows.Rows))
		copy(oldRows, oldTable.Rows.Rows)
		return oldRows
	}

	oldRows := []*model.DataRow{}
	for _, oldRow := range oldTable.Rows.Rows {
		if oldRow.Delete {
			// don't consider this row if it was deleted in old, regardless of status in new
			// TODO(go,pgsql) is this correct?
			continue
		}
		// NOTE: we use new primary key here, because new is new, baby
		newRow := newTable.Rows.TryGetRowMatchingKeyCols(oldRow, newTable.PrimaryKey)
		if newRow == nil || newRow.Delete {
			// if the new row is missing or marked for deletion, we want to drop it
			oldRows = append(oldRows, oldRow)
		}
		// don't bother checking for changes, that's handled by getNewAndUpdatedRows in a completely different codepath
	}
	return oldRows
}

func (self *DiffTables) buildDataInsert(schema *model.Schema, table *model.Table, row *model.DataRow) output.ToSql {
	util.Assert(table.Rows != nil, "table.Rows should not be nil when calling buildDataInsert")
	util.Assert(!row.Delete, "do not call buildDataInsert for a row marked for deletion")
	values := make([]sql.ToSqlValue, len(row.Columns))
	for i, col := range table.Rows.Columns {
		values[i] = GlobalOperations.ColumnValueDefault(schema, table, col, row.Columns[i])
	}
	return &sql.DataInsert{
		Table:   sql.TableRef{schema.Name, table.Name},
		Columns: table.Rows.Columns,
		Values:  values,
	}
}

func (self *DiffTables) buildDataUpdate(schema *model.Schema, table *model.Table, change *changedRow) output.ToSql {
	// TODO(feat) deal with column renames
	util.Assert(table.Rows != nil, "table.Rows should not be nil when calling buildDataUpdate")
	util.Assert(!change.newRow.Delete, "do not call buildDataUpdate for a row marked for deletion")

	updateCols := []string{}
	updateVals := []sql.ToSqlValue{}
	for i, newCol := range change.newRow.Columns {
		newColName := table.Rows.Columns[i]

		oldColIdx := util.IIndexOfStr(newColName, change.oldCols)
		if oldColIdx < 0 {
			lib.GlobalDBSteward.Fatal("Could not compare rows: could not find column %s in table %s.%s <rows columns>", newColName, schema.Name, table.Name)
		}
		oldCol := change.oldRow.Columns[oldColIdx]

		if !oldCol.Equals(newCol) {
			updateCols = append(updateCols, newColName)
			updateVals = append(updateVals, GlobalOperations.ColumnValueDefault(schema, table, newColName, newCol))
		}
	}

	keyVals := []sql.ToSqlValue{}
	pkCols, ok := table.Rows.TryGetColsMatchingKeyCols(change.newRow, table.PrimaryKey)
	if !ok {
		lib.GlobalDBSteward.Fatal("Could not compare rows: could not find primary key columns %v in <rows columns=%v> in table %s.%s", table.PrimaryKey, table.Rows.Columns, schema.Name, table.Name)
	}
	for i, pkCol := range pkCols {
		// TODO(go,pgsql) orig code in dbx::primary_key_expression uses `format::value_escape`, but that doesn't account for null, empty, sql, etc
		keyVals = append(keyVals, GlobalOperations.ColumnValueDefault(schema, table, table.PrimaryKey[i], pkCol))
	}

	return &sql.DataUpdate{
		Table:          sql.TableRef{schema.Name, table.Name},
		UpdatedColumns: updateCols,
		UpdatedValues:  updateVals,
		KeyColumns:     table.PrimaryKey,
		KeyValues:      keyVals,
	}
}

func (self *DiffTables) buildDataDelete(schema *model.Schema, table *model.Table, row *model.DataRow) output.ToSql {
	keyVals := []sql.ToSqlValue{}
	pkCols, ok := table.Rows.TryGetColsMatchingKeyCols(row, table.PrimaryKey)
	if !ok {
		lib.GlobalDBSteward.Fatal("Could not compare rows: could not find primary key columns %v in <rows columns=%v> in table %s.%s", table.PrimaryKey, table.Rows.Columns, schema.Name, table.Name)
	}
	for i, pkCol := range pkCols {
		// TODO(go,pgsql) orig code in dbx::primary_key_expression uses `format::value_escape`, but that doesn't account for null, empty, sql, etc
		keyVals = append(keyVals, GlobalOperations.ColumnValueDefault(schema, table, table.PrimaryKey[i], pkCol))
	}
	return &sql.DataDelete{
		Table:      sql.TableRef{schema.Name, table.Name},
		KeyColumns: table.PrimaryKey,
		KeyValues:  keyVals,
	}
}
