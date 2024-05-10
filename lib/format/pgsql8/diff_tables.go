package pgsql8

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
	"github.com/pkg/errors"
)

// TODO(go,core) lift much of this up to sql99

// applies transformations to tables that exist in both old and new
func diffTables(conf lib.Config, stage1, stage3 output.OutputFileSegmenter, oldSchema, newSchema *ir.Schema) error {
	// note: old dbsteward called create_tables here, but because we split out DiffTable, we can't call it both places,
	// so callers were updated to call createTables or CreateTable just before calling DiffTables or DiffTable, respectively

	if oldSchema == nil {
		return nil
	}
	for _, newTable := range newSchema.Tables {
		oldTable := oldSchema.TryGetTableNamed(newTable.Name)
		var err error
		oldSchema, oldTable, err = conf.OldDatabase.NewTableName(oldSchema, oldTable, newSchema, newTable)
		if err != nil {
			return err
		}
		err = diffTable(conf, stage1, stage3, oldSchema, oldTable, newSchema, newTable)
		if err != nil {
			return errors.Wrapf(err, "while diffing table %s.%s", newSchema.Name, newTable.Name)
		}
	}
	return nil
}

func diffTable(conf lib.Config, stage1, stage3 output.OutputFileSegmenter, oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) error {
	if oldTable == nil || newTable == nil {
		// create and drop are handled elsewhere
		return nil
	}

	err := updateTableOptions(conf.Logger, stage1, oldTable, newSchema, newTable)
	if err != nil {
		return errors.Wrap(err, "while diffing table options")
	}
	err = updateTableColumns(conf, stage1, stage3, oldTable, newSchema, newTable)
	if err != nil {
		return errors.Wrap(err, "while diffing table columns")
	}
	err = checkPartition(oldSchema, oldTable, newSchema, newTable)
	if err != nil {
		return errors.Wrap(err, "while diffing table partitions")
	}
	err = checkInherits(oldTable, newSchema, newTable)
	if err != nil {
		return errors.Wrap(err, "while diffing table inheritance")
	}
	err = addAlterStatistics(stage1, oldTable, newSchema, newTable)
	if err != nil {
		return errors.Wrap(err, "while diffing table statistics")
	}

	return nil
}

func updateTableOptions(l *slog.Logger, stage1 output.OutputFileSegmenter, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) error {
	util.Assert(oldTable != nil, "expect oldTable to not be nil")
	util.Assert(newTable != nil, "expect newTable to not be nil")

	oldOpts := oldTable.GetTableOptionStrMap(ir.SqlFormatPgsql8)
	newOpts := newTable.GetTableOptionStrMap(ir.SqlFormatPgsql8)

	// dropped options are those present in old table but not new
	deleteOpts := oldOpts.DifferenceFunc(newOpts, strings.EqualFold)

	// added options are those present in new table but not old
	createOpts := newOpts.DifferenceFunc(oldOpts, strings.EqualFold)

	// changed options are those present in both tables but with different values
	updateOpts := newOpts.IntersectFunc(oldOpts, func(newKey, oldKey string) bool {
		return strings.EqualFold(newKey, oldKey) && !strings.EqualFold(newOpts.Get(newKey), oldOpts.Get(newKey))
	})

	return applyTableOptionsDiff(l, stage1, newSchema, newTable, updateOpts, createOpts, deleteOpts)
}

func applyTableOptionsDiff(l *slog.Logger, stage1 output.OutputFileSegmenter, schema *ir.Schema, table *ir.Table, updateOpts, createOpts, deleteOpts *util.OrderedMap[string, string]) error {
	alters := []sql.TableAlterPart{}
	ref := sql.TableRef{Schema: schema.Name, Table: table.Name}

	// in pgsql create and alter have the same syntax
	for _, entry := range createOpts.UnionFunc(updateOpts, strings.EqualFold).Entries() {
		if strings.EqualFold(entry.Key, "with") {
			// ALTER TABLE ... SET (params) doesn't accept oids=true/false unlike CREATE TABLE
			// only WITH OIDS or WITHOUT OIDS
			params := parseStorageParams(entry.Value)
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
			alters = append(alters, &sql.TableAlterPartSetStorageParams{Params: params})
		} else if strings.EqualFold(entry.Key, "tablespace") {
			alters = append(alters, &sql.TableAlterPartSetTablespace{TablespaceName: entry.Value})
			// TODO(go,3) MoveTablespaceIndexes generates a whole function that just walks indexes and issues ALTER INDEXes. can we move that to this side?
			stage1.WriteSql(&sql.TableMoveTablespaceIndexes{
				Table:      ref,
				Tablespace: entry.Value,
			})
		} else {
			l.Warn(fmt.Sprintf("Ignoring create/update of unknown table option %s on table %s.%s", entry.Key, schema.Name, table.Name))
		}
	}

	for _, entry := range deleteOpts.Entries() {
		if strings.EqualFold(entry.Key, "with") {
			params := parseStorageParams(entry.Value)
			// handle oids separately since pgsql doesn't recognize it as a storage parameter in an ALTER TABLE
			if _, ok := params["oids"]; ok {
				delete(params, "oids")
				alters = append(alters, &sql.TableAlterPartWithoutOids{})
			}
			// handle rest normally
			alters = append(alters, &sql.TableAlterPartResetStorageParams{Params: util.MapKeys(params)})
		} else if strings.EqualFold(entry.Key, "tablespace") {
			stage1.WriteSql(&sql.TableResetTablespace{
				Table: ref,
			})
		} else {
			l.Warn(fmt.Sprintf("Ignoring removal of unknown table option %s on table %s.%s", entry.Key, schema.Name, table.Name))
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

func updateTableColumns(conf lib.Config, stage1, stage3 output.OutputFileSegmenter, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) error {
	agg := &updateTableColumnsAgg{}

	// TODO(go,pgsql) old dbsteward interleaved commands into a single list, and output in the same order
	// meaning that a BEFORE3 could be output before a BEFORE1 in a single-stage upgrade. in this implementation,
	// _all_ BEFORE1s are printed before BEFORE3s. Double check that this doesn't break anything.

	err := addDropTableColumns(conf, agg, oldTable, newTable)
	if err != nil {
		return err
	}
	err = addCreateTableColumns(conf, agg, oldTable, newSchema, newTable)
	if err != nil {
		return err
	}
	err = addModifyTableColumns(conf, agg, oldTable, newSchema, newTable)
	if err != nil {
		return err
	}

	// Note: in the case of single stage upgrades, stage1==stage3, so do all the Before's before all of the stages, and do them in stage order
	stage1.WriteSql(agg.before1...)
	stage3.WriteSql(agg.before3...)

	ref := sql.TableRef{Schema: newSchema.Name, Table: newTable.Name}
	// TODO: This code didn't do anything. Is there a bug hiding here?
	//ownRole := newTable.Owner
	//if ownRole == "" {
	//	ownRole = lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, ir.RoleOwner)
	//}
	if len(agg.stage1) > 0 {
		stage1.WriteSql(&sql.TableAlterParts{
			Table: ref,
			Parts: agg.stage1,
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

func addDropTableColumns(conf lib.Config, agg *updateTableColumnsAgg, oldTable, newTable *ir.Table) error {
	for _, oldColumn := range oldTable.Columns {
		if newTable.TryGetColumnNamed(oldColumn.Name) != nil {
			// new column exists, not dropping it
			continue
		}

		renamedColumn := newTable.TryGetColumnOldNamed(oldColumn.Name)
		if !conf.IgnoreOldNames && renamedColumn != nil {
			agg.after3 = append(agg.after3, sql.NewComment(
				"%s DROP COLUMN %s omitted: new column %s indicates it is the replacement for %s",
				oldTable.Name, oldColumn.Name, renamedColumn.Name, oldColumn.Name,
			))
		} else {
			agg.stage3 = append(agg.stage3, &sql.TableAlterPartColumnDrop{Column: oldColumn.Name})
		}
	}
	return nil
}

func addCreateTableColumns(conf lib.Config, agg *updateTableColumnsAgg, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) error {
	// note that postgres treats identifiers as case-sensitive when quoted
	// TODO(go,3) find a way to generalize/streamline this
	caseSensitive := conf.QuoteAllNames || conf.QuoteColumnNames

	for _, newColumn := range newTable.Columns {
		if oldTable.TryGetColumnNamedCase(newColumn.Name, caseSensitive) != nil {
			// old column exists, nothing to create
			continue
		}

		isRenamed, err := isRenamedColumn(conf, oldTable, newTable, newColumn)
		if err != nil {
			return errors.Wrapf(err, "while adding new table columns")
		}
		if isRenamed {
			agg.after1 = append(agg.after1, &sql.Annotated{
				Annotation: "column rename from oldColumnName specification",
				Wrapped: &sql.ColumnRename{
					Column:  sql.ColumnRef{Schema: newSchema.Name, Table: newTable.Name, Column: newColumn.OldColumnName},
					NewName: newColumn.Name,
				},
			})
			continue
		}

		// notice $include_null_definition is false
		// this is because ADD COLUMNs with NOT NULL will fail when there are existing rows
		colDef, err := getFullColumnDefinition(conf.Logger, conf.NewDatabase, newSchema, newTable, newColumn, false, true)
		if err != nil {
			return err
		}
		agg.stage1 = append(agg.stage1, &sql.TableAlterPartColumnCreate{
			// TODO(go,nth) clean up this call, get rid of booleans and global flag
			ColumnDef: colDef,
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
					Table:          sql.TableRef{Schema: newSchema.Name, Table: newTable.Name},
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
		if hasDefaultNow(newColumn) {
			agg.after1 = append(agg.after1, &sql.Annotated{
				Annotation: "has_default_now: this statement is to make sure new columns are in sync on replicas",
				Wrapped: &sql.DataUpdate{
					Table:          sql.TableRef{Schema: newSchema.Name, Table: newTable.Name},
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

func addModifyTableColumns(conf lib.Config, agg *updateTableColumnsAgg, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) error {
	// note that postgres treats identifiers as case-sensitive when quoted
	// TODO(go,3) find a way to generalize/streamline this
	caseSensitive := conf.QuoteAllNames || conf.QuoteColumnNames

	for _, newColumn := range newTable.Columns {
		oldColumn := oldTable.TryGetColumnNamedCase(newColumn.Name, caseSensitive)
		if oldColumn == nil {
			// old table does not contain column, CREATE handled by addCreateTableColumns
			continue
		}
		isRenamed, err := isRenamedColumn(conf, oldTable, newTable, newColumn)
		if err != nil {
			return errors.Wrapf(err, "while diffing table columns")
		}
		if isRenamed {
			// column is renamed, RENAME is handled by addCreateTableColumns
			// TODO(feat) doens't this mean the ONLY change to a renamed column is the RENAME? That doesn't seem right, could lead to bad data
			continue
		}

		// TODO(go,pgsql) orig code calls (oldDB, *newSchema*, oldTable, oldColumn) but that seems wrong, need to validate this
		oldType, err := getColumnType(conf.Logger, conf.OldDatabase, newSchema, oldTable, oldColumn)
		if err != nil {
			return err
		}
		newType, err := getColumnType(conf.Logger, conf.NewDatabase, newSchema, newTable, newColumn)
		if err != nil {
			return err
		}

		if !isLinkedTableType(oldType) && isLinkedTableType(newType) {
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
				agg.stage1 = append(agg.stage1, &sql.TableAlterPartColumnDropDefault{Column: newColumn.Name})
			} else {
				agg.stage1 = append(agg.stage1, &sql.TableAlterPartColumnSetDefault{Column: newColumn.Name, Default: sql.RawSql(newColumn.Default)})
			}
		}

		if oldColumn.Nullable != newColumn.Nullable {
			if newColumn.Nullable {
				agg.stage1 = append(agg.stage1, &sql.TableAlterPartColumnSetNull{Column: newColumn.Name, Nullable: true})
			} else {
				// if the default value is defined in the dbsteward XML
				// set the value of the column to the default in end of stage 1 so that NOT NULL can be applied in stage 3
				// this way custom <sql> tags can be avoided for upgrade generation if defaults are specified
				if newColumn.Default != "" {
					agg.after1 = append(agg.after1, &sql.Annotated{
						Annotation: "make modified column that is null the default value before NOT NULL hits",
						Wrapped: &sql.DataUpdate{
							Table:          sql.TableRef{Schema: newSchema.Name, Table: newTable.Name},
							UpdatedColumns: []string{newColumn.Name},
							UpdatedValues:  []sql.ToSqlValue{sql.RawSql(newColumn.Default)},
							KeyColumns:     []string{newColumn.Name},
							KeyValues:      []sql.ToSqlValue{sql.ValueNull},
						},
					})
				}

				agg.stage3 = append(agg.stage3, &sql.TableAlterPartColumnSetNull{Column: newColumn.Name, Nullable: false})
			}
		}

		// drop sequence and default if converting from serial to int
		if isSerialType(oldColumn.Type) && isIntType(newColumn.Type) {
			agg.before3 = append(agg.before3, &sql.SequenceDrop{
				Sequence: sql.SequenceRef{
					Schema:   newSchema.Name,
					Sequence: buildSequenceName(newSchema.Name, newTable.Name, newColumn.Name),
				},
			})
			agg.stage1 = append(agg.stage1, &sql.TableAlterPartColumnDropDefault{Column: newColumn.Name})
		}
	}

	return nil
}

func checkPartition(oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) error {
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
	xmlParser := NewXmlParser(quoter)
	return xmlParser.CheckPartitionChange(oldSchema, oldTable, newSchema, newTable)
}

func checkInherits(oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) error {
	if oldTable.InheritsSchema == "" && oldTable.InheritsTable == "" && newTable.InheritsSchema == "" && newTable.InheritsTable == "" {
		return nil
	}

	if (oldTable.InheritsSchema == "" && oldTable.InheritsTable == "") != (newTable.InheritsSchema == "" && newTable.InheritsTable == "") {
		return errors.Errorf("Changing table inheritance is not supported in %s.%s", newSchema.Name, newTable.Name)
	}

	return nil
}

func addAlterStatistics(stage1 output.OutputFileSegmenter, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) error {
	for _, newColumn := range newTable.Columns {
		oldColumn := oldTable.TryGetColumnNamed(newColumn.Name)
		if oldColumn == nil {
			continue
		}

		if newColumn.Statistics != nil && (oldColumn.Statistics == nil || *oldColumn.Statistics != *newColumn.Statistics) {
			stage1.WriteSql(&sql.ColumnAlterStatistics{
				Column:     sql.ColumnRef{Schema: newSchema.Name, Table: newTable.Name, Column: newColumn.Name},
				Statistics: *newColumn.Statistics,
			})
		} else if oldColumn.Statistics != nil && newColumn.Statistics == nil {
			stage1.WriteSql(&sql.ColumnAlterStatistics{
				Column:     sql.ColumnRef{Schema: newSchema.Name, Table: newTable.Name, Column: newColumn.Name},
				Statistics: -1,
			})
		}
	}
	return nil
}

func isRenamedColumn(conf lib.Config, oldTable, newTable *ir.Table, newColumn *ir.Column) (bool, error) {
	if conf.IgnoreOldNames {
		return false, nil
	}

	caseSensitive := false
	if conf.QuoteColumnNames || conf.QuoteAllNames || conf.SqlFormat.Equals(ir.SqlFormatMysql5) {
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
		conf.Logger.Info(fmt.Sprintf("Column %s.%s used to be called %s", newTable.Name, newColumn.Name, newColumn.OldColumnName))
		return true, nil
	}
	return false, nil
}

func createTables(conf lib.Config, ofs output.OutputFileSegmenter, oldSchema, newSchema *ir.Schema) error {
	if newSchema == nil {
		// if the new schema is nil, there's no tables to create
		return nil
	}
	for _, newTable := range newSchema.Tables {
		err := createTable(conf, ofs, oldSchema, newSchema, newTable)
		if err != nil {
			return err
		}
	}
	return nil
}

func createTable(conf lib.Config, ofs output.OutputFileSegmenter, oldSchema, newSchema *ir.Schema, newTable *ir.Table) error {
	l := conf.Logger.With(
		slog.String("function", "createTable()"),
		slog.String("old schema", oldSchema.Name),
		slog.String("new schema", newSchema.Name),
	)
	if newTable == nil {
		// TODO(go,nth) we shouldn't be here? should this be an Assert?
		l.Warn("empty table object")
		return nil
	}
	l = l.With(slog.String("new table", newTable.Name))
	if oldSchema.TryGetTableNamed(newTable.Name) != nil {
		// old table exists, alters or drops will be handled by other code
		l.Debug("old table exists")
		return nil
	}

	isRenamed, err := conf.OldDatabase.IsRenamedTable(slog.Default(), newSchema, newTable)
	if err != nil {
		return err
	}
	if isRenamed {
		l.Debug("table renamed")
		// this is a renamed table, so rename it instead of creating a new one
		oldTableSchema := conf.OldDatabase.GetOldTableSchema(newSchema, newTable)
		oldTable := conf.OldDatabase.GetOldTable(newSchema, newTable)

		// ALTER TABLE ... RENAME TO does not accept schema qualifiers ...
		oldRef := sql.TableRef{Schema: oldTableSchema.Name, Table: oldTable.Name}
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
		l.Debug("table not renamed")
		createTableSQL, err := getCreateTableSql(conf, newSchema, newTable)
		if err != nil {
			return err
		}
		err = ofs.WriteSql(createTableSQL...)
		if err != nil {
			return err
		}
		err = ofs.WriteSql(defineTableColumnDefaults(l, newSchema, newTable)...)
		if err != nil {
			return err
		}
	}
	return nil
}

func dropTables(conf lib.Config, ofs output.OutputFileSegmenter, oldSchema, newSchema *ir.Schema) {
	// if newSchema is nil, we'll have already dropped all the tables in it
	if oldSchema != nil && newSchema != nil {
		for _, oldTable := range oldSchema.Tables {
			dropTable(conf, ofs, oldSchema, oldTable, newSchema)
		}
	}
}

func dropTable(conf lib.Config, ofs output.OutputFileSegmenter, oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema) {
	newTable := newSchema.TryGetTableNamed(oldTable.Name)
	if newTable != nil {
		// table exists, nothing to do
		return
	}
	if !conf.IgnoreOldNames {
		renamedRef := conf.NewDatabase.TryGetTableFormerlyKnownAs(oldSchema, oldTable)
		if renamedRef != nil {
			ofs.WriteSql(sql.NewComment("DROP TABLE %s.%s omitted: new table %s indicates it is her replacement", oldSchema.Name, oldTable.Name, renamedRef))
			return
		}
	}

	ofs.WriteSql(getDropTableSql(oldSchema, oldTable)...)
}

func diffClusters(ofs output.OutputFileSegmenter, oldSchema, newSchema *ir.Schema) {
	for _, newTable := range newSchema.Tables {
		oldTable := oldSchema.TryGetTableNamed(newTable.Name)
		diffClustersTable(ofs, oldTable, newSchema, newTable)
	}
}

func diffClustersTable(ofs output.OutputFileSegmenter, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) {
	if (oldTable == nil && newTable.ClusterIndex != "") || (oldTable != nil && oldTable.ClusterIndex != newTable.ClusterIndex) {
		ofs.WriteSql(&sql.TableAlterClusterOn{
			Table: sql.TableRef{Schema: newSchema.Name, Table: newTable.Name},
			Index: newTable.ClusterIndex,
		})
	}
}

func diffData(ops *Operations, ofs output.OutputFileSegmenter, oldSchema, newSchema *ir.Schema) error {
	for _, newTable := range newSchema.Tables {
		isRenamed, err := ops.config.OldDatabase.IsRenamedTable(slog.Default(), newSchema, newTable)
		if err != nil {
			return fmt.Errorf("while diffing data: %w", err)
		}
		if isRenamed {
			// if the table was renamed, get old definition pointers, diff that
			oldSchema := ops.config.OldDatabase.GetOldTableSchema(newSchema, newTable)
			oldTable := ops.config.OldDatabase.GetOldTable(newSchema, newTable)
			s, err := getCreateDataSql(ops, oldSchema, oldTable, newSchema, newTable)
			if err != nil {
				return err
			}
			ofs.WriteSql(s...)
		} else {
			oldTable := oldSchema.TryGetTableNamed(newTable.Name)
			s, err := getCreateDataSql(ops, oldSchema, oldTable, newSchema, newTable)
			if err != nil {
				return err
			}
			ofs.WriteSql(s...)
		}
	}
	return nil
}

func getCreateDataSql(ops *Operations, oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) ([]output.ToSql, error) {
	newRows, updatedRows := getNewAndChangedRows(oldTable, newTable)
	// cut back on allocations - we know that there's going to be _at least_ one statement for every new and updated row, and likely 1 for the serial start
	out := make([]output.ToSql, 0, len(newRows)+len(updatedRows)+1)

	for _, updatedRow := range updatedRows {
		update, err := buildDataUpdate(ops, newSchema, newTable, updatedRow)
		if err != nil {
			return nil, err
		}
		out = append(out, update)
	}
	for _, newRow := range newRows {
		// TODO(go,3) batch inserts
		insert, err := buildDataInsert(ops, newSchema, newTable, newRow)
		if err != nil {
			return nil, err
		}
		out = append(out, insert)
	}

	if oldTable == nil {
		// if this is a fresh build, make sure serial starts are issued _after_ the hardcoded data inserts
		dml, err := getSerialStartDml(newSchema, newTable, nil)
		if err != nil {
			return nil, err
		}
		out = append(out, dml...)
		return out, nil
	}

	return out, nil
}

func getDeleteDataSql(ops *Operations, oldSchema *ir.Schema, oldTable *ir.Table, newSchema *ir.Schema, newTable *ir.Table) ([]output.ToSql, error) {
	oldRows := getOldRows(oldTable, newTable)
	out := make([]output.ToSql, len(oldRows))
	var err error
	for i, oldRow := range oldRows {
		out[i], err = buildDataDelete(ops, oldSchema, oldTable, oldRow)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// TODO(go,3) all these row diffing functions feel awkward and too involved, let's see if we can't revisit these

// returns the rows in newTable which are new or updated, respectively, relative to oldTable
// TODO(go,3) move this to model
type changedRow struct {
	oldCols []string
	oldRow  *ir.DataRow
	newRow  *ir.DataRow
}

func getNewAndChangedRows(oldTable, newTable *ir.Table) ([]*ir.DataRow, []*changedRow) {
	if newTable == nil || newTable.Rows == nil || len(newTable.Rows.Rows) == 0 || len(newTable.Rows.Columns) == 0 {
		// there are no new rows at all, so nothing is new or changed
		return nil, nil
	}

	if oldTable == nil || oldTable.Rows == nil || len(oldTable.Rows.Rows) == 0 || len(oldTable.Rows.Columns) == 0 {
		// there are no old rows at all, so everything is new, nothing is changed
		newRows := make([]*ir.DataRow, len(newTable.Rows.Rows))
		copy(newRows, newTable.Rows.Rows)
		return newRows, nil
	}

	newRows := []*ir.DataRow{}
	updatedRows := []*changedRow{}
	for _, newRow := range newTable.Rows.Rows {
		if newRow.Delete {
			// if the new row marked for deletion, it is neither new nor updated
			continue
		}

		newKeyCols := newTable.Rows.GetColMapKeys(newRow, newTable.PrimaryKey)
		oldRow := oldTable.Rows.TryGetRowMatchingColMap(newKeyCols)
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
func getOldRows(oldTable, newTable *ir.Table) []*ir.DataRow {
	if oldTable == nil || oldTable.Rows == nil || len(oldTable.Rows.Rows) == 0 || len(oldTable.Rows.Columns) == 0 {
		// there are no old rows at all
		return nil
	}
	if newTable == nil || newTable.Rows == nil || len(newTable.Rows.Rows) == 0 || len(newTable.Rows.Columns) == 0 {
		// there are no new rows at all, so everything is old
		oldRows := make([]*ir.DataRow, len(oldTable.Rows.Rows))
		copy(oldRows, oldTable.Rows.Rows)
		return oldRows
	}

	oldRows := []*ir.DataRow{}
	for _, oldRow := range oldTable.Rows.Rows {
		if oldRow.Delete {
			// don't consider this row if it was deleted in old, regardless of status in new
			// TODO(go,pgsql) is this correct?
			continue
		}
		// NOTE: we use new primary key here, because new is new, baby
		oldKeyVals := oldTable.Rows.GetColMapKeys(oldRow, newTable.PrimaryKey)
		newRow := newTable.Rows.TryGetRowMatchingColMap(oldKeyVals)
		if newRow == nil || newRow.Delete {
			// if the new row is missing or marked for deletion, we want to drop it
			oldRows = append(oldRows, oldRow)
		}
		// don't bother checking for changes, that's handled by getNewAndUpdatedRows in a completely different codepath
	}
	return oldRows
}

func buildDataInsert(ops *Operations, schema *ir.Schema, table *ir.Table, row *ir.DataRow) (output.ToSql, error) {
	util.Assert(table.Rows != nil, "table.Rows should not be nil when calling buildDataInsert")
	util.Assert(!row.Delete, "do not call buildDataInsert for a row marked for deletion")
	values := make([]sql.ToSqlValue, len(row.Columns))
	var err error
	for i, col := range table.Rows.Columns {
		values[i], err = ops.columnValueDefault(ops.logger, schema, table, col, row.Columns[i])
		if err != nil {
			return nil, err
		}
	}
	return &sql.DataInsert{
		Table:   sql.TableRef{Schema: schema.Name, Table: table.Name},
		Columns: table.Rows.Columns,
		Values:  values,
	}, nil
}

func buildDataUpdate(ops *Operations, schema *ir.Schema, table *ir.Table, change *changedRow) (output.ToSql, error) {
	// TODO(feat) deal with column renames
	util.Assert(table.Rows != nil, "table.Rows should not be nil when calling buildDataUpdate")
	util.Assert(!change.newRow.Delete, "do not call buildDataUpdate for a row marked for deletion")

	updateCols := []string{}
	updateVals := []sql.ToSqlValue{}
	for i, newCol := range change.newRow.Columns {
		newColName := table.Rows.Columns[i]

		oldColIdx := util.IStrsIndex(change.oldCols, newColName)
		if oldColIdx < 0 || !change.oldRow.Columns[oldColIdx].Equals(newCol) {
			updateCols = append(updateCols, newColName)
			cvd, err := ops.columnValueDefault(ops.logger, schema, table, newColName, newCol)
			if err != nil {
				return nil, err
			}
			updateVals = append(updateVals, cvd)
		}
	}

	keyVals := []sql.ToSqlValue{}
	pkColMap := table.Rows.GetColMapKeys(change.newRow, table.PrimaryKey)
	for name, col := range pkColMap {
		// TODO(go,pgsql) orig code in dbx::primary_key_expression uses `format::value_escape`, but that doesn't account for null, empty, sql, etc
		cvd, err := ops.columnValueDefault(ops.logger, schema, table, name, col)
		if err != nil {
			return nil, err
		}
		keyVals = append(keyVals, cvd)
	}

	return &sql.DataUpdate{
		Table:          sql.TableRef{Schema: schema.Name, Table: table.Name},
		UpdatedColumns: updateCols,
		UpdatedValues:  updateVals,
		KeyColumns:     table.PrimaryKey,
		KeyValues:      keyVals,
	}, nil
}

func buildDataDelete(ops *Operations, schema *ir.Schema, table *ir.Table, row *ir.DataRow) (output.ToSql, error) {
	keyVals := []sql.ToSqlValue{}
	pkColMap := table.Rows.GetColMapKeys(row, table.PrimaryKey)
	for name, col := range pkColMap {
		// TODO(go,pgsql) orig code in dbx::primary_key_expression uses `format::value_escape`, but that doesn't account for null, empty, sql, etc
		val, err := ops.columnValueDefault(ops.logger, schema, table, name, col)
		if err != nil {
			return nil, err
		}
		keyVals = append(keyVals, val)
	}
	return &sql.DataDelete{
		Table:      sql.TableRef{Schema: schema.Name, Table: table.Name},
		KeyColumns: table.PrimaryKey,
		KeyValues:  keyVals,
	}, nil
}
