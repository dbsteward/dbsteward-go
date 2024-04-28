package pgsql8

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/format/sql99"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
	"github.com/jackc/pgx/v4"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/ir"
)

type Operations struct {
	*sql99.Operations
	EscapeStringValues bool
	quoter             output.Quoter
}

func NewOperations() *Operations {
	pgsql := &Operations{
		Operations:         sql99.NewOperations(),
		EscapeStringValues: false,
	}
	pgsql.Operations.Operations = pgsql
	return pgsql
}

func (ops *Operations) GetQuoter() output.Quoter {
	// TODO(go,core) can we push this out to the GlobalLookup instance?
	if ops.quoter == nil {
		dbsteward := lib.GlobalDBSteward
		return &sql.Quoter{
			Logger:                         dbsteward,
			ShouldQuoteSchemaNames:         dbsteward.QuoteAllNames || dbsteward.QuoteSchemaNames,
			ShouldQuoteTableNames:          dbsteward.QuoteAllNames || dbsteward.QuoteTableNames,
			ShouldQuoteColumnNames:         dbsteward.QuoteAllNames || dbsteward.QuoteColumnNames,
			ShouldQuoteObjectNames:         dbsteward.QuoteAllNames || dbsteward.QuoteObjectNames,
			ShouldQuoteIllegalIdentifiers:  dbsteward.QuoteIllegalIdentifiers,
			ShouldQuoteReservedIdentifiers: dbsteward.QuoteReservedIdentifiers,
			ShouldEEscape:                  ops.EscapeStringValues,
			RequireVerboseIntervalNotation: dbsteward.RequireVerboseIntervalNotation,
		}
	}
	return ops.quoter
}

func (ops *Operations) CreateStatements(def ir.Definition) ([]output.DDLStatement, error) {
	ofs := output.NewSegmenter(ops.GetQuoter())
	err := ops.build(ofs, &def)
	if err != nil {
		return nil, err
	}
	return ofs.AllStatements(), nil
}

func (ops *Operations) Build(outputPrefix string, dbDoc *ir.Definition) {
	dbsteward := lib.GlobalDBSteward

	buildFileName := outputPrefix + "_build.sql"
	dbsteward.Info("Building complete file %s", buildFileName)

	buildFile, err := os.Create(buildFileName)
	dbsteward.FatalIfError(err, "Failed to open file %s for output", buildFileName)

	buildFileOfs := output.NewOutputFileSegmenterToFile(dbsteward, ops.GetQuoter(), buildFileName, 1, buildFile, buildFileName, dbsteward.OutputFileStatementLimit)
	err = ops.build(buildFileOfs, dbDoc)
	dbsteward.FatalIfError(err, "build error")
}

func (ops *Operations) build(buildFileOfs output.OutputFileSegmenter, dbDoc *ir.Definition) error {
	// TODO(go,4) can we just consider a build(def) to be diff(null, def)?
	// some shortcuts, since we're going to be typing a lot here
	dbsteward := lib.GlobalDBSteward
	dbx := lib.GlobalDBX

	if len(dbsteward.LimitToTables) == 0 {
		buildFileOfs.Write("-- full database definition file generated %s\n", time.Now().Format(time.RFC1123Z))
	}
	if !dbsteward.GenerateSlonik {
		buildFileOfs.Write("BEGIN;\n\n")
	}

	dbsteward.Info("Calculating table foreign dependency order...")
	tableDependency := dbx.TableDependencyOrder(dbDoc)

	// database-specific implementation code refers to dbsteward::$new_database when looking up roles/values/conflicts etc
	dbsteward.NewDatabase = dbDoc
	dbx.SetDefaultSchema(dbDoc, "public")

	// language definitions
	if dbsteward.CreateLanguages {
		for _, language := range dbDoc.Languages {
			buildFileOfs.WriteSql(getCreateLanguageSql(language)...)
		}
	}

	// by default, postgresql will validate the contents of LANGUAGE SQL functions during creation
	// because we are creating all functions before tables, this doesn't work when LANGUAGE SQL functions
	// refer to tables yet to be created.
	// scan language="sql" functions for <functionDefiniton>s that contain FROM (<TABLE>) statements
	setCheckFunctionBodies := true
	setCheckFunctionBodiesInfo := ""
outer:
	for _, schema := range dbDoc.Schemas {
		for _, function := range schema.Functions {
			if definition := function.TryGetDefinition(ir.SqlFormatPgsql8); definition != nil {
				if strings.EqualFold(definition.Language, "sql") {
					referenced := functionDefinitionReferencesTable(definition)
					if referenced == nil {
						continue
					}

					referencedSchema := dbDoc.TryGetSchemaNamed(referenced.Schema)
					if referencedSchema == nil {
						continue
					}

					referencedTable := referencedSchema.TryGetTableNamed(referenced.Table)
					if referencedTable == nil {
						continue
					}

					setCheckFunctionBodies = false
					setCheckFunctionBodiesInfo = fmt.Sprintf(
						"Detected LANGUAGE SQL function %s.%s referring to table %s.%s in the database definition",
						schema.Name, function.Name, referencedSchema.Name, referencedTable.Name,
					)
					break outer
				}
			}
		}
	}
	if !setCheckFunctionBodies {
		buildFileOfs.Write("\n")
		buildFileOfs.WriteSql(&sql.Annotated{
			Wrapped:    &sql.SetCheckFunctionBodies{Value: false},
			Annotation: setCheckFunctionBodiesInfo,
		})
		dbsteward.Info(setCheckFunctionBodiesInfo)
	}

	if dbsteward.OnlySchemaSql || !dbsteward.OnlyDataSql {
		dbsteward.Info("Defining structure")
		err := buildSchema(dbDoc, buildFileOfs, tableDependency)
		if err != nil {
			return err
		}
	}
	if !dbsteward.OnlySchemaSql || dbsteward.OnlyDataSql {
		dbsteward.Info("Defining data inserts")
		buildData(dbDoc, buildFileOfs, tableDependency)
	}
	dbsteward.NewDatabase = nil

	if !dbsteward.GenerateSlonik {
		buildFileOfs.Write("COMMIT;\n\n")
	}

	// TODO(go,slony)
	// if dbsteward.GenerateSlonik {}
	return nil
}

func (ops *Operations) BuildUpgrade(
	oldOutputPrefix string, oldCompositeFile string, oldDoc *ir.Definition, oldFiles []string,
	newOutputPrefix string, newCompositeFile string, newDoc *ir.Definition, newFiles []string,
) {
	upgradePrefix := newOutputPrefix + "_upgrade"

	lib.GlobalDBSteward.Info("Calculating old table foreign key dependency order...")
	differ.OldTableDependency = lib.GlobalDBX.TableDependencyOrder(oldDoc)

	lib.GlobalDBSteward.Info("Calculating new table foreign key dependency order...")
	differ.NewTableDependency = lib.GlobalDBX.TableDependencyOrder(newDoc)

	differ.DiffDoc(oldCompositeFile, newCompositeFile, oldDoc, newDoc, upgradePrefix)

	// TODO(go,slony)
	// if lib.GlobalDBSteward.GenerateSlonik {}
}

func (ops *Operations) ExtractSchemaConn(ctx context.Context, c *pgx.Conn) (*ir.Definition, error) {
	conn := &liveConnection{c}
	return ops.extractSchema(ctx, conn)
}

func (ops *Operations) ExtractSchema(host string, port uint, name, user, pass string) *ir.Definition {
	dbsteward := lib.GlobalDBSteward
	dbsteward.Notice("Connecting to pgsql8 host %s:%d database %s as %s", host, port, name, user)
	conn, err := newConnection(host, port, name, user, pass)
	dbsteward.FatalIfError(err, "connecting to database")
	// TODO(go,pgsql) this is deadlocking during a panic
	defer conn.disconnect()
	def, err := ops.extractSchema(context.TODO(), conn)
	dbsteward.FatalIfError(err, "extracting schema")
	def.Database.Roles = &ir.RoleAssignment{
		Application: user,
		Owner:       user,
		Replication: user,
		ReadOnly:    user,
	}
	return def
}

func (ops *Operations) extractSchema(ctx context.Context, conn *liveConnection) (*ir.Definition, error) {
	dbsteward := lib.GlobalDBSteward
	introspector := introspector{conn: conn}
	pgDoc, err := introspector.GetFullStructure(ctx)
	if err != nil {
		return nil, err
	}
	dbsteward.Info("Connected to database, server version %s", pgDoc.Version)
	return ops.pgToIR(pgDoc)
}

func (ops *Operations) pgToIR(pgDoc structure) (*ir.Definition, error) {
	dbsteward := lib.GlobalDBSteward
	doc := &ir.Definition{
		Database: &ir.Database{
			SqlFormat: ir.SqlFormatPgsql8,
		},
	}
	roles := newRoleIndex(pgDoc.Database.Owner)

	for _, schema := range pgDoc.Schemas {
		storeSchema(doc, roles, schema)
	}

	for _, pgTable := range pgDoc.Tables {
		schemaName := pgTable.Schema
		tableName := pgTable.Table

		dbsteward.Info("Analyze table options %s.%s", pgTable.Schema, pgTable.Table)
		schema := doc.TryGetSchemaNamed(schemaName)
		if schema == nil {
			return nil, fmt.Errorf("table '%s' references missing schema '%s'", tableName, schemaName)
		}

		// create the table in the schema space
		table := schema.TryGetTableNamed(tableName)
		util.Assert(table == nil, "table %s.%s already defined in xml object - unexpected", schema.Name, tableName)
		roles.registerRole(roleContextOwner, pgTable.Owner)
		table = &ir.Table{
			Name:        tableName,
			Owner:       pgTable.Owner,
			Description: pgTable.TableDescription,
		}
		schema.AddTable(table)

		// extract tablespace as a tableOption
		if pgTable.Tablespace != nil {
			table.SetTableOption(ir.SqlFormatPgsql8, "tablespace", *pgTable.Tablespace)
		}

		if len(pgTable.StorageOptions) > 0 {
			table.SetTableOption(ir.SqlFormatPgsql8, "with", "("+util.EncodeKV(pgTable.StorageOptions, ",", "=")+")")
		}

		// NEW(2): extract table inheritance. need this to complete example diffing validation
		if len(pgTable.ParentTables) > 1 {
			// TODO(go,4) remove this restriction
			dbsteward.Fatal("Unsupported: Table %s.%s inherits from more than one table: %v", schema.Name, table.Name, pgTable.ParentTables)
		}
		if len(pgTable.ParentTables) == 1 {
			parts := strings.Split(pgTable.ParentTables[0], ".")
			table.InheritsSchema = parts[0]
			table.InheritsTable = parts[1]
		}

		dbsteward.Info("Analyze table columns %s.%s", schema.Name, table.Name)
		// hasindexes | hasrules | hastriggers handled later
		for _, colRow := range pgTable.Columns {
			column := &ir.Column{
				Name:        colRow.Name,
				Description: colRow.Description, // note that column numbers are 1-indexed
				Type:        colRow.AttrType,
				// TODO(go,nth) legacy logic only ever sets nullable to false (pgsql8.php:1638) but that really doesn't seem correct to me. validate this
				Nullable: colRow.Nullable,
				// TODO(go,nth) how does this handle expression defaults?
				Default: colRow.Default,
			}
			table.AddColumn(column)

			// look for serial columns that are primary keys and collapse them down from integers with sequence defualts into serials
			// type int or bigint
			// is_nullable = NO
			// column_default starts with nextval and contains _seq
			// default will look like:    nextval('test_blah_seq'::regclass)
			// TODO(feat) this list of conditions is probably not sufficient to check for serials in all cases
			// TODO(go,nth) is there a better way to test this?
			// TODO(go,core) this is absolutely broken, need to fix; switch to prefix test for types, suffix test for seq, look at that column_default equalfold
			if (strings.EqualFold(column.Type, "integer") || strings.EqualFold(column.Type, "bigint")) &&
				!column.Nullable &&
				(util.IIndex(column.Default, "nextval") == 0 && util.IIndex(column.Default, "_seq") >= 0) {
				column.Type = "serial"
				if strings.EqualFold("column_default", "bigint") {
					column.Type = "bigserial"
				}
				// TODO(feat) legacy logic doesn't set default or nullable for serial types... is that correct?
				column.Nullable = false
				column.Default = ""
			}
		}

		dbsteward.Info("Analyze table indexes %s.%s", schema.Name, table.Name)
		for _, indexRow := range pgTable.Indexes {
			// If the index is unique on a single column, convert it to a unique constraint
			if len(indexRow.Dimensions) == 1 && indexRow.Unique {
				success := false
				for _, col := range table.Columns {
					if col.Name == indexRow.Dimensions[0] {
						success = true
						col.Unique = true
						break
					}
				}
				if !success {
					return nil, fmt.Errorf(
						"unique index %s references nonexistent column %s",
						indexRow.Name, indexRow.Dimensions[0],
					)
				}
			} else {
				index := &ir.Index{
					Name:   indexRow.Name,
					Using:  indexRow.UsingToIR(),
					Unique: indexRow.Unique,
				}
				for _, dim := range indexRow.Dimensions {
					index.AddDimension(dim)
				}
				if indexRow.Condition != "" {
					index.AddCondition(ir.SqlFormatPgsql8, indexRow.Condition)
				}
				table.AddIndex(index)
			}
		}
	}

	for _, sequence := range pgDoc.Sequences {
		schema := doc.TryGetSchemaNamed(sequence.Schema)
		if schema == nil {
			return nil, fmt.Errorf("sequence '%s' missing schema '%s'", sequence.Name, sequence.Schema)
		}
		schema.AddSequence(
			&ir.Sequence{
				Name:          sequence.Name,
				Description:   sequence.Description,
				Owner:         sequence.Owner,
				Cache:         util.OptFromSQLNullInt64(sequence.Cache),
				Start:         util.OptFromSQLNullInt64(sequence.Start),
				Min:           util.OptFromSQLNullInt64(sequence.Min),
				Max:           util.OptFromSQLNullInt64(sequence.Max),
				Increment:     util.OptFromSQLNullInt64(sequence.Increment),
				Cycle:         sequence.Cycled,
				OwnedBySchema: sequence.SerialSchema,
				OwnedByTable:  sequence.SerialTable,
				OwnedByColumn: sequence.SerialColumn,
			},
		)
	}

	for _, viewRow := range pgDoc.Views {
		dbsteward.Info("Analyze view %s.%s", viewRow.Schema, viewRow.Name)
		schema := doc.TryGetSchemaNamed(viewRow.Schema)
		if schema == nil {
			return nil, fmt.Errorf("view '%s' references missing schema '%s'", viewRow.Name, viewRow.Schema)
		}

		view := schema.TryGetViewNamed(viewRow.Name)
		util.Assert(view == nil, "view %s.%s already defined in XML object -- unexpected", schema.Name, viewRow.Name)
		roles.registerRole(roleContextOwner, viewRow.Owner)
		schema.AddView(&ir.View{
			Name:        viewRow.Name,
			Description: viewRow.Description,
			Owner:       viewRow.Owner,
			Queries: []*ir.ViewQuery{
				{
					SqlFormat: ir.SqlFormatPgsql8,
					Text:      viewRow.Definition,
				},
			},
		})
	}

	// for all schemas, all tables - get table constraints that are not type 'FOREIGN KEY'
	// TODO(go,4) support constraint deferredness
	for _, constraintRow := range pgDoc.Constraints {
		dbsteward.Info("Analyze table constraints %s.%s", constraintRow.Schema, constraintRow.Table)

		schema := doc.TryGetSchemaNamed(constraintRow.Schema)
		util.Assert(schema != nil, "failed to find schema %s for constraint in table %s", constraintRow.Schema, constraintRow.Table)

		table := schema.TryGetTableNamed(constraintRow.Table)
		util.Assert(table != nil, "failed to find table %s.%s for constraint", constraintRow.Schema, constraintRow.Table)

		switch strings.ToLower(constraintRow.Type) {
		case "p": // primary key
			table.PrimaryKey = constraintRow.Columns
			table.PrimaryKeyName = constraintRow.Name
		case "u": // unique
			table.AddConstraint(&ir.Constraint{
				Name:       constraintRow.Name,
				Type:       ir.ConstraintTypeUnique,
				Definition: fmt.Sprintf(`("%s")`, strings.Join(constraintRow.Columns, `", "`)),
			})
		case "c": // check
			// NEW(2) implementing CHECK constraint extraction
			// TODO(go,4) we have access to the columns affected by the constraint... can we utilize that somehow?
			table.AddConstraint(&ir.Constraint{
				Name:       constraintRow.Name,
				Type:       ir.ConstraintTypeCheck,
				Definition: *constraintRow.CheckDef,
			})
		default:
			dbsteward.Fatal("Unknown constraint_type %s", constraintRow.Type)
		}
	}

	fkRules := map[string]ir.ForeignKeyAction{
		"a": ir.ForeignKeyActionNoAction,
		"r": ir.ForeignKeyActionRestrict,
		"c": ir.ForeignKeyActionCascade,
		"n": ir.ForeignKeyActionSetNull,
		"d": ir.ForeignKeyActionSetDefault,
	}
	for _, fkRow := range pgDoc.ForeignKeys {
		if len(fkRow.LocalColumns) != len(fkRow.ForeignColumns) {
			dbsteward.Fatal(
				"Unexpected: Foreign key columns (%v) on %s.%s are mismatched with columns (%v) on %s.%s",
				fkRow.LocalColumns, fkRow.LocalSchema, fkRow.LocalTable,
				fkRow.ForeignColumns, fkRow.ForeignSchema, fkRow.ForeignTable,
			)
		}

		schema := doc.TryGetSchemaNamed(fkRow.LocalSchema)
		util.Assert(schema != nil, "failed to find schema %s for foreign key in table %s", fkRow.LocalSchema, fkRow.LocalTable)

		table := schema.TryGetTableNamed(fkRow.LocalTable)
		util.Assert(table != nil, "failed to find table %s.%s for foreign key", fkRow.LocalSchema, fkRow.LocalTable)

		if len(fkRow.LocalColumns) == 1 {
			// add inline on the column
			column := table.TryGetColumnNamed(fkRow.LocalColumns[0])
			util.Assert(column != nil, "failed to find column %s.%s.%s for foreign key", fkRow.LocalSchema, fkRow.LocalTable, fkRow.LocalColumns[0])

			column.ForeignSchema = fkRow.ForeignSchema
			column.ForeignTable = fkRow.ForeignTable
			column.ForeignColumn = fkRow.ForeignColumns[0]
			column.ForeignKeyName = fkRow.ConstraintName
			column.ForeignOnUpdate = fkRules[fkRow.UpdateRule]
			column.ForeignOnDelete = fkRules[fkRow.DeleteRule]

			// dbsteward fk columns aren't supposed to specify a type, they get it from the referenced column
			column.Type = ""
		} else if len(fkRow.LocalColumns) > 1 {
			table.AddForeignKey(&ir.ForeignKey{
				Columns:        fkRow.LocalColumns,
				ForeignSchema:  fkRow.ForeignSchema,
				ForeignTable:   fkRow.ForeignTable,
				ForeignColumns: fkRow.ForeignColumns,
				ConstraintName: fkRow.ConstraintName,
				OnUpdate:       fkRules[fkRow.UpdateRule],
				OnDelete:       fkRules[fkRow.DeleteRule],
			})
		}
	}

	// extract normal and trigger functions
	// NEW(2) no longer excludes trigger functions, but now ignores/warns on aggregate/window functions. added warning for c-lang functions
	// TODO(go,4) support aggregate/window, c functions
	for _, fnRow := range pgDoc.Functions {
		if fnRow.Type == "window" || fnRow.Type == "aggregate" {
			dbsteward.Warning("Ignoring %s function %s.%s, this is not currently supported by DBSteward", fnRow.Type, fnRow.Schema, fnRow.Name)
			continue
		}
		if fnRow.Language == "c" {
			dbsteward.Warning("Ignoring native (c) function %s.%s, this is not currently supported by DBSteward", fnRow.Schema, fnRow.Name)
			continue
		}
		dbsteward.Info("Analyze function %s.%s", fnRow.Schema, fnRow.Name)
		schema := doc.TryGetSchemaNamed(fnRow.Schema)
		if schema == nil {
			return nil, fmt.Errorf("function '%s' references missing schema '%s'", fnRow.Name, fnRow.Schema)
		}

		// TODO(feat) should we see if there's another function by this name already? that'd probably be unexpected, but would likely indicate a bug in our query
		roles.registerRole(roleContextOwner, fnRow.Owner)
		function := &ir.Function{
			Name:        fnRow.Name,
			Owner:       fnRow.Owner,
			Returns:     fnRow.Return,
			CachePolicy: fnRow.Volatility,
			Description: fnRow.Description,
			// TODO(feat): how is / figure out how to express securityDefiner attribute in the functions query
			Definitions: []*ir.FunctionDefinition{
				{
					SqlFormat: ir.SqlFormatPgsql8,
					Language:  fnRow.Language,
					Text:      strings.TrimSpace(fnRow.Source),
				},
			},
		}
		schema.AddFunction(function)

		for _, argsRow := range fnRow.Args {
			// TODO(feat) param direction?
			function.AddParameter(argsRow.Name, argsRow.Type, ir.FuncParamDir(argsRow.Direction))
		}
	}

	// TODO(go,nth) don't use *, name columns explicitly
	for _, triggerRow := range pgDoc.Triggers {
		dbsteward.Info("Analyze trigger %s.%s", triggerRow.Schema, triggerRow.Name)

		schema := doc.TryGetSchemaNamed(triggerRow.Schema)
		util.Assert(schema != nil, "failed to find schema %s for trigger on table %s", triggerRow.Schema, triggerRow.Table)

		table := schema.TryGetTableNamed(triggerRow.Table)
		util.Assert(table != nil, "failed to find table %s.%s for trigger", triggerRow.Schema, triggerRow.Table)

		// there is a row for each event_manipulation, so we need to aggregate them, see if the trigger already exists
		// TODO(go,nth) can we simplify this by adding a groupby in the query?
		trigger := schema.TryGetTriggerNamedForTable(triggerRow.Name, triggerRow.Table)
		if trigger == nil {
			trigger = &ir.Trigger{
				Name:      triggerRow.Name,
				SqlFormat: ir.SqlFormatPgsql8,
			}
			schema.AddTrigger(trigger)
		}

		// TODO(feat) what should happen if we have two events with different settings??
		// TODO(go,nth) validate string constant casts
		trigger.AddEvent(triggerRow.Event)
		trigger.Timing = ir.TriggerTiming(triggerRow.Timing)
		trigger.Table = triggerRow.Table
		trigger.ForEach = ir.TriggerForEach(triggerRow.Orientation)
		trigger.Function = strings.TrimSpace(util.IReplaceAll(triggerRow.Statement, "EXECUTE PROCEDURE", ""))
	}

	// Find table/view grants and save them in the roleIndex
	// TODO(go,3) can simplify this by array_agg(privilege_type)
	dbsteward.Info("Analyze table permissions")
	for _, grantRow := range pgDoc.TablePerms {
		schema := doc.TryGetSchemaNamed(grantRow.Schema)
		util.Assert(schema != nil, "failed to find schema %s for trigger on table %s", grantRow.Schema, grantRow.Table)

		relation := schema.TryGetRelationNamed(grantRow.Table) // relation = table|view
		util.Assert(relation != nil, "failed to find relation %s.%s for trigger", grantRow.Schema, grantRow.Table)

		// ignore owner roles; those permissions are implicitly assigned by ALTER ... OWNER
		if strings.EqualFold(relation.GetOwner(), grantRow.Grantee) {
			continue
		}
		roles.registerRole(roleContextGrant, grantRow.Grantee)
	}

	// analyze sequence grants and assign those to the roleIndex
	dbsteward.Info("Analyze isolated sequence permissions")
	for _, sequence := range pgDoc.Sequences {
		for _, grantRow := range sequence.ACL {
			// privileges for unassociated sequences are not listed in
			// information_schema.sequences; i think this is probably the most
			// accurate way to get sequence-level grants
			if grantRow == "" {
				continue
			}
			grantPerms := parseSequenceRelAcl(grantRow)
			for user := range grantPerms {
				roles.registerRole(roleContextGrant, user)
			}
		}
	}

	// Get any roles/grants on schemas and add to the roleIndex
	for _, permEntry := range pgDoc.SchemaPerms {
		if permEntry.Grantee == "public" {
			dbsteward.Warning("Ignoring grant on psuedo user \"public\"")
		} else {
			roles.registerRole(roleContextGrant, permEntry.Grantee)
		}
	}

	// The entire list of roles is now available to be analyzed
	doc.Database.Roles = roles.resolveRoles()

	// Add any grants on the schema to the IR
	// pseudo-roles may result in duplicate entries
	for k := range pgDoc.SchemaPerms {
		p := pgDoc.SchemaPerms[k]
		p.Grantee = roles.get(p.Grantee)
		pgDoc.SchemaPerms[k] = p
	}
	slices.SortFunc(pgDoc.SchemaPerms, func(a, b schemaPermEntry) int {
		c := cmp.Compare(a.Grantee, b.Grantee)
		if c == 0 {
			return cmp.Compare(a.Type, b.Type)
		}
		return c
	})
	pgDoc.SchemaPerms = slices.Compact(pgDoc.SchemaPerms)
	for _, permEntry := range pgDoc.SchemaPerms {
		if permEntry.Grantee != "public" {
			np := ir.Grant{
				Roles:       []string{roles.get(permEntry.Grantee)},
				Permissions: []string{permEntry.Type},
			}
			schema := doc.TryGetSchemaNamed(permEntry.Schema)
			if schema == nil {
				return nil, fmt.Errorf("perms found but no schema found for '%s'", permEntry.Schema)
			}
			schema.AddGrant(&np)
		}
	}

	// analyze sequence grants and assign those to the IR
	for _, pgSequence := range pgDoc.Sequences {
		for _, grantRow := range pgSequence.ACL {
			// privileges for unassociated sequences are not listed in
			// information_schema.sequences; i think this is probably the most
			// accurate way to get sequence-level grants
			if grantRow == "" {
				continue
			}
			schema, err := doc.GetSchemaNamed(pgSequence.Schema)
			if err != nil {
				return doc, fmt.Errorf(
					"sequence '%s' schema match: %w",
					pgSequence.Name, err,
				)
			}
			sequence := schema.TryGetSequenceNamed(pgSequence.Name)
			if sequence == nil {
				return doc, fmt.Errorf(
					"sequence '%s.%s' missing from IR",
					pgSequence.Schema, pgSequence.Name,
				)
			}
			grantPerms := parseSequenceRelAcl(grantRow)
			for user, perms := range grantPerms {
				grantee := roles.get(user)
				for _, perm := range perms {
					// TODO(feat) what about revokes?
					grants := sequence.GetGrantsForRole(grantee)
					var grant *ir.Grant
					if len(grants) == 0 {
						grant = &ir.Grant{
							Roles: []string{grantee},
						}
						sequence.AddGrant(grant)
					} else {
						grant = grants[0]
					}
					grant.AddPermission(perm)
				}
			}
		}
	}
	for _, schema := range doc.Schemas {
		schema.Owner = roles.get(schema.Owner) // Replace meta-role with actual role if needed
		for _, table := range schema.Tables {
			table.Owner = roles.get(table.Owner)
			// if table does not have a primary key defined, add placeholder
			if len(table.PrimaryKey) == 0 {
				table.PrimaryKey = []string{"dbsteward_primary_key_not_found"}
				tableNoticeDesc := fmt.Sprintf("DBSTEWARD_EXTRACTION_WARNING: primary key definition not found for %s.%s - placeholder has been specified for DTD validity", schema.Name, table.Name)
				dbsteward.Warning(tableNoticeDesc)
				if len(table.Description) == 0 {
					table.Description = tableNoticeDesc
				} else {
					table.Description += "; " + tableNoticeDesc
				}
			}

			// NEW(2) if the table inherits from the parent, remove any inherited objects
			if table.InheritsTable != "" || table.InheritsSchema != "" {
				parentRef := lib.GlobalDBX.ResolveSchemaTable(doc, schema, table.InheritsSchema, table.InheritsTable, "inheritance")
				for _, parentColumn := range parentRef.Table.Columns {
					column := table.TryGetColumnNamed(parentColumn.Name)
					if column != nil && column.EqualsInherited(parentColumn) {
						dbsteward.Debug("Dropping column %s.%s.%s inherited from parent %s.%s", schema.Name, table.Name, column.Name, parentRef.Schema.Name, parentRef.Table.Name)
						table.RemoveColumn(column)
					}
				}
			}
		}
	}

	// Assign roles to the IR
	for _, relationGrant := range pgDoc.TablePerms {
		schema, err := doc.GetSchemaNamed(relationGrant.Schema)
		if err != nil {
			return nil, fmt.Errorf("fetching schema %s: %w", relationGrant.Schema, err)
		}
		relation := schema.TryGetRelationNamed(relationGrant.Table) // relation = table|view
		util.Assert(relation != nil, "failed to find relation %s.%s for trigger", relationGrant.Schema, relationGrant.Table)

		// ignore owner roles; those permissions are implicitly assigned by ALTER ... OWNER
		if strings.EqualFold(roles.get(relation.GetOwner()), roles.get(relationGrant.Grantee)) {
			continue
		}

		// aggregate privileges by role
		grantee := roles.get(relationGrant.Grantee)
		docGrants := relation.GetGrantsForRole(grantee)
		var grant *ir.Grant
		if len(docGrants) == 0 {
			grant = &ir.Grant{
				Roles: []string{grantee},
			}
			relation.AddGrant(grant)
		} else {
			grant = docGrants[0]
		}
		grant.AddPermission(relationGrant.Type)
		// TODO(feat) what should happen if two grants for the same role have different is_grantable?
		// TODO(feat) what about other WITH flags?
		grant.SetCanGrant(relationGrant.Grantable)
	}

	return doc, nil
}

// storeSchema creates a schema record and stores it in the IR
// Ensures the schema's owner is registered with the roleIndex
func storeSchema(doc *ir.Definition, roles *roleIndex, schema schemaEntry) {
	irSchema := &ir.Schema{
		Name:        schema.Name,
		Description: schema.Description,
		Owner:       schema.Owner,
	}
	doc.AddSchema(irSchema)
	roles.registerRole(roleContextOwner, schema.Owner)
}

func (ops *Operations) CompareDbData(doc *ir.Definition, host string, port uint, name, user, pass string) *ir.Definition {
	dbsteward := lib.GlobalDBSteward

	dbsteward.Notice("Connecting to pgsql8 host %s:%d database %s as user %s", host, port, name, user)
	conn, err := newConnection(host, port, name, user, pass)
	dbsteward.FatalIfError(err, "Could not compare db data")
	defer conn.disconnect()

	dbsteward.Info("Comparing composited dbsteward definition data rows to postgresql database connection table contents")
	// compare the composited dbsteward document to the established database connection
	// effectively looking to see if rows are found that match primary keys, and if their contents are the same
	for _, schema := range doc.Schemas {
		for _, table := range schema.Tables {
			if table.Rows != nil {
				// TODO(go,nth) quote this
				tableName := fmt.Sprintf("%s.%s", schema.Name, table.Name)
				pkCols := table.PrimaryKey
				cols := table.Rows.Columns

				colTypes := map[string]string{}
				for _, column := range table.Columns {
					colType := column.Type

					// foreign keyed columns inherit their foreign reference type
					// TODO(go,3) this seems like something nice to have on the model
					if len(column.ForeignTable) > 0 && len(column.ForeignColumn) > 0 {
						if len(colType) > 0 {
							dbsteward.Fatal("type of %s was found for column %s but it is foreign keyed", colType, column.Name)
						}
						foreign := lib.GlobalDBX.GetTerminalForeignColumn(doc, schema, table, column)
						colType = foreign.Type
					}

					if len(colType) == 0 {
						dbsteward.Fatal("%s column %s type was not found", table.Name, column.Name)
					}

					colTypes[column.Name] = colType
				}

				q := ops.GetQuoter()
				for _, row := range table.Rows.Rows {
					// TODO(go,nth) can we fix this direct sql construction with a ToSql struct?
					pkExprs := []string{}
					for _, pkCol := range pkCols {
						// TODO(go,nth) can we put this column lookup in the model? `row.GetValueForColumn(pkCol)`
						pkIndex := util.IndexOf(cols, pkCol)
						if pkIndex < 0 {
							dbsteward.Fatal("failed to find %s.%s primary key column %s in cols list %v",
								schema.Name, table.Name, pkCol, cols)
						}

						expr := fmt.Sprintf(
							"%s = %s",
							q.QuoteColumn(pkCol),
							sql.NewValue(colTypes[pkCol], row.Columns[pkIndex].Text, row.Columns[pkIndex].Null).GetValueSql(q),
						)
						pkExprs = append(pkExprs, expr)
					}
					pkExpr := strings.Join(pkExprs, " AND ")

					// TODO(go,nth) use parameterized queries
					sql := fmt.Sprintf(`SELECT * FROM %s WHERE %s`, tableName, pkExpr)
					rows, err := conn.queryMap(sql)
					dbsteward.FatalIfError(err, "Error with data query")

					if row.Delete {
						if len(rows) > 0 {
							dbsteward.Notice("%s row marked for DELETE found WHERE %s", tableName, pkExpr)
						}
					} else if len(rows) == 0 {
						dbsteward.Notice("%s does not contain row WHERE %s", tableName, pkExpr)
					} else if len(rows) > 1 {
						dbsteward.Notice("%s contains more than one row WHERE %s", tableName, pkExpr)
						for _, dbRow := range rows {
							dbsteward.Notice("\t%v", dbRow)
						}
					} else {
						dbRow := rows[0]
						for i, col := range cols {
							// TODO(feat) what about row.Columns[i].Null?
							valuesMatch, xmlValue, dbValue := compareDbDataRow(conn, colTypes[col], row.Columns[i].Text, dbRow[col])
							if !valuesMatch {
								dbsteward.Warning("%s row column WHERE (%s) %s data does not match database row column: '%s' vs '%s'",
									tableName, pkExpr, col, xmlValue, dbValue)
							}
						}
					}
				}
			}
		}
	}
	return doc
}
func compareDbDataRow(conn *liveConnection, colType, xmlValue, dbValue string) (bool, string, string) {
	colType = strings.ToLower(colType)
	xmlValue = pgdataHomogenize(colType, xmlValue)
	dbValue = pgdataHomogenize(colType, dbValue)
	if xmlValue == dbValue {
		return true, xmlValue, dbValue
	}

	// if they are not equal, and are alternately expressable, ask the database
	if strings.HasPrefix(colType, "time") || strings.HasPrefix(colType, "date") || strings.HasPrefix(colType, "interval") {
		if len(xmlValue) > 0 && len(dbValue) > 0 {
			sql := fmt.Sprintf(`SELECT $1::%s = $2::%[1]s`, colType)
			var eq bool
			err := conn.queryVal(&eq, sql, xmlValue, dbValue)
			lib.GlobalDBSteward.FatalIfError(err, "Could not query database")
			return eq, xmlValue, dbValue
		}
	}

	return false, xmlValue, dbValue
}
func pgdataHomogenize(colType string, value string) string {
	switch {
	case strings.HasPrefix(colType, "bool"):
		switch strings.ToLower(value) {
		case "true", "t":
			return "true"
		default:
			return "false"
		}

	default:
		return value
	}
}

func (ops *Operations) SqlDiff(old, new []string, upgradePrefix string) {
	lib.GlobalDBSteward.Notice("Calculating sql differences:")
	lib.GlobalDBSteward.Notice("Old set: %v", old)
	lib.GlobalDBSteward.Notice("New set: %v", new)
	lib.GlobalDBSteward.Notice("Upgrade: %s", upgradePrefix)
	differ.DiffSql(old, new, upgradePrefix)
}

func buildSchema(doc *ir.Definition, ofs output.OutputFileSegmenter, tableDep []*ir.TableRef) error {
	// TODO(go,3) roll this into diffing nil -> doc
	// schema creation
	for _, schema := range doc.Schemas {
		ofs.WriteSql(GlobalSchema.GetCreationSql(schema)...)

		// schema grants
		for _, grant := range schema.Grants {
			ofs.WriteSql(GlobalSchema.GetGrantSql(doc, schema, grant)...)
		}
	}

	// types: enumerated list, etc
	for _, schema := range doc.Schemas {
		for _, datatype := range schema.Types {
			sql, err := getCreateTypeSql(schema, datatype)
			lib.GlobalDBSteward.FatalIfError(err, "Could not get data type creation sql for build")
			ofs.WriteSql(sql...)
		}
	}

	// table structure creation
	for _, schema := range doc.Schemas {
		// create defined tables
		includeColumnDefaultNextvalInCreateSql = false
		for _, table := range schema.Tables {
			// table definition
			ofs.WriteSql(getCreateTableSql(schema, table)...)

			// table indexes
			diffIndexesTable(ofs, nil, nil, schema, table)

			// table grants
			for _, grant := range table.Grants {
				ofs.WriteSql(getTableGrantSql(schema, table, grant)...)
			}
		}
		includeColumnDefaultNextvalInCreateSql = true

		// sequences contained in the schema
		for _, sequence := range schema.Sequences {
			if sequence.OwnedByColumn == "" {
				sql, err := getCreateSequenceSql(schema, sequence)
				if err != nil {
					return err
				}
				ofs.WriteSql(sql...)
			} else {
				// If sequence already created as part of a serial, generate
				// an ALTER against a default sequence
				ofs.WriteSql(getAlterSequenceSql(schema.Name, &ir.Sequence{}, sequence))
			}

			// sequence permission grants
			for _, grant := range sequence.Grants {
				ofs.WriteSql(getSequenceGrantSql(schema, sequence, grant)...)
			}
		}

		// add table nextvals that were omitted
		for _, table := range schema.Tables {
			if table.HasDefaultNextVal() {
				ofs.WriteSql(getDefaultNextvalSql(schema, table)...)
			}
		}
	}

	// function definitions
	for _, schema := range doc.Schemas {
		for _, function := range schema.Functions {
			if function.HasDefinition(ir.SqlFormatPgsql8) {
				ofs.WriteSql(getFunctionCreationSql(schema, function)...)
				// when pg:build_schema() is doing its thing for straight builds, include function permissions
				// they are not included in pg_function::get_creation_sql()

				for _, grant := range function.Grants {
					ofs.WriteSql(getFunctionGrantSql(schema, function, grant)...)
				}
			}
		}
	}

	// maybe move this but here we're defining column defaults fo realz
	for _, schema := range doc.Schemas {
		for _, table := range schema.Tables {
			// TODO(go,nth) method name consistency - should be GetColumnDefaultsSql?
			ofs.WriteSql(defineTableColumnDefaults(schema, table)...)
		}
	}

	// define table primary keys before foreign keys so unique requirements are always met for FOREIGN KEY constraints
	for _, schema := range doc.Schemas {
		for _, table := range schema.Tables {
			createConstraintsTable(ofs, nil, nil, schema, table, sql99.ConstraintTypePrimaryKey)
		}
	}

	// foreign key references
	// use the dependency order to specify foreign keys in an order that will satisfy nested foreign keys and etc
	// TODO(feat) shouldn't this consider GlobalDBSteward.LimitToTables like BuildData does?
	for _, entry := range tableDep {
		createConstraintsTable(ofs, nil, nil, entry.Schema, entry.Table, sql99.ConstraintTypeConstraint)
	}

	// trigger definitions
	for _, schema := range doc.Schemas {
		for _, trigger := range schema.Triggers {
			if trigger.SqlFormat.Equals(ir.SqlFormatPgsql8) {
				ofs.WriteSql(getCreateTriggerSql(schema, trigger)...)
			}
		}
	}

	createViewsOrdered(ofs, nil, doc)

	// view permission grants
	for _, schema := range doc.Schemas {
		for _, view := range schema.Views {
			for _, grant := range view.Grants {
				ofs.WriteSql(getViewGrantSql(doc, schema, view, grant)...)
			}
		}
	}

	differ.UpdateDatabaseConfigParameters(ofs, nil, doc)
	return nil
}

func buildData(doc *ir.Definition, ofs output.OutputFileSegmenter, tableDep []*ir.TableRef) {
	limitToTables := lib.GlobalDBSteward.LimitToTables

	// use the dependency order to then write out the actual data inserts into the data sql file
	for _, entry := range tableDep {
		schema := entry.Schema
		table := entry.Table

		// skip any tables that are not in the limit list, if there are any tables to limit
		if len(limitToTables) > 0 {
			if includeTables, ok := limitToTables[schema.Name]; ok {
				if !util.Contains(includeTables, table.Name) {
					continue
				}
			} else {
				// if this entry's schema didn't appear in the include list, we can't possibly include any tables from it
				continue
			}
		}

		ofs.WriteSql(getCreateDataSql(nil, nil, schema, table)...)

		// set serial primary keys to the max value after inserts have been performed
		// only if the PRIMARY KEY is not a multi column
		if table.Rows != nil && len(table.PrimaryKey) == 1 {
			dataCols := table.Rows.Columns
			pkCol := table.PrimaryKey[0]
			if util.Contains(dataCols, pkCol) {
				// TODO(go,3) seems like this could be refactored better by putting much of the lookup
				// into the model structs
				pk := lib.GlobalDBX.TryInheritanceGetColumn(doc, schema, table, pkCol)
				if pk == nil {
					lib.GlobalDBSteward.Fatal("Failed to find primary key column '%s' for %s.%s",
						pkCol, schema.Name, table.Name)
				}
				// TODO(go,nth) unify DataType.IsLinkedType and Column.IsSerialType
				if isColumnSerialType(pk) && pk.SerialStart == nil {
					ofs.WriteSql(&sql.SequenceSerialSetValMax{
						Column: sql.ColumnRef{
							Schema: schema.Name,
							Table:  table.Name,
							Column: pk.Name,
						},
					})
				}
			}
		}

		// check if primary key columns are columns of this table
		// TODO(go,3) does this check belong here? should there be some kind of post-parse validation?
		for _, columnName := range table.PrimaryKey {
			col := lib.GlobalDBX.TryInheritanceGetColumn(doc, schema, table, columnName)
			if col == nil {
				lib.GlobalDBSteward.Fatal("Declared primary key column (%s) does not exist as column in table %s.%s",
					columnName, schema.Name, table.Name)
			}
		}
	}

	// include all of the unstaged sql elements
	lib.GlobalDBX.BuildStagedSql(doc, ofs, "")
}

func columnValueDefault(schema *ir.Schema, table *ir.Table, columnName string, dataCol *ir.DataCol) sql.ToSqlValue {
	// if the column represents NULL, return a NULL value
	if dataCol.Null {
		return sql.ValueNull
	}
	// if the column represents an empty string, return an empty string
	if dataCol.Empty {
		return sql.StringValue("")
	}
	// if the column represents a sql expression, return an expression or DEFAULT
	if dataCol.Sql {
		if strings.EqualFold(strings.TrimSpace(dataCol.Text), "default") {
			return sql.ValueDefault
		} else {
			return sql.ExpressionValue(dataCol.Text)
		}
	}

	col := lib.GlobalDBX.TryInheritanceGetColumn(lib.GlobalDBSteward.NewDatabase, schema, table, columnName)
	if col == nil {
		lib.GlobalDBSteward.Fatal("Failed to find table %s.%s column %s for default value check", schema.Name, table.Name, columnName)
	}

	// if col is zero length, make it default or db null
	if dataCol.Text == "" {
		// note: inlined and simplified from xml_parser::column_default_value
		if col.Default == "" || strings.EqualFold(strings.TrimSpace(col.Default), "null") {
			return sql.ValueNull
		}
		// TODO(go,pgsql) xml_parser::column_default_value strips quoting, but I'm not sure why, that doesn't seem right
		// if we have <column ... default="'foo'"/> then this would result in INSERT ... VALUES (..., foo, ...) instead of 'foo'
		// we need to test this very thoroughly to establish intended behavior
		// until then, we'll treat the default as literal sql, as in other locations in the code
		// return ops.StripStringQuoting(col.Default)
		return sql.RawSql(col.Default)
	}

	return &sql.TypedValue{
		Type:  getColumnType(lib.GlobalDBSteward.NewDatabase, schema, table, col),
		Value: dataCol.Text,
	}
}

// TODO(go,nth) should this live somewhere else?
// TODO(go,pgsql8) test this
func parseSqlArray(str string) []string {
	var out []string
	str = strings.Trim(str, "{}")
	if str == "" {
		return out
	}

	// can't just split this, actually need to parse to account for quotes
	// TODO(go,nth) can we just use stdlib csv parsing?
	next := ""
	inQuote := false
	inEscape := false
	for _, c := range str {
		if inEscape {
			inEscape = false
			continue
		}

		if inQuote {
			if c == '"' {
				inQuote = false
				continue
			}

			if c == '\\' {
				inEscape = true
				continue
			}
		}

		if c == ',' {
			out = append(out, next)
			next = ""
			continue
		}

		if c == '"' {
			inQuote = true
			continue
		}

		next += string(c)
	}
	return append(out, next)
}

func buildSequenceName(schema, table, column string) string {
	return buildIdentifierName(schema, table, column, "_seq")
}

// buildIdentifierName(schema, table, column, suffix)
// TODO: schema is unusued. Why? Remove it not really needed.
func buildIdentifierName(_, table, column, suffix string) string {
	// these will change as we build the identifier
	identTable := table
	identColumn := column

	maxLen := MAX_IDENT_LENGTH - 1 - len(suffix)
	tableMaxLen := util.IntCeil(maxLen, 2)
	colMaxLen := maxLen - tableMaxLen

	if len(identTable) > tableMaxLen && len(identColumn) < colMaxLen {
		// table is longer than max and column is shorter than max
		// give column excess to table max
		tableMaxLen += colMaxLen - len(identColumn)
	} else if len(identTable) < tableMaxLen && len(identColumn) > colMaxLen {
		// table is shorter, column is longer
		// give table excess to column max
		colMaxLen += tableMaxLen - len(identTable)
	}

	if len(identTable) > tableMaxLen {
		identTable = identTable[0:tableMaxLen]
	}
	if len(identColumn) > colMaxLen {
		identColumn = identColumn[0:colMaxLen]
	}

	// TODO(go,pgsql8) track_pg_identifiers

	return fmt.Sprintf("%s_%s%s", identTable, identColumn, suffix)
}

// https://www.postgresql.org/docs/current/ddl-priv.html
var aclMapping = map[rune]string{
	'a': "UPDATE", // "append" or update on table, column
	'r': "SELECT",
	'w': "UPDATE", // "update" on object, sequence, table, column
	'd': "DELETE",
	'D': "TRUNCATE",
	'x': "REFERENCES",
	't': "TRIGGER",
	'C': "CREATE",  // DB, schema, tablespace
	'c': "CONNECT", // to database
	'T': "TEMPORARY",
	'X': "EXECUTE",
	'U': "USAGE",
	's': "SET",          // config parameter
	'A': "ALTER SYSTEM", // config parameter
}

// parseACL unmarshalls an ACL string into a map of users each
// with an array of permissions
// will be receiving something like:
// pg_database_owner=UC/pg_database_owner
// =U/pg_database_owner
// additional_role=U/pg_database_owner
// output {superuser: [select, usage, ...], ...}
func parseACL(str string) map[string][]string {
	out := map[string][]string{}

	for _, elem := range strings.Split(str, "\n") {
		userperms := strings.SplitN(elem, "=", 2)
		if len(userperms) == 1 {
			// no perms
			continue
		}
		user := userperms[0]
		if user == "" {
			user = "public"
		}
		perms := userperms[1]

		for _, c := range strings.SplitN(perms, "/", 2)[0] {
			perm, ok := aclMapping[c]
			if !ok {
				log.Panicf("unrecognized permission '%s'", string(c))
			}
			out[user] = append(out[user], perm)
		}
	}

	return out
}

// parseACL unmarshalls an array of ACL strings into a map of users each
// with an array of permissions
// will be receiving something like '{superuser=rwU/superuser_role,normal_role=rw/superuser_role}'
// output {superuser: [select, usage, ...], ...}
func parseSequenceRelAcl(str string) map[string][]string {
	out := map[string][]string{}

	for _, elem := range parseSqlArray(str) {
		userperms := strings.SplitN(elem, "=", 2)
		if len(userperms) == 1 {
			// no perms
			continue
		}
		user := userperms[0]
		if user == "" {
			user = "public"
		}
		perms := userperms[1]

		for _, c := range strings.SplitN(perms, "/", 2)[0] {
			perm, ok := aclMapping[c]
			if !ok {
				log.Panicf("unrecognized permission '%s'", string(c))
			}
			out[user] = append(out[user], perm)
		}
	}

	return out
}
