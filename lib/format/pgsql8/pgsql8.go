package pgsql8

import (
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/format/sql99"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/model"
)

var GlobalOperations *Operations = NewOperations()

type Operations struct {
	*sql99.Operations

	EscapeStringValues bool

	contextReplicaSetId int
}

func NewOperations() *Operations {
	pgsql := &Operations{
		Operations:         sql99.NewOperations(),
		EscapeStringValues: false,
	}
	pgsql.Operations.Operations = pgsql
	return pgsql
}

func (self *Operations) Build(outputPrefix string, dbDoc *model.Definition) {
	// TODO(go,4) can we just consider a build(def) to be diff(null, def)?
	// some shortcuts, since we're going to be typing a lot here
	dbsteward := lib.GlobalDBSteward
	xmlParser := lib.GlobalXmlParser
	sqlParser := lib.GlobalSqlParser
	dbx := lib.GlobalDBX

	buildFileName := outputPrefix + "_build.sql"
	dbsteward.Info("Building complete file %s", buildFileName)

	buildFile, err := os.Create(buildFileName)
	dbsteward.FatalIfError(err, "Failed to open file %s for output", buildFileName)

	buildFileOfs := output.NewOutputFileSegmenterToFile(dbsteward, self, buildFileName, 1, buildFile, buildFileName, dbsteward.OutputFileStatementLimit)
	if len(dbsteward.LimitToTables) == 0 {
		buildFileOfs.Write("-- full database definition file generated %s\n", time.Now().Format(time.RFC1123Z))
	}
	if !dbsteward.GenerateSlonik {
		buildFileOfs.Write("BEGIN;\n\n")
	}

	dbsteward.Info("Calculating table foreign dependency order...")
	tableDependency := xmlParser.TableDependencyOrder(dbDoc)

	// database-specific implementation code refers to dbsteward::$new_database when looking up roles/values/conflicts etc
	dbsteward.NewDatabase = dbDoc
	dbx.SetDefaultSchema(dbDoc, "public")

	// language definitions
	if dbsteward.CreateLanguages {
		for _, language := range dbDoc.Languages {
			buildFileOfs.WriteSql(GlobalLanguage.GetCreationSql(language)...)
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
			if definition, ok := function.TryGetDefinition(); ok {
				if strings.EqualFold(definition.Language, "sql") && definition.SqlFormat == model.SqlFormatPgsql8 {
					referencedTableName := self.functionDefinitionReferencesTable(definition)
					if len(referencedTableName) > 0 {
						referencedSchemaName := sqlParser.GetSchemaName(referencedTableName, dbDoc)
						// TODO(go,pgsql8) handle error cases
						referencedSchema, _ := dbDoc.GetSchemaNamed(referencedSchemaName)
						referencedTable, err := referencedSchema.GetTableNamed(sqlParser.GetObjectName(referencedTableName, dbDoc))
						if err == nil {
							setCheckFunctionBodies = false
							setCheckFunctionBodiesInfo = fmt.Sprintf(
								"Detected LANGUAGE SQL function %s.%s referring to table %s.%s in the database definition",
								schema.Name, function.Name, referencedSchemaName, referencedTable.Name,
							)
							break outer
						}
					}
				}
			}
		}
	}
	if !setCheckFunctionBodies {
		buildFileOfs.Write("\n")
		buildFileOfs.WriteSql(&sql.SetCheckFunctionBodies{setCheckFunctionBodiesInfo})
		dbsteward.Info(setCheckFunctionBodiesInfo)
	}

	if dbsteward.OnlySchemaSql || !dbsteward.OnlyDataSql {
		dbsteward.Info("Defining structure")
		self.BuildSchema(dbDoc, buildFileOfs, tableDependency)
	}
	if !dbsteward.OnlySchemaSql || dbsteward.OnlyDataSql {
		dbsteward.Info("Defining data inserts")
		self.BuildData(dbDoc, buildFileOfs, tableDependency)
	}
	dbsteward.NewDatabase = nil

	if !dbsteward.GenerateSlonik {
		buildFileOfs.Write("COMMIT;\n\n")
	}

	if dbsteward.GenerateSlonik {
		// TODO(go,slony)
	}
}
func (self *Operations) BuildUpgrade(
	oldOutputPrefix string, oldCompositeFile string, oldDoc *model.Definition, oldFiles []string,
	newOutputPrefix string, newCompositeFile string, newDoc *model.Definition, newFiles []string,
) {
	upgradePrefix := newOutputPrefix + "_upgrade"

	lib.GlobalDBSteward.Info("Calculating old table foreign key dependency order...")
	GlobalDiff.OldTableDependency = lib.GlobalXmlParser.TableDependencyOrder(oldDoc)

	lib.GlobalDBSteward.Info("Calculating new table foreign key dependency order...")
	GlobalDiff.NewTableDependency = lib.GlobalXmlParser.TableDependencyOrder(newDoc)

	GlobalDiff.DiffDoc(oldCompositeFile, newCompositeFile, oldDoc, newDoc, upgradePrefix)

	if lib.GlobalDBSteward.GenerateSlonik {
		// TODO(go,slony)
	}
}
func (self *Operations) ExtractSchema(host string, port uint, name, user, pass string) *model.Definition {
	// TODO(go,nth) extract this massive function into separate functions, different structs, etc
	dbsteward := lib.GlobalDBSteward

	dbsteward.Notice("Connecting to pgsql8 host %s:%d database %s as %s", host, port, name, user)
	GlobalDb.Connect(host, port, name, user, pass)

	doc := &model.Definition{
		Database: &model.Database{
			SqlFormat: model.SqlFormatPgsql8,
			Roles: &model.RoleAssignment{
				Application: user,
				Owner:       user,
				Replication: user,
				ReadOnly:    user,
			},
		},
	}

	// find all tables in the schema that aren't in the built-in schemas
	// TODO(go,nth) move this into a dedicated function returning structs
	res := GlobalDb.Query(`
		SELECT
			t.schemaname, t.tablename, t.tableowner, t.tablespace,
			sd.description as schema_description, td.description as table_description,
			( SELECT array_agg(cd.objsubid::text || ';' ||cd.description)
				FROM pg_catalog.pg_description cd
				WHERE cd.objoid = c.oid AND cd.classoid = c.tableoid AND cd.objsubid > 0 ) AS column_descriptions
		FROM pg_catalog.pg_tables t
		LEFT JOIN pg_catalog.pg_namespace n ON (n.nspname = t.schemaname)
		LEFT JOIN pg_catalog.pg_class c ON (c.relname = t.tablename AND c.relnamespace = n.oid)
		LEFT JOIN pg_catalog.pg_description td ON (td.objoid = c.oid AND td.classoid = c.tableoid AND td.objsubid = 0)
		LEFT JOIN pg_catalog.pg_description sd ON (sd.objoid = n.oid)
		WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
		ORDER BY schemaname, tablename;
	`)
	// serials that are implicitly created as part of a table, no need to explicitly create these
	sequenceCols := []string{}
	tableSerials := []string{}
	for res.Next() {
		row := res.FetchRowStringMap()
		schemaName := row["schemaname"]
		tableName := row["tablename"]

		dbsteward.Info("Analyze table options %s.%s", row["schemaname"], row["tablename"])
		// schemaname | tablename | tableowner | tablespace | hasindexes | hasrules | hastriggers
		// create the schema if it is missing
		schema := doc.TryGetSchemaNamed(schemaName)
		if schema == nil {
			schema = &model.Schema{
				Name:        schemaName,
				Description: row["schema_description"],
			}
			doc.AddSchema(schema)

			// TODO(feat) can we just add this to the main query?
			var owner string
			err := GlobalDb.QueryVal(&owner, `SELECT schema_owner FROM information_schema.schemata WHERE schema_name = $1`, schemaName)
			dbsteward.FatalIfError(err, "Could not query database")
			schema.Owner = self.translateRoleName(owner)
		}

		// create the table in the schema space
		table := schema.TryGetTableNamed(tableName)
		if table != nil {
			// if this happens, it means our query or logic is wrong
			panic(errors.Errorf("table %s.%s already defined in xml object - unexpected", schema.Name, table.Name))
		}
		table = &model.Table{
			Name:        tableName,
			Owner:       self.translateRoleName(row["tableowner"]),
			Description: row["table_description"],
		}
		schema.AddTable(table)

		// extract tablespace as a tableOption
		if len(row["tablespace"]) > 0 {
			table.SetTableOption(model.SqlFormatPgsql8, "tablespace", row["tablespace"])
		}

		// extract storage parameters as a tableOption
		// TODO(feat) can we just add this to the main query?
		paramsRow, err := GlobalDb.QueryStringMap(`
			SELECT reloptions, relhasoids
			FROM pg_catalog.pg_class
			WHERE relname = $1
				AND relnamespace = (
					SELECT oid
					FROM pg_catalog.pg_namespace
					WHERE nspname = $2
				)
		`, table.Name, schema.Name)
		dbsteward.FatalIfError(err, "Could not query database")

		// TODO(go,pgsql) this logic is a little different than legacy, need to double check everything works as expected here
		// legacy shoved everything in a single table option with name = "with", this adds separate table options

		// reloptions is formatted as {name=value,name=value}
		reloptions := paramsRow["reloptions"]
		params := strings.Split(reloptions[1:len(reloptions)-1], ",")
		for _, param := range params {
			nameval := strings.Split(param, "=")
			table.SetTableOption(model.SqlFormatPgsql8, nameval[0], nameval[1])
		}

		// TODO(feat) pg 11.0 dropped support for "with oids" or "oids=true"
		table.SetTableOption(model.SqlFormatPgsql8, "oids", paramsRow["relhasoids"])

		dbsteward.Info("Analyze table columns %s.%s", schema.Name, table.Name)
		columnDescriptions := map[string]string{}
		for _, desc := range self.parseSqlArray(row["column_descriptions"]) {
			indexDesc := strings.Split(desc, ";") // see the array_agg in the main query
			columnDescriptions[indexDesc[0]] = indexDesc[1]
		}

		// hasindexes | hasrules | hastriggers handled later
		// get columns for the table
		colRes := GlobalDb.Query(`
			SELECT
				column_name, data_type,
				column_default, is_nullable,
				ordinal_position, numeric_precision,
				format_type(atttypid, atttypmod) as attribute_data_type
			FROM information_schema.columns
				JOIN pg_class pgc ON (pgc.relname = table_name AND pgc.relkind='r')
				JOIN pg_namespace nsp ON (nsp.nspname = table_schema AND nsp.oid = pgc.relnamespace)
				JOIN pg_attribute pga ON (pga.attrelid = pgc.oid AND columns.column_name = pga.attname)
			WHERE table_schema=$1 AND table_name=$2
				AND attnum > 0
				AND NOT attisdropped
		`, schema.Name, table.Name)
		for colRes.Next() {
			colRow := colRes.FetchRowStringMap()
			column := &model.Column{
				Name:        colRow["column_name"],
				Description: columnDescriptions[colRow["ordinal_position"]],
				Type:        colRow["attribute_data_type"],
				// TODO(go,nth) legacy logic only ever sets nullable to false (pgsql8.php:1638) but that really doesn't seem correct to me. validate this
				Nullable: !util.IsFalsey(colRow["is_nullable"]),
				// TODO(go,nth) how does this handle expression defaults?
				Default: colRow["column_default"],
			}
			table.AddColumn(column)

			// look for serial columns that are primary keys and collapse them down from integers with sequence defualts into serials
			// type int or bigint
			// is_nullable = NO
			// column_default starts with nextval and contains iq_seq
			// TODO(feat) this list of conditions is probably not sufficient to check for serials in all cases
			// TODO(go,nth) is there a better way to test this?
			if (strings.EqualFold(column.Type, "integer") || strings.EqualFold(column.Type, "bigint")) &&
				!column.Nullable &&
				(util.IIndex(column.Default, "nextval") == 0 && util.IIndex(column.Default, "_seq") >= 0) {
				column.Type = "serial"
				if strings.EqualFold("column_default", "bigint") {
					column.Type = "bigserial"
				}

				// store sequences that will be implicitly genreated during table create
				// could use pgsql8::identifier_name and fully qualify the table but it will just truncate "for us" anyhow, so manually prepend schema
				identName := schema.Name + "." + self.IdentifierName(schema.Name, table.Name, column.Name, "_seq")
				tableSerials = append(tableSerials, identName)

				// TODO(go,nth) explain this logic, see pgsql8.php:1631, :1691
				seqName := strings.Split(column.Default, "'")
				sequenceCols = append(sequenceCols, fmt.Sprintf("'%s'", seqName[1]))

				// TODO(feat) legacy logic doesn't set default or nullable for serial types... is that correct?
				column.Nullable = false
				column.Default = ""
			}
		}
		dbsteward.FatalIfError(colRes.Err(), "Error while querying database")

		dbsteward.Info("Analyze table indexes %s.%s", schema.Name, table.Name)
		indexRes := GlobalDb.Query(`
			SELECT
				ic.relname, i.indisunique,
				(
					-- get the n'th dimension's definition
					SELECT array_agg(pg_catalog.pg_get_indexdef(i.indexrelid, n, true))
					FROM generate_series(1, i.indnatts) AS n
				) AS dimensions
			FROM pg_index i
				LEFT JOIN pg_class ic ON ic.oid = i.indexrelid
				LEFT JOIN pg_class tc ON tc.oid = i.indrelid
				LEFT JOIN pg_catalog.pg_namespace n ON n.oid = tc.relnamespace
			WHERE tc.relname = $2
				AND n.nspname = $1
				AND i.indisprimary != 't'
				AND ic.relname NOT IN (
					SELECT constraint_name
					FROM information_schema.table_constraints
					WHERE table_schema = $1
						AND table_name = $2);
		`, schema.Name, table.Name)
		for indexRes.Next() {
			indexRow := indexRes.FetchRowStringMap()
			// only add a unique index if the column was unique
			index := &model.Index{
				Name:   indexRow["relname"],
				Using:  "btree", // TODO(go,pgsql) this is definitely incorrect, need to fix before release
				Unique: util.IsTruthy(indexRow["indisunique"]),
			}
			table.AddIndex(index)

			for _, dim := range self.parseSqlArray(indexRow["dimensions"]) {
				index.AddDimension(dim)
			}
		}
		dbsteward.FatalIfError(indexRes.Err(), "Error while querying database")
	}
	dbsteward.FatalIfError(res.Err(), "Error while querying database")

	for _, schema := range doc.Schemas {
		dbsteward.Info("Analyze isolated sequences in schema %s", schema.Name)

		// filter by sequences we've defined as part of a table already and get the owner of each sequence
		sql := `
			SELECT s.relname, r.rolname
			FROM pg_statio_all_sequences s
			JOIN pg_class c ON (s.relname = c.relname)
			JOIN pg_roles r ON (c.relowner = r.oid)
			WHERE schemaname = $1
		`
		params := []interface{}{schema.Name}
		if len(sequenceCols) > 0 {
			sql += `AND s.relname NOT IN $2`
			params = append(params, sequenceCols)
		}
		sql += `GROUP BY s.relname, r.rolname`

		seqListRes := GlobalDb.Query(sql, params...)
		for seqListRes.Next() {
			seqListRow := seqListRes.FetchRowStringMap()
			// TODO(feat) can we do away with the N+1 here?
			seqRes := GlobalDb.Query(fmt.Sprintf(`
				SELECT cache_value, start_value, min_value, max_value, increment_by, is_cycled
				FROM "%s"."%s"
			`, schema.Name, seqListRow["relname"]))
			for seqRes.Next() {
				seqRow := seqRes.FetchRowStringMap()
				seq := schema.TryGetSequenceNamed(seqListRow["relname"])
				if seq != nil {
					schema.AddSequence(&model.Sequence{
						Name:      seqListRow["relname"],
						Owner:     seqListRow["rolname"], // TODO(feat) should this have a translateRoleName call?
						Cache:     seqRow["cache_value"],
						Start:     seqRow["start_value"],
						Min:       seqRow["min_value"],
						Max:       seqRow["max_value"],
						Increment: seqRow["increment_by"],
						Cycle:     util.IsTruthy(seqRow["is_cycled"]),
					})
				}
			}
			dbsteward.FatalIfError(seqRes.Err(), "Error while querying database")
		}
		dbsteward.FatalIfError(seqListRes.Err(), "Error while querying database")
	}

	viewRes := GlobalDb.Query(`
		SELECT constraint_name, constraint_type, table_schema, table_name, array_agg(columns) AS columns
		FROM (
			SELECT tc.constraint_name, tc.constraint_type, tc.table_schema, tc.table_name, kcu.column_name::text AS columns
			FROM information_schema.table_constraints tc
			LEFT JOIN information_schema.key_column_usage kcu ON tc.constraint_catalog = kcu.constraint_catalog AND tc.constraint_schema = kcu.constraint_schema AND tc.constraint_name = kcu.constraint_name
			WHERE tc.table_schema NOT IN ('information_schema', 'pg_catalog')
				AND tc.constraint_type != 'FOREIGN KEY'
			GROUP BY tc.constraint_name, tc.constraint_type, tc.table_schema, tc.table_name, kcu.column_name
			ORDER BY kcu.column_name, tc.table_schema, tc.table_name
		) AS results
		GROUP BY results.constraint_name, results.constraint_type, results.table_schema, results.table_name;
	`)
	for viewRes.Next() {
		viewRow := viewRes.FetchRowStringMap()
		dbsteward.Info("Analyze view %s.%s", viewRow["schemaname"], viewRow["viewname"])

		schema := doc.TryGetSchemaNamed(viewRow["schemaname"])
		if schema == nil {
			schema = &model.Schema{
				Name: viewRow["schemaname"],
			}
			doc.AddSchema(schema)

			// TODO(feat) can we just add this to the main query?
			var owner string
			err := GlobalDb.QueryVal(&owner, `SELECT schema_owner FROM information_schema.schemata WHERE schema_name = $1`, schema.Name)
			dbsteward.FatalIfError(err, "Could not query database")
			schema.Owner = self.translateRoleName(owner)
		}

		view := schema.TryGetViewNamed(viewRow["viewname"])
		if view != nil {
			panic(errors.Errorf("view %s.%s already defined in XML object -- unexpected", schema.Name, viewRow["viewname"]))
		}

		schema.AddView(&model.View{
			Name:  viewRow["viewname"],
			Owner: self.translateRoleName(viewRow["viewowner"]),
			Queries: []*model.ViewQuery{
				&model.ViewQuery{
					SqlFormat: model.SqlFormatPgsql8,
					Text:      viewRow["definition"],
				},
			},
		})
	}
	dbsteward.FatalIfError(viewRes.Err(), "Error while querying database")

	// for all schemas, all tables - get table constraints that are not type 'FOREIGN KEY'
	constraintRes := GlobalDb.Query(`
		SELECT constraint_name, constraint_type, table_schema, table_name, array_agg(columns) AS columns
    FROM (
      SELECT tc.constraint_name, tc.constraint_type, tc.table_schema, tc.table_name, kcu.column_name::text AS columns
      FROM information_schema.table_constraints tc
				LEFT JOIN information_schema.key_column_usage kcu ON
					tc.constraint_catalog = kcu.constraint_catalog
					AND tc.constraint_schema = kcu.constraint_schema
					AND tc.constraint_name = kcu.constraint_name
      WHERE tc.table_schema NOT IN ('information_schema', 'pg_catalog')
        AND tc.constraint_type != 'FOREIGN KEY'
      GROUP BY tc.constraint_name, tc.constraint_type, tc.table_schema, tc.table_name, kcu.column_name
			ORDER BY kcu.column_name, tc.table_schema, tc.table_name
		) AS results
		GROUP BY results.constraint_name, results.constraint_type, results.table_schema, results.table_name
	`)
	for constraintRes.Next() {
		constraintRow := constraintRes.FetchRowStringMap()
		dbsteward.Info("Analyze table constraints %s.%s", constraintRow["table_schema"], constraintRow["table_name"])

		schema := doc.TryGetSchemaNamed(constraintRow["table_schema"])
		if schema == nil {
			panic(errors.Errorf("failed to find schema %s for constraint in table %s", constraintRow["table_schema"], constraintRow["table_name"]))
		}

		table := schema.TryGetTableNamed(constraintRow["table_name"])
		if table == nil {
			panic(errors.Errorf("failed to find table %s.%s for constraint", constraintRow["table_schema"], constraintRow["table_name"]))
		}

		columns := self.parseSqlArray(constraintRow["columns"])

		switch strings.ToLower(constraintRow["constraint_type"]) {
		case "primary key":
			table.PrimaryKey = columns
			table.PrimaryKeyName = constraintRow["constraint_name"]
		case "unique":
			table.AddConstraint(&model.Constraint{
				Name:       constraintRow["constraint_name"],
				Type:       "unique",
				Definition: fmt.Sprintf(`("%s")`, strings.Join(columns, `", "`)),
			})
		case "check":
			// TODO(feat) implement CHECK constraints
		default:
			dbsteward.Fatal("Unknown constraint_type %s", constraintRow["constraint_type"])
		}
	}
	dbsteward.FatalIfError(constraintRes.Err(), "Error while querying database")

	// We cannot accurately retrieve FOREIGN KEYs via information_schema
	// We must rely on getting them from pg_catalog instead
	// See http://stackoverflow.com/questions/1152260/postgres-sql-to-list-table-foreign-keys
	fkRules := map[string]string{
		"a": "NO_ACTION",
		"r": "RESTRICT",
		"c": "CASCADE",
		"n": "SET_NULL",
		"d": "SET_DEFAULT",
	}
	fkRes := GlobalDb.Query(`
		SELECT
			con.constraint_name, con.update_rule, con.delete_rule,
			lns.nspname AS local_schema, lt_cl.relname AS local_table, array_to_string(array_agg(lc_att.attname), ' ') AS local_columns,
			fns.nspname AS foreign_schema, ft_cl.relname AS foreign_table, array_to_string(array_agg(fc_att.attname), ' ') AS foreign_columns
		FROM (
			-- get column mappings
			SELECT
				local_constraint.conrelid AS local_table, unnest(local_constraint.conkey) AS local_col,
				local_constraint.confrelid AS foreign_table, unnest(local_constraint.confkey) AS foreign_col,
				local_constraint.conname AS constraint_name, local_constraint.confupdtype AS update_rule, local_constraint.confdeltype as delete_rule
			FROM pg_class cl
				INNER JOIN pg_namespace ns ON cl.relnamespace = ns.oid
				INNER JOIN pg_constraint local_constraint ON local_constraint.conrelid = cl.oid
			WHERE ns.nspname NOT IN ('pg_catalog','information_schema')
				AND local_constraint.contype = 'f'
		) con
			INNER JOIN pg_class lt_cl ON lt_cl.oid = con.local_table
			INNER JOIN pg_namespace lns ON lns.oid = lt_cl.relnamespace
			INNER JOIN pg_attribute lc_att ON lc_att.attrelid = con.local_table AND lc_att.attnum = con.local_col
			INNER JOIN pg_class ft_cl ON ft_cl.oid = con.foreign_table
			INNER JOIN pg_namespace fns ON fns.oid = ft_cl.relnamespace
			INNER JOIN pg_attribute fc_att ON fc_att.attrelid = con.foreign_table AND fc_att.attnum = con.foreign_col
		GROUP BY con.constraint_name, lns.nspname, lt_cl.relname, fns.nspname, ft_cl.relname, con.update_rule, con.delete_rule;
	`)
	for fkRes.Next() {
		fkRow := fkRes.FetchRowStringMap()
		localCols := strings.Split(fkRow["local_columns"], " ")
		foreignCols := strings.Split(fkRow["foreign_columns"], " ")

		if len(localCols) != len(foreignCols) {
			dbsteward.Fatal(
				"Unexpected: Foreign key columns (%v) on %s.%s are mismatched with columns (%v) on %s.%s",
				localCols, fkRow["local_schema"], fkRow["local_table"],
				foreignCols, fkRow["foreign_schema"], fkRow["foreign_table"],
			)
		}

		schema := doc.TryGetSchemaNamed(fkRow["local_schema"])
		if schema == nil {
			panic(errors.Errorf("failed to find schema %s for foreign key in table %s", fkRow["local_schema"], fkRow["local_table"]))
		}

		table := schema.TryGetTableNamed(fkRow["local_table"])
		if table == nil {
			panic(errors.Errorf("failed to find table %s.%s for foreign key", fkRow["local_schema"], fkRow["local_table"]))
		}

		if len(localCols) == 1 {
			// add inline on the column
			column := table.TryGetColumnNamed(localCols[0])
			if column == nil {
				panic(errors.Errorf("failed to find column %s.%s.%s for foreign key", fkRow["local_schema"], fkRow["local_table"], localCols[0]))
			}

			column.ForeignSchema = fkRow["foreign_schema"]
			column.ForeignTable = fkRow["foreign_table"]
			column.ForeignColumn = foreignCols[0]
			column.ForeignKeyName = fkRow["constraint_name"]
			column.ForeignOnUpdate = fkRules[fkRow["update_rule"]]
			column.ForeignOnDelete = fkRules[fkRow["delete_rule"]]

			// dbsteward fk columns aren't supposed to specify a type, they get it from the referenced column
			column.Type = ""
		} else if len(localCols) > 1 {
			table.AddForeignKey(&model.ForeignKey{
				Columns:        localCols,
				ForeignSchema:  fkRow["foreign_schema"],
				ForeignTable:   fkRow["foreign_table"],
				ForeignColumns: foreignCols,
				ConstraintName: fkRow["constraint_name"],
				OnUpdate:       fkRules[fkRow["update_rule"]],
				OnDelete:       fkRules[fkRow["delete_rule"]],
			})
		}
	}
	dbsteward.FatalIfError(fkRes.Err(), "Error while querying database")

	// get function info for all functions
	// this is based on psql 8.4's \df+ query
	// that are not language c
	// that are not triggers
	fnRes := GlobalDb.Query(`
		SELECT
			p.oid, n.nspname as schema, p.proname as name,
      pg_catalog.pg_get_function_result(p.oid) as return_type,
      CASE
        WHEN p.proisagg THEN 'agg'
        WHEN p.proiswindow THEN 'window'
        WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN 'trigger'
        ELSE 'normal'
      END as type,
      CASE
        WHEN p.provolatile = 'i' THEN 'IMMUTABLE'
        WHEN p.provolatile = 's' THEN 'STABLE'
        WHEN p.provolatile = 'v' THEN 'VOLATILE'
      END as volatility,
			pg_catalog.pg_get_userbyid(p.proowner) as owner,
			l.lanname as language,
			p.prosrc as source,
			pg_catalog.obj_description(p.oid, 'pg_proc') as description
		FROM pg_catalog.pg_proc p
			LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
			LEFT JOIN pg_catalog.pg_language l ON l.oid = p.prolang
		WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
			AND l.lanname NOT IN ( 'c' )
			AND pg_catalog.pg_get_function_result(p.oid) NOT IN ( 'trigger' );
	`)
	for fnRes.Next() {
		fnRow := fnRes.FetchRowStringMap()
		dbsteward.Info("Analyze function %s.%s", fnRow["schema"], fnRow["name"])

		schema := doc.TryGetSchemaNamed(fnRow["schema"])
		if schema == nil {
			schema = &model.Schema{
				Name: fnRow["schema"],
			}
			doc.AddSchema(schema)

			// TODO(feat) can we just add this to the main query?
			var owner string
			err := GlobalDb.QueryVal(&owner, `SELECT schema_owner FROM information_schema.schemata WHERE schema_name = $1`, schema.Name)
			dbsteward.FatalIfError(err, "Could not query database")
			schema.Owner = self.translateRoleName(owner)
		}

		// TODO(feat) should we see if there's another function by this name already? that'd probably be unexpected, but would likely indicate a bug in our query
		function := &model.Function{
			Name:        fnRow["name"],
			Returns:     fnRow["return_type"],
			CachePolicy: fnRow["volatility"],
			Owner:       self.translateRoleName(fnRow["owner"]),
			Description: fnRow["description"],
			// TODO(feat): how is / figure out how to express securityDefiner attribute in the functions query
			Definitions: []*model.FunctionDefinition{
				&model.FunctionDefinition{
					SqlFormat: model.SqlFormatPgsql8,
					Language:  fnRow["language"],
					Text:      fnRow["source"],
				},
			},
		}
		schema.AddFunction(function)

		// unnest the proargtypes (which are in ordinal order) and get the correct format for them.
		// information_schema.parameters does not contain enough information to get correct type (e.g. ARRAY)
		//   Note: * proargnames can be empty (not null) if there are no parameters names
		//         * proargnames will contain empty strings for unnamed parameters if there are other named
		//                       parameters, e.g. {"", parameter_name}
		//         * proargtypes is an oidvector, enjoy the hackery to deal with NULL proargnames
		//         * proallargtypes is NULL when all arguments are IN.
		argsRes := GlobalDb.Query(`
			SELECT
				unnest(coalesce(
					proargnames,
					array_fill(''::text, ARRAY[(
						SELECT count(*)
						FROM unnest(coalesce(proallargtypes, proargtypes))
					)]::int[])
				)) as parameter_name,
				format_type(unnest(coalesce(proallargtypes, proargtypes)), NULL) AS data_type
			FROM pg_proc pr
			WHERE oid = $1
		`, fnRow["oid"])
		for argsRes.Next() {
			argsRow := argsRes.FetchRowStringMap()
			// TODO(feat) out params?
			function.AddParameter(argsRow["parameter_name"], argsRow["data_type"])
		}
		dbsteward.FatalIfError(argsRes.Err(), "Error while querying database")
	}
	dbsteward.FatalIfError(fnRes.Err(), "Error while querying database")

	// TODO(go,nth) don't use *, name columns explicitly
	triggerRes := GlobalDb.Query(`
		SELECT *
		FROM information_schema.triggers
		WHERE trigger_schema NOT IN ('pg_catalog', 'information_schema')
	`)
	for triggerRes.Next() {
		triggerRow := triggerRes.FetchRowStringMap()
		dbsteward.Info("Analyze trigger %s.%s", triggerRow["event_object_schema"], triggerRow["trigger_name"])

		schema := doc.TryGetSchemaNamed(triggerRow["event_object_schema"])
		if schema == nil {
			panic(errors.Errorf("failed to find schema %s for trigger on table %s", triggerRow["event_object_schema"], triggerRow["event_object_table"]))
		}

		table := schema.TryGetTableNamed(triggerRow["event_object_table"])
		if table == nil {
			panic(errors.Errorf("failed to find table %s.%s for trigger", triggerRow["event_object_schema"], triggerRow["event_object_table"]))
		}

		// there is a row for each event_manipulation, so we need to aggregate them, see if the trigger already exists
		// TODO(go,nth) can we simplify this by adding a groupby in the query?
		trigger := schema.TryGetTriggerNamedForTable(triggerRow["trigger_name"], triggerRow["event_object_table"])
		if trigger == nil {
			trigger = &model.Trigger{
				Name:      triggerRow["trigger_name"],
				SqlFormat: model.SqlFormatPgsql8,
			}
			schema.AddTrigger(trigger)
		}

		// TODO(feat) what should happen if we have two events with different settings??
		trigger.AddEvent(triggerRow["event_manipulation"])
		trigger.When = util.CoalesceStr(triggerRow["condition_timing"], triggerRow["action_timing"])
		trigger.Table = triggerRow["event_object_table"]
		trigger.ForEach = triggerRow["action_orientation"]
		trigger.Function = strings.TrimSpace(util.IReplaceAll(triggerRow["action_statement"], "EXECUTE PROCEDURE", ""))
	}
	dbsteward.FatalIfError(triggerRes.Err(), "Error while querying database")

	// Find table grants and save them in the xml document
	dbsteward.Info("Analyze table permissions")
	// TODO(feat) use concrete column list
	grantRes := GlobalDb.Query(`
		SELECT *
		FROM information_schema.table_privileges
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
	`)
	for grantRes.Next() {
		grantRow := grantRes.FetchRowStringMap()

		schema := doc.TryGetSchemaNamed(grantRow["table_schema"])
		if schema == nil {
			panic(errors.Errorf("failed to find schema %s for trigger on table %s", grantRow["table_schema"], grantRow["table_name"]))
		}

		relation := schema.TryGetRelationNamed(grantRow["table_name"]) // relation = table|view
		if relation == nil {
			panic(errors.Errorf("failed to find relation %s.%s for trigger", grantRow["table_schema"], grantRow["table_name"]))
		}

		// aggregate privileges by role
		// TODO(feat) what about revokes?
		grantee := self.translateRoleName(grantRow["grantee"])
		docGrants := relation.GetGrantsForRole(grantee)
		var grant *model.Grant
		if len(docGrants) == 0 {
			grant = &model.Grant{
				Role: grantee,
			}
			relation.AddGrant(grant)
		} else {
			grant = docGrants[0]
		}
		grant.AddOperation(grantRow["privilege_type"])
		// TODO(feat) what should happen if two grants for the same role have different is_grantable?
		// TODO(feat) what about other WITH flags?
		grant.SetCanGrant(util.IsTruthy(grantRow["is_grantable"]))
	}
	dbsteward.FatalIfError(grantRes.Err(), "Error while querying database")

	// analyze sequence grants and assign those to the xml document as well
	dbsteward.Info("Analyze isolated sequence permissions")
	for _, schema := range doc.Schemas {
		for _, sequence := range schema.Sequences {
			grantRes := GlobalDb.Query(`SELECT relacl FROM pg_class WHERE relname = $1`, sequence.Name)
			for grantRes.Next() {
				grantRow := grantRes.FetchRowStringMap()
				// privileges for unassociated sequences are not listed in
				// information_schema.sequences; i think this is probably the most
				// accurate way to get sequence-level grants
				if grantRow["relacl"] == "" {
					continue
				}
				grantPerms := self.parseSequenceRelAcl(grantRow["relacl"])
				for user, perms := range grantPerms {
					grantee := self.translateRoleName(user)
					for _, perm := range perms {
						// TODO(feat) what about revokes?
						grants := sequence.GetGrantsForRole(grantee)
						var grant *model.Grant
						if len(grants) == 0 {
							grant = &model.Grant{
								Role: grantee,
							}
							sequence.AddGrant(grant)
						} else {
							grant = grants[0]
						}
						grant.AddOperation(perm)
					}
				}
			}
			dbsteward.FatalIfError(grantRes.Err(), "Error while querying database")
		}
	}

	GlobalDb.Disconnect()

	// scan all now defined tables
	// TODO(feat) what about other grant checks?
	for _, schema := range doc.Schemas {
		for _, table := range schema.Tables {
			// if table does not have a primary key defined, add placeholder
			if len(table.PrimaryKey) == 0 {
				table.PrimaryKey = model.DelimitedList{"dbsteward_primary_key_not_found"}
				tableNoticeDesc := fmt.Sprintf("DBSTEWARD_EXTRACTION_WARNING: primary key definition not found for %s - placeholder has been specified for DTD validity", table.Name)
				dbsteward.Warning(tableNoticeDesc)
				if len(table.Description) == 0 {
					table.Description = tableNoticeDesc
				} else {
					table.Description += "; " + tableNoticeDesc
				}
			}

			// check owner and grant role definitions
			if !doc.IsRoleDefined(table.Owner) {
				doc.AddCustomRole(table.Owner)
			}
			for _, grant := range table.Grants {
				if !doc.IsRoleDefined(grant.Role) {
					doc.AddCustomRole(grant.Role)
				}
			}
		}
	}

	return doc
}
func (self *Operations) CompareDbData(doc *model.Definition, host string, port uint, name, user, pass string) *model.Definition {
	dbsteward := lib.GlobalDBSteward

	dbsteward.Notice("Connecting to pgsql8 host %s:%d database %s as user %s", host, port, name, user)
	GlobalDb.Connect(host, port, name, user, pass)

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
							dbsteward.Fatal("type of %s was found for column %s but it is foreign keyed", colType, column)
						}
						foreign := lib.GlobalDBX.ForeignKey(doc, schema, table, column)
						colType = foreign.Column.Type
					}

					if len(colType) == 0 {
						dbsteward.Fatal("%s column %s type was not found", table.Name, column.Name)
					}

					colTypes[column.Name] = colType
				}

				for _, row := range table.Rows.Rows {
					// TODO(go,nth) can we fix this direct sql construction with a ToSql struct?
					pkExprs := []string{}
					for _, pkCol := range pkCols {
						// TODO(go,nth) can we put this column lookup in the model? `row.GetValueForColumn(pkCol)`
						pkIndex := util.IndexOfStr(pkCol, cols)
						if pkIndex < 0 {
							dbsteward.Fatal("failed to find %s.%s primary key column %s in cols list %v",
								schema.Name, table.Name, pkCol, cols)
						}

						expr := fmt.Sprintf(
							"%s = %s",
							self.QuoteColumn(pkCol),
							// TODO(feat) what about row.Columns[pkIndex].Null
							self.ValueEscape(colTypes[pkCol], row.Columns[pkIndex].Text, doc),
						)
						pkExprs = append(pkExprs, expr)
					}
					pkExpr := strings.Join(pkExprs, " AND ")

					// TODO(go,nth) use parameterized queries
					sql := fmt.Sprintf(`SELECT * FROM %s WHERE %s`, tableName, pkExpr)
					res := GlobalDb.Query(sql)

					if row.Delete {
						if res.RowCount() > 0 {
							dbsteward.Notice("%s row marked for DELETE found WHERE %s", tableName, pkExpr)
						}
					} else if res.RowCount() == 0 {
						dbsteward.Notice("%s does not contain row WHERE %s", tableName, pkExpr)
					} else if res.RowCount() > 1 {
						dbsteward.Notice("%s contains more than one row WHERE %s", tableName, pkExpr)
						for res.Next() {
							dbRow := res.FetchRowStringMap()
							dbsteward.Notice("\t%v", dbRow)
						}
						// TODO(go,nth) error handling
					} else {
						res.Next()
						dbRow := res.FetchRowStringMap()
						for i, col := range cols {
							// TODO(feat) what about row.Columns[i].Null?
							valuesMatch, xmlValue, dbValue := self.compareDbDataRow(colTypes[col], row.Columns[i].Text, dbRow[col])
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
func (self *Operations) compareDbDataRow(colType, xmlValue, dbValue string) (bool, string, string) {
	colType = strings.ToLower(colType)
	xmlValue = self.pgdataHomogenize(colType, xmlValue)
	dbValue = self.pgdataHomogenize(colType, dbValue)
	if xmlValue == dbValue {
		return true, xmlValue, dbValue
	}

	// if they are not equal, and are alternately expressable, ask the database
	if strings.HasPrefix(colType, "time") || strings.HasPrefix(colType, "date") || strings.HasPrefix(colType, "interval") {
		if len(xmlValue) > 0 && len(dbValue) > 0 {
			sql := fmt.Sprintf(`SELECT $1::%s = $2::%[1]s`, colType)
			var eq bool
			err := GlobalDb.QueryVal(&eq, sql, xmlValue, dbValue)
			lib.GlobalDBSteward.FatalIfError(err, "Could not query database")
			return eq, xmlValue, dbValue
		}
	}

	return false, xmlValue, dbValue
}
func (self *Operations) pgdataHomogenize(colType string, value string) string {
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

func (self *Operations) SqlDiff(old, new []string, upgradePrefix string) {
	lib.GlobalDBSteward.Notice("Calculating sql differences:")
	lib.GlobalDBSteward.Notice("Old set: %v", old)
	lib.GlobalDBSteward.Notice("New set: %v", new)
	lib.GlobalDBSteward.Notice("Upgrade: %s", upgradePrefix)
	GlobalDiff.DiffSql(old, new, upgradePrefix)
}

func (self *Operations) SlonyCompare(file string) {
	// TODO(go,slony)
}
func (self *Operations) SlonyDiff(oldFile string, newFile string) {
	// TODO(go,slony)
}

func (self *Operations) functionDefinitionReferencesTable(definition *model.FunctionDefinition) string {
	// TODO(go,nth) move this to model?
	// TODO(feat) a function could reference many tables, but this only returns the first; make it understand many tables
	// TODO(feat) this won't detect quoted table names
	// TODO(go,pgsql) test this
	validTableName := `[\w\.]+`
	if matches := util.IMatch(fmt.Sprintf(`SELECT\s+.+\s+FROM\s+(%s)`, validTableName), definition.Text); matches != nil {
		return matches[1]
	}
	if matches := util.IMatch(fmt.Sprintf(`INSERT\s+INTO\s+(%s)`, validTableName), definition.Text); matches != nil {
		return matches[1]
	}
	if matches := util.IMatch(fmt.Sprintf(`DELETE\s+FROM\s+(?:ONLY)?\s*(%s)`, validTableName), definition.Text); matches != nil {
		return matches[1]
	}
	if matches := util.IMatch(fmt.Sprintf(`UPDATE\s+(?:ONLY)?\s*(%s)`, validTableName), definition.Text); matches != nil {
		return matches[1]
	}
	return ""
}

func (self *Operations) BuildSchema(doc *model.Definition, ofs output.OutputFileSegmenter, tableDep []*model.TableDepEntry) {
	// schema creation
	for _, schema := range doc.Schemas {
		ofs.WriteSql(GlobalSchema.GetCreationSql(schema)...)

		// schema grants
		for _, grant := range schema.Grants {
			// TODO(feat) revokes too?
			ofs.WriteSql(GlobalPermission.GetGrantSql(doc, schema, schema, grant)...)
		}
	}

	// types: enumerated list, etc
	for _, schema := range doc.Schemas {
		for _, datatype := range schema.Types {
			ofs.WriteSql(GlobalDataType.GetCreationSql(schema, datatype)...)
		}
	}

	// table structure creation
	for _, schema := range doc.Schemas {
		// create defined tables
		GlobalTable.IncludeColumnDefaultNextvalInCreateSql = false
		for _, table := range schema.Tables {
			// table definition
			ofs.WriteSql(GlobalTable.GetCreationSql(schema, table)...)

			// table indexes
			GlobalDiffIndexes.DiffIndexesTable(ofs, nil, nil, schema, table)

			// table grants
			for _, grant := range table.Grants {
				// TODO(feat) revokes too?
				ofs.WriteSql(GlobalPermission.GetGrantSql(doc, schema, table, grant)...)
			}
		}
		GlobalTable.IncludeColumnDefaultNextvalInCreateSql = true

		// sequences contained in the schema
		for _, sequence := range schema.Sequences {
			ofs.WriteSql(GlobalSequence.GetCreationSql(schema, sequence)...)

			// sequence permission grants
			for _, grant := range sequence.Grants {
				// TODO(feat) revokes too?
				ofs.WriteSql(GlobalPermission.GetGrantSql(doc, schema, sequence, grant)...)
			}
		}

		// add table nextvals that were omitted
		for _, table := range schema.Tables {
			if table.HasDefaultNextVal() {
				ofs.WriteSql(GlobalTable.GetDefaultNextvalSql(schema, table)...)
			}
		}
	}

	// function definitions
	for _, schema := range doc.Schemas {
		for _, function := range schema.Functions {
			if function.HasDefinition() {
				ofs.WriteSql(GlobalFunction.GetCreationSql(schema, function)...)
				// when pg:build_schema() is doing its thing for straight builds, include function permissions
				// they are not included in pg_function::get_creation_sql()

				// TODO(feat) functions generate sql for both grant and revoke, but other objects only do grant? can we unify this?
				// TODO(go,pgsql) verify that order of this doesn't matter. this code does grants then revokes, orig does them in xpath order
				for _, grant := range function.Grants {
					ofs.WriteSql(GlobalPermission.GetGrantSql(doc, schema, function, grant)...)
				}
				for _, revoke := range function.Revokes {
					ofs.WriteSql(GlobalPermission.GetRevokeSql(doc, schema, function, revoke)...)
				}
			}
		}
	}

	// maybe move this but here we're defining column defaults fo realz
	for _, schema := range doc.Schemas {
		for _, table := range schema.Tables {
			// TODO(go,nth) method name consistency - should be GetColumnDefaultsSql?
			ofs.WriteSql(GlobalTable.DefineTableColumnDefaults(schema, table)...)
		}
	}

	// define table primary keys before foreign keys so unique requirements are always met for FOREIGN KEY constraints
	for _, schema := range doc.Schemas {
		for _, table := range schema.Tables {
			GlobalDiffTables.DiffConstraintsTable(ofs, nil, nil, schema, table, "primaryKey", false)
		}
	}

	// foreign key references
	// use the dependency order to specify foreign keys in an order that will satisfy nested foreign keys and etc
	// TODO(feat) shouldn't this consider GlobalDBSteward.LimitToTables like BuildData does?
	for _, entry := range tableDep {
		if entry.IgnoreEntry {
			continue
		}

		GlobalDiffTables.DiffConstraintsTable(ofs, nil, nil, entry.Schema, entry.Table, "constraint", false)
	}

	// trigger definitions
	for _, schema := range doc.Schemas {
		for _, trigger := range schema.Triggers {
			if trigger.SqlFormat == model.SqlFormatPgsql8 {
				ofs.WriteSql(GlobalTrigger.GetCreationSql(schema, trigger)...)
			}
		}
	}

	GlobalDiffViews.CreateViewsOrdered(ofs, nil, doc)

	// view permission grants
	for _, schema := range doc.Schemas {
		for _, view := range schema.Views {
			for _, grant := range view.Grants {
				// TODO(feat) revokes too?
				ofs.WriteSql(GlobalPermission.GetGrantSql(doc, schema, view, grant)...)
			}
		}
	}

	GlobalDiff.UpdateDatabaseConfigParameters(ofs, nil, doc)
}

func (self *Operations) BuildData(doc *model.Definition, ofs output.OutputFileSegmenter, tableDep []*model.TableDepEntry) {
	limitToTables := lib.GlobalDBSteward.LimitToTables

	// use the dependency order to then write out the actual data inserts into the data sql file
	for _, entry := range tableDep {
		if entry.IgnoreEntry {
			continue
		}
		schema := entry.Schema
		table := entry.Table

		// skip any tables that are not in the limit list, if there are any tables to limit
		if len(limitToTables) > 0 {
			if includeTables, ok := limitToTables[schema.Name]; ok {
				if !util.InArrayStr(table.Name, includeTables) {
					continue
				}
			} else {
				// if this entry's schema didn't appear in the include list, we can't possibly include any tables from it
				continue
			}
		}

		ofs.WriteSql(GlobalDiffTables.GetDataSql(nil, nil, schema, table, false)...)

		// set serial primary keys to the max value after inserts have been performed
		// only if the PRIMARY KEY is not a multi column
		if table.Rows != nil && len(table.PrimaryKey) == 1 {
			dataCols := table.Rows.Columns
			pkCol := table.PrimaryKey[0]
			if util.InArrayStr(pkCol, dataCols) {
				// TODO(go,3) seems like this could be refactored better by putting much of the lookup
				// into the model structs
				cols := lib.GlobalXmlParser.InheritanceGetColumn(table, pkCol)
				if len(cols) != 1 {
					lib.GlobalDBSteward.Fatal("Failed to find primary key column '%s' for %s.%s",
						pkCol, schema.Name, table.Name)
				}
				pk := cols[0]
				if GlobalDataType.IsLinkedTableType(pk.Type) {
					if len(pk.SerialStart) > 0 {
						ofs.WriteSql(&sql.SetValSerialSequenceMax{
							Schema: schema.Name,
							Table:  table.Name,
							Column: pk.Name,
						})
					}
				}
			}
		}

		// check if primary key columns are columns of this table
		// TODO(feat) does this check belong here? should there be some kind of post-parse validation?
		for _, columnName := range table.PrimaryKey {
			cols := lib.GlobalXmlParser.InheritanceGetColumn(table, columnName)
			if len(cols) != 1 {
				lib.GlobalDBSteward.Fatal("Declared primary key column (%s) does not exist as column in table %s.%s",
					columnName, schema.Name, table.Name)
			}
		}
	}

	// include all of the unstaged sql elements
	lib.GlobalDBX.BuildStagedSql(doc, ofs, "")
}

func (self *Operations) ValueEscape(datatype string, value string, doc *model.Definition) string {
	// TODO(go,3) it'd be amazing to have a dedicated Value type that encapsulates this logic and is type-aware, instead of the mishmash of string parsing and type matching we do
	if len(value) == 0 {
		// TODO(feat) this can't distinguish between empty strings and null
		return "NULL"
	}

	// complain when we require verbose interval notation but data uses a different format
	if lib.GlobalDBSteward.RequireVerboseIntervalNotation && util.IMatch("interval", datatype) != nil && value[0] != '@' {
		lib.GlobalDBSteward.Fatal("bad interval value: '%s' -- interval types must be postgresql verbose format: '@ 2 hours 30 minutes'", value)
	}

	// data types that should be quoted
	enumRegex := lib.GlobalDBX.EnumRegex(doc)
	if len(enumRegex) > 0 {
		enumRegex = "|" + enumRegex
	}
	if util.IMatch(fmt.Sprintf(`^(bool.*|character.*|string|text|date|time.*|(var)?char.*|interval|money|inet|uuid|ltree%s)`, enumRegex), datatype) != nil {
		// data types that should have E prefix to their quotes
		if self.EscapeStringValues && util.IMatch(`^(character.*|string|text|(var)?char.*)`, datatype) != nil {
			return self.LiteralStringEscaped(value)
		} else {
			return self.LiteralString(value)
		}
	}

	return value
}

func (self *Operations) LiteralString(str string) string {
	return fmt.Sprintf("'%s'", strings.ReplaceAll(str, "'", "''"))
}

func (self *Operations) LiteralStringEscaped(str string) string {
	// TODO(go,nth) verify this works in all cases
	return "E" + self.LiteralString(str)
}

func (self *Operations) SetContextReplicaSetId(setId int) {
	self.contextReplicaSetId = setId
}

func (self *Operations) translateRoleName(role string) string {
	switch strings.ToLower(role) {
	/* TODO(feat) allow for special translations
	case "pgsql": return "ROLE_OWNER"
	case "dbsteward", "application1": return "ROLE_APPLICATION"
	*/
	default:
		return role
	}
}

// TODO(go,nth) should this live somewhere else?
// TODO(go,pgsql8) test this
func (self *Operations) parseSqlArray(str string) []string {
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

func (self *Operations) IdentifierName(schema, table, column, suffix string) string {
	// these will change as we build the identifier
	identTable := table
	identColumn := column

	maxLen := MAX_IDENT_LENGTH - 1 - len(suffix)
	tableMaxLen := int(math.Ceil(float64(maxLen) / 2.0))
	colMaxLen := maxLen - tableMaxLen

	// table is longer than max and column is shorter than max
	if len(identTable) > tableMaxLen && len(identColumn) < colMaxLen {
		// give column excess to table max
		tableMaxLen += colMaxLen - len(identColumn)
	}

	// table is shorter, column is longer
	// TODO(feat) should this be an else if?
	if len(identTable) < tableMaxLen && len(identColumn) > colMaxLen {
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

func (self *Operations) parseSequenceRelAcl(str string) map[string][]string {
	// will be receiving something like '{superuser=rwU/superuser_role,normal_role=rw/superuser_role}'
	// output {superuser: [select, usage, ...], ...}
	out := map[string][]string{}

	// TODO(feat) uhhh shouldn't there be more of these?
	mapping := map[rune]string{
		'a': "UPDATE",
		'r': "SELECT",
		'U': "USAGE",
	}

	for _, elem := range self.parseSqlArray(str) {
		userperms := strings.SplitN(elem, "=", 2)
		if len(userperms) == 1 {
			// no perms
			continue
		}
		user := userperms[0]
		perms := userperms[1]

		for _, c := range strings.SplitN(perms, "/", 2)[0] {
			if perm, ok := mapping[c]; ok {
				out[user] = append(out[user], perm)
			}
		}
	}

	return out
}
