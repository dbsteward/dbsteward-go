package live

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/util"
	"github.com/pkg/errors"
)

type IntrospectorFactory interface {
	NewIntrospector(*Connection) (Introspector, error)
}

type LiveIntrospectorFactory struct{}

func (*LiveIntrospectorFactory) NewIntrospector(conn *Connection) (Introspector, error) {
	return NewIntrospector(conn)
}

type Introspector interface {
	GetTableList() ([]TableEntry, error)
	GetSchemaOwner(schema string) (string, error)
	GetTableStorageOptions(schema, table string) (map[string]string, error)
	GetColumns(schema, table string) ([]ColumnEntry, error)
	GetIndexes(schema, table string) ([]IndexEntry, error)
	GetSequenceRelList(schema string, sequenceCols []string) ([]SequenceRelEntry, error)
	GetSequencesForRel(schema, rel string) ([]SequenceEntry, error)
	GetViews() (StringMapList, error)
	GetConstraints() (StringMapList, error)
	GetForeignKeys() (StringMapList, error)
	GetFunctions() (StringMapList, error)
	GetFunctionArgs(fnOid string) (StringMapList, error)
	GetTriggers() (StringMapList, error)
	GetTablePerms() (StringMapList, error)
	GetSequencePerms(seq string) (StringMapList, error)
}

type LiveIntrospector struct {
	conn *Connection
	vers int
}

var _ Introspector = &LiveIntrospector{}

// TODO(go,3) can we elevate this to an engine-agnostic interface?
// TODO(go,3) can we defer this to model operations entirely?

func NewIntrospector(conn *Connection) (*LiveIntrospector, error) {
	vers, err := conn.Version()
	if err != nil {
		return nil, err
	}
	return &LiveIntrospector{conn, vers}, nil
}

func (self *LiveIntrospector) GetTableList() ([]TableEntry, error) {
	// TODO(go,3) move column description to column query
	// Note that old versions of postgres don't support array_agg(description ORDER BY objsubid)
	// so we need to use subquery to do ordering
	res, err := self.conn.QueryRaw(`
		SELECT
			t.schemaname, t.tablename, t.tableowner, t.tablespace,
			sd.description as schema_description, td.description as table_description,
			( SELECT array_agg(pn.nspname || '.' || pc.relname)
				FROM pg_catalog.pg_inherits i
				LEFT JOIN pg_catalog.pg_class pc ON (i.inhparent = pc.oid)
				LEFT JOIN pg_catalog.pg_namespace pn ON (pc.relnamespace = pn.oid)
				WHERE i.inhrelid = c.oid) AS parent_tables
		FROM pg_catalog.pg_tables t
		LEFT JOIN pg_catalog.pg_namespace n ON (n.nspname = t.schemaname)
		LEFT JOIN pg_catalog.pg_class c ON (c.relname = t.tablename AND c.relnamespace = n.oid)
		LEFT JOIN pg_catalog.pg_description td ON (td.objoid = c.oid AND td.classoid = c.tableoid AND td.objsubid = 0)
		LEFT JOIN pg_catalog.pg_description sd ON (sd.objoid = n.oid)
		WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
		ORDER BY schemaname, tablename;
	`)
	if err != nil {
		return nil, errors.Wrap(err, "while running query")
	}

	out := []TableEntry{}
	for res.Next() {
		entry := TableEntry{}
		err := res.Scan(
			&entry.Schema, &entry.Table, &entry.Owner, &entry.Tablespace,
			&maybeStr{&entry.SchemaDescription}, &maybeStr{&entry.TableDescription},
			&entry.ParentTables,
		)
		if err != nil {
			return nil, errors.Wrap(err, "while scanning result")
		}
		out = append(out, entry)
	}
	if err := res.Err(); err != nil {
		return nil, errors.Wrap(err, "while iterating results")
	}
	return out, nil
}

func (self *LiveIntrospector) GetSchemaOwner(schema string) (string, error) {
	var owner string
	err := self.conn.QueryVal(&owner, `SELECT schema_owner FROM information_schema.schemata WHERE schema_name = $1`, schema)
	return owner, err
}

func (self *LiveIntrospector) GetTableStorageOptions(schema, table string) (map[string]string, error) {
	// TODO(feat) can we just add this to the main query?
	// NOTE: pg 11.0 dropped support for "with oids" or "oids=true" in DDL
	//       pg 12.0 drops the relhasoids column from pg_class
	relhasoids := "false"
	reloptions := ""
	if self.vers < Version12_0 {
		paramsRow, err := self.conn.QueryStringMap(`
			SELECT reloptions, relhasoids
			FROM pg_catalog.pg_class
			WHERE relname = $1
				AND relnamespace = (
					SELECT oid
					FROM pg_catalog.pg_namespace
					WHERE nspname = $2
				)
		`, table, schema)
		if err != nil {
			return nil, err
		}
		reloptions = paramsRow["reloptions"]
		relhasoids = paramsRow["relhasoids"]
	} else {
		paramsRow, err := self.conn.QueryStringMap(`
			SELECT reloptions
			FROM pg_catalog.pg_class
			WHERE relname = $1
				AND relnamespace = (
					SELECT oid
					FROM pg_catalog.pg_namespace
					WHERE nspname = $2
				)
		`, table, schema)
		if err != nil {
			return nil, err
		}
		reloptions = paramsRow["reloptions"]
	}

	// reloptions is formatted as {name=value,name=value}
	params := map[string]string{}
	if len(reloptions) > 2 {
		params = util.ParseKV(reloptions[1:len(reloptions)-1], ",", "=")
	}
	// dbsteward/dbsteward#97: with oids=false is the default
	if hasoids := util.IsTruthy(relhasoids); hasoids {
		params["oids"] = "true"
	}

	return params, nil
}

func (self *LiveIntrospector) GetColumns(schema, table string) ([]ColumnEntry, error) {
	res, err := self.conn.QueryRaw(`
		SELECT
			column_name, column_default, is_nullable = 'YES', pgd.description,
			ordinal_position, format_type(atttypid, atttypmod) as attribute_data_type
		FROM information_schema.columns
			JOIN pg_class pgc ON (pgc.relname = table_name AND pgc.relkind='r')
			JOIN pg_namespace nsp ON (nsp.nspname = table_schema AND nsp.oid = pgc.relnamespace)
			JOIN pg_attribute pga ON (pga.attrelid = pgc.oid AND columns.column_name = pga.attname)
			LEFT JOIN pg_description pgd ON (pgd.objoid = pgc.oid AND pgd.classoid = pgc.tableoid AND pgd.objsubid = ordinal_position)
		WHERE table_schema=$1 AND table_name=$2
			AND attnum > 0
			AND NOT attisdropped
		ORDER BY ordinal_position ASC
	`, schema, table)
	if err != nil {
		return nil, errors.Wrap(err, "while running query")
	}

	out := []ColumnEntry{}
	for res.Next() {
		entry := ColumnEntry{}
		err := res.Scan(
			&entry.Name, &maybeStr{&entry.Default}, &entry.Nullable,
			&maybeStr{&entry.Description}, &entry.Position, &entry.AttrType,
		)
		if err != nil {
			return nil, errors.Wrap(err, "while scanning result")
		}
		out = append(out, entry)
	}
	if err := res.Err(); err != nil {
		return nil, errors.Wrap(err, "while iterating results")
	}
	return out, nil
}

func (self *LiveIntrospector) GetIndexes(schema, table string) ([]IndexEntry, error) {
	// TODO(go,nth) double check the `relname NOT IN` clause, it smells fishy to me
	res, err := self.conn.QueryRaw(`
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
	`, schema, table)
	if err != nil {
		return nil, errors.Wrap(err, "while running query")
	}

	out := []IndexEntry{}
	for res.Next() {
		entry := IndexEntry{}
		err := res.Scan(&entry.Name, &entry.Unique, &entry.Dimensions)
		if err != nil {
			return nil, errors.Wrap(err, "while scanning result")
		}
		out = append(out, entry)
	}
	if err := res.Err(); err != nil {
		return nil, errors.Wrap(err, "while iterating results")
	}
	return out, nil
}

func (self *LiveIntrospector) GetSequenceRelList(schema string, sequenceCols []string) ([]SequenceRelEntry, error) {
	sql := `
		SELECT s.relname, r.rolname
		FROM pg_statio_all_sequences s
		JOIN pg_class c ON (s.relname = c.relname)
		JOIN pg_roles r ON (c.relowner = r.oid)
		WHERE schemaname = $1
	`
	params := []interface{}{schema}
	if len(sequenceCols) > 0 {
		sql += `AND s.relname != ANY($2)`
		params = append(params, sequenceCols)
	}
	sql += `GROUP BY s.relname, r.rolname`
	res, err := self.conn.QueryRaw(sql, params...)
	if err != nil {
		return nil, errors.Wrap(err, "while running query")
	}

	out := []SequenceRelEntry{}
	for res.Next() {
		entry := SequenceRelEntry{}
		err := res.Scan(&entry.Name, &entry.Owner)
		if err != nil {
			return nil, errors.Wrap(err, "while scanning result")
		}
		out = append(out, entry)
	}
	if err := res.Err(); err != nil {
		return nil, errors.Wrap(err, "while iterating results")
	}
	return out, nil
}

func (self *LiveIntrospector) GetSequencesForRel(schema, rel string) ([]SequenceEntry, error) {
	// TODO(feat) should this read from a catalog instead? can we do away with the dynamic sql? can we merge into GetSequenceRelList()?
	res, err := self.conn.QueryRaw(fmt.Sprintf(`
		SELECT cache_value, start_value, min_value, max_value, increment_by, is_cycled
		FROM "%s"."%s"
	`, schema, rel))
	if err != nil {
		return nil, errors.Wrap(err, "while running query")
	}

	out := []SequenceEntry{}
	for res.Next() {
		entry := SequenceEntry{}
		err := res.Scan(&entry.Cache, &entry.Start, &entry.Min, &entry.Max, &entry.Increment, &entry.Cycled)
		if err != nil {
			return nil, errors.Wrap(err, "while scanning result")
		}
		out = append(out, entry)
	}
	if err := res.Err(); err != nil {
		return nil, errors.Wrap(err, "while iterating results")
	}
	return out, nil
}

func (self *LiveIntrospector) GetViews() (StringMapList, error) {
	return self.conn.Query(`
		SELECT *
      FROM pg_catalog.pg_views
      WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
      ORDER BY schemaname, viewname;
	`)
}

func (self *LiveIntrospector) GetConstraints() (StringMapList, error) {
	return self.conn.Query(`
		SELECT
			nspname AS table_schema,
			relname AS table_name,
			conname AS constraint_name,
			contype AS constraint_type,
			consrc AS check_src,
			ARRAY(
				SELECT attname
				FROM unnest(conkey) num
				INNER JOIN pg_catalog.pg_attribute pga ON pga.attrelid = pgt.oid AND pga.attnum = num
			) AS columns
		FROM pg_catalog.pg_constraint pgc
		LEFT JOIN pg_catalog.pg_class pgt ON pgc.conrelid = pgt.oid
		LEFT JOIN pg_catalog.pg_namespace pgn ON pgc.connamespace = pgn.oid
		WHERE pgn.nspname not in ('information_schema', 'pg_catalog')
			AND contype != 'f' -- ignore foreign keys here
		ORDER BY pgn.nspname, pgt.relname
	`)
}

func (self *LiveIntrospector) GetForeignKeys() (StringMapList, error) {
	// We cannot accurately retrieve FOREIGN KEYs via information_schema
	// We must rely on getting them from pg_catalog instead
	// See http://stackoverflow.com/questions/1152260/postgres-sql-to-list-table-foreign-keys
	return self.conn.Query(`
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
}

func (self *LiveIntrospector) GetFunctions() (StringMapList, error) {
	return self.conn.Query(`
		SELECT
			p.oid as oid, n.nspname as schema, p.proname as name,
			pg_catalog.pg_get_function_result(p.oid) as return_type,
			CASE
				WHEN p.proisagg THEN 'aggregate'
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
		WHERE n.nspname NOT IN ('pg_catalog', 'information_schema');
	`)
}

func (self *LiveIntrospector) GetFunctionArgs(fnOid string) (StringMapList, error) {
	// unnest the proargtypes (which are in ordinal order) and get the correct format for them.
	// information_schema.parameters does not contain enough information to get correct type (e.g. ARRAY)
	//   Note: * proargnames can be empty (not null) if there are no parameters names
	//         * proargnames will contain empty strings for unnamed parameters if there are other named
	//                       parameters, e.g. {"", parameter_name}
	//         * proargtypes is an oidvector, enjoy the hackery to deal with NULL proargnames
	//         * proallargtypes is NULL when all arguments are IN.
	// TODO(go,3) use something besides oid

	return self.conn.Query(`
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
	`, fnOid)
}

func (self *LiveIntrospector) GetTriggers() (StringMapList, error) {
	return self.conn.Query(`
		SELECT *
		FROM information_schema.triggers
		WHERE trigger_schema NOT IN ('pg_catalog', 'information_schema')
	`)
}

func (self *LiveIntrospector) GetTablePerms() (StringMapList, error) {
	return self.conn.Query(`
		SELECT table_schema, table_name, grantee, privilege_type, is_grantable
		FROM information_schema.table_privileges
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
	`)
}

func (self *LiveIntrospector) GetSequencePerms(seq string) (StringMapList, error) {
	return self.conn.Query(`SELECT relacl FROM pg_class WHERE relname = $1`, seq)
}
