package sql

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/output"
)

type TableCreate struct {
	Table        TableRef
	Columns      []ColumnDefinition
	Inherits     *TableRef
	OtherOptions []TableCreateOption // TODO make individual options first-class
}

type TableCreateOption struct {
	Option string
	Value  string
}

func (self *TableCreate) ToSql(q output.Quoter) string {
	cols := []string{}
	for _, col := range self.Columns {
		cols = append(cols, col.GetSql(q))
	}
	colsql := ""
	if len(cols) > 0 {
		colsql = fmt.Sprintf("\n\t%s\n", strings.Join(cols, ",\n\t"))
	}

	opts := []string{}
	for _, opt := range self.OtherOptions {
		opts = append(opts, fmt.Sprintf("%s %s", strings.ToUpper(opt.Option), opt.Value))
	}
	if self.Inherits != nil {
		opts = append(opts, fmt.Sprintf("INHERITS (%s)", self.Inherits.Qualified(q)))
	}
	optsql := ""
	if len(opts) > 0 {
		optsql = fmt.Sprintf("\n%s", strings.Join(opts, "\n"))
	}

	return fmt.Sprintf(
		"CREATE TABLE %s(%s)%s;",
		self.Table.Qualified(q),
		colsql,
		optsql,
	)
}

type TableSetComment struct {
	Table   TableRef
	Comment string
}

func (self *TableSetComment) ToSql(q output.Quoter) string {
	return fmt.Sprintf(
		"COMMENT ON TABLE %s IS %s;",
		self.Table.Qualified(q),
		q.LiteralString(self.Comment),
	)
}

type TableAlterOwner struct {
	Table TableRef
	Role  string
}

func (self *TableAlterOwner) ToSql(q output.Quoter) string {
	return NewTableAlter(self.Table, &TableAlterPartOwner{self.Role}).ToSql(q)
}

type TableGrant struct {
	Table    TableRef
	Perms    []string
	Roles    []string
	CanGrant bool
}

func (self *TableGrant) ToSql(q output.Quoter) string {
	return (&grant{
		grantTypeTable,
		&self.Table,
		self.Perms,
		self.Roles,
		self.CanGrant,
	}).ToSql(q)
}

type TableDrop struct {
	Table TableRef
}

func (self *TableDrop) ToSql(q output.Quoter) string {
	return fmt.Sprintf("DROP TABLE %s;", self.Table.Qualified(q))
}

type TableAlterRename struct {
	Table   TableRef
	NewName string
}

func (self *TableAlterRename) ToSql(q output.Quoter) string {
	return NewTableAlter(self.Table, &TableAlterPartRename{self.NewName}).ToSql(q)
}

type TableAlterSetSchema struct {
	Table     TableRef
	NewSchema string
}

func (self *TableAlterSetSchema) ToSql(q output.Quoter) string {
	return NewTableAlter(self.Table, &TableAlterPartSetSchema{self.NewSchema}).ToSql(q)
}

type TableMoveTablespaceIndexes struct {
	Table      TableRef
	Tablespace string
}

func (self *TableMoveTablespaceIndexes) ToSql(q output.Quoter) string {
	// TODO(go,3) DO blocks are introduced in 9.0, that would be much nicer than the IIFE
	return fmt.Sprintf(`
CREATE FUNCTION __dbsteward_migrate_move_index_tablespace(TEXT,TEXT,TEXT) RETURNS void AS $$
  DECLARE idx RECORD;
BEGIN
  -- need to move the tablespace of the indexes as well
  FOR idx IN SELECT index_pgc.relname FROM pg_index
               INNER JOIN pg_class index_pgc ON index_pgc.oid = pg_index.indexrelid
               INNER JOIN pg_class table_pgc ON table_pgc.oid = pg_index.indrelid AND table_pgc.relname=$2
               INNER JOIN pg_namespace ON pg_namespace.oid = table_pgc.relnamespace AND pg_namespace.nspname=$1 LOOP
    EXECUTE 'ALTER INDEX ' || quote_ident($1) || '.' || quote_ident(idx.relname) || ' SET TABLESPACE ' || quote_ident($3) || ';';
  END LOOP;
END $$ LANGUAGE plpgsql;
SELECT __dbsteward_migrate_move_index_tablespace(%s,%s,%s);
DROP FUNCTION __dbsteward_migrate_move_index_tablespace(TEXT,TEXT,TEXT);
	`, q.LiteralString(self.Table.Schema), q.LiteralString(self.Table.Table), q.LiteralString(self.Tablespace))
}

type TableResetTablespace struct {
	Table TableRef
}

func (self *TableResetTablespace) ToSql(q output.Quoter) string {
	return fmt.Sprintf(`
CREATE OR REPLACE FUNCTION __dbsteward_migrate_reset_tablespace(TEXT,TEXT) RETURNS void AS $$
  DECLARE tbsp TEXT;
  DECLARE idx RECORD;
BEGIN
  SELECT setting FROM pg_settings WHERE name='default_tablespace' INTO tbsp;

  IF tbsp = '' THEN
    tbsp := 'pg_default';
  END IF;

  EXECUTE 'ALTER TABLE ' || quote_ident($1) || '.' || quote_ident($2) || ' SET TABLESPACE ' || quote_ident(tbsp) || ';';

  -- need to move the tablespace of the indexes as well
  FOR idx IN SELECT index_pgc.relname FROM pg_index
               INNER JOIN pg_class index_pgc ON index_pgc.oid = pg_index.indexrelid
               INNER JOIN pg_class table_pgc ON table_pgc.oid = pg_index.indrelid AND table_pgc.relname=$2
               INNER JOIN pg_namespace ON pg_namespace.oid = table_pgc.relnamespace AND pg_namespace.nspname=$1 LOOP
    EXECUTE 'ALTER INDEX ' || quote_ident($1) || '.' || quote_ident(idx.relname) || ' SET TABLESPACE ' || quote_ident(tbsp) || ';';
  END LOOP;
END $$ LANGUAGE plpgsql;
SELECT __dbsteward_migrate_reset_tablespace(%s,%s);
DROP FUNCTION __dbsteward_migrate_reset_tablespace(TEXT,TEXT);
	`, q.LiteralString(self.Table.Schema), q.LiteralString(self.Table.Table))
}

type TableAlterClusterOn struct {
	Table TableRef
	Index string
}

func (self *TableAlterClusterOn) ToSql(q output.Quoter) string {
	if self.Index == "" {
		return NewTableAlter(self.Table, &TableAlterPartSetWithoutCluster{}).ToSql(q)
	}
	return NewTableAlter(self.Table, &TableAlterPartClusterOn{self.Index}).ToSql(q)
}
