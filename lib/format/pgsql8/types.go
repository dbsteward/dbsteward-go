package pgsql8

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/jackc/pgtype"
)

type structure struct {
	Version     VersionNum
	Database    Database
	Schemas     []schemaEntry
	Tables      []tableEntry
	Sequences   []sequenceRelEntry
	Views       []viewEntry
	Constraints []constraintEntry
	ForeignKeys []foreignKeyEntry
	Functions   []functionEntry
	Triggers    []triggerEntry
	TablePerms  []tablePermEntry
	SchemaPerms []schemaPermEntry
}

type schemaEntry struct {
	Name        string
	Owner       string
	Description string
}

type tableEntry struct {
	Schema            string
	Table             string
	Owner             string
	Columns           []columnEntry
	Indexes           []indexEntry
	Tablespace        *string
	SchemaDescription string
	TableDescription  string
	ParentTables      []string
	StorageOptions    map[string]string
}

type columnEntry struct {
	Name        string
	Default     string
	Nullable    bool
	Description string
	Position    int
	AttrType    string
}

type indexEntry struct {
	Name       string
	Unique     bool
	Using      string
	Condition  string
	Dimensions []string
}

func (i indexEntry) UsingToIR() ir.IndexType {
	switch strings.ToLower(i.Using) {
	case "btree":
		return ir.IndexTypeBtree
	case "hash":
		return ir.IndexTypeHash
	case "gin":
		return ir.IndexTypeGin
	case "gist":
		return ir.IndexTypeGist
	default:
		panic(fmt.Sprintf("unknown index type '%s'", i.Using))
	}
}

type sequenceRelEntry struct {
	Schema       string
	Name         string
	SerialSchema string
	SerialTable  string
	SerialColumn string
	Description  string
	Owner        string
	Cache        sql.NullInt64
	Start        sql.NullInt64
	Min          sql.NullInt64
	Max          sql.NullInt64
	Increment    sql.NullInt64
	Cycled       bool
	ACL          []string
}

type sequenceEntry struct {
	Cache     sql.NullInt64
	Start     sql.NullInt64
	Min       sql.NullInt64
	Max       sql.NullInt64
	Increment sql.NullInt64
	Cycled    bool
}

type viewEntry struct {
	Schema      string
	Name        string
	Description string
	Owner       string
	Definition  string
}

type constraintEntry struct {
	Schema   string
	Table    string
	Name     string
	Type     string
	CheckDef *string
	Columns  []string
}

type foreignKeyEntry struct {
	ConstraintName string
	UpdateRule     string
	DeleteRule     string
	LocalSchema    string
	LocalTable     string
	LocalColumns   []string
	ForeignSchema  string
	ForeignTable   string
	ForeignColumns []string
}

type functionEntry struct {
	Oid         pgtype.OID
	Schema      string
	Name        string
	Return      string
	Type        string
	Volatility  string
	Owner       string
	Language    string
	Source      string
	Description string
	Args        []functionArgEntry
}

type functionArgEntry struct {
	Name      string
	Type      string
	Direction string
}

type triggerEntry struct {
	Schema      string
	Table       string
	Name        string
	Event       string
	Timing      string
	Orientation string
	Statement   string
}

type schemaPermEntry struct {
	Schema    string
	Grantee   string
	Type      string
	Grantable bool
}

type tablePermEntry struct {
	Schema    string
	Table     string
	Grantee   string
	Type      string
	Grantable bool
}
