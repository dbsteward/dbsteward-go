package pgsql8

import (
	"database/sql"

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
	Dimensions []string
}

type sequenceRelEntry struct {
	Schema      string
	Name        string
	Description string
	Owner       string
	Cache       sql.NullInt64
	Start       sql.NullInt64
	Min         sql.NullInt64
	Max         sql.NullInt64
	Increment   sql.NullInt64
	Cycled      bool
	ACL         []string
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
