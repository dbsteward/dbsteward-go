package pgsql8

import (
	"database/sql"

	"github.com/jackc/pgtype"
)

type Structure struct {
	Version     VersionNum
	Database    Database
	Schemas     []SchemaEntry
	Tables      []TableEntry
	Sequences   []SequenceRelEntry
	Views       []ViewEntry
	Constraints []ConstraintEntry
	ForeignKeys []ForeignKeyEntry
	Functions   []FunctionEntry
	Triggers    []TriggerEntry
	TablePerms  []TablePermEntry
	SchemaPerms []SchemaPermEntry
}

type SchemaEntry struct {
	Name        string
	Owner       string
	Description string
}

type TableEntry struct {
	Schema            string
	Table             string
	Owner             string
	Columns           []ColumnEntry
	Indexes           []IndexEntry
	Tablespace        *string
	SchemaDescription string
	TableDescription  string
	ParentTables      []string
	StorageOptions    map[string]string
}

type ColumnEntry struct {
	Name        string
	Default     string
	Nullable    bool
	Description string
	Position    int
	AttrType    string
}

type IndexEntry struct {
	Name       string
	Unique     bool
	Dimensions []string
}

type SequenceRelEntry struct {
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

type SequenceEntry struct {
	Cache     sql.NullInt64
	Start     sql.NullInt64
	Min       sql.NullInt64
	Max       sql.NullInt64
	Increment sql.NullInt64
	Cycled    bool
}

type ViewEntry struct {
	Schema      string
	Name        string
	Description string
	Owner       string
	Definition  string
}

type ConstraintEntry struct {
	Schema   string
	Table    string
	Name     string
	Type     string
	CheckDef *string
	Columns  []string
}

type ForeignKeyEntry struct {
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

type Oid struct {
	pgtype.OID
}

type FunctionEntry struct {
	Oid         Oid
	Schema      string
	Name        string
	Return      string
	Type        string
	Volatility  string
	Owner       string
	Language    string
	Source      string
	Description string
	Args        []FunctionArgEntry
}

type FunctionArgEntry struct {
	Name      string
	Type      string
	Direction string
}

type TriggerEntry struct {
	Schema      string
	Table       string
	Name        string
	Event       string
	Timing      string
	Orientation string
	Statement   string
}

type SchemaPermEntry struct {
	Schema    string
	Grantee   string
	Type      string
	Grantable bool
}

type TablePermEntry struct {
	Schema    string
	Table     string
	Grantee   string
	Type      string
	Grantable bool
}
