package live

type TableEntry struct {
	Schema             string
	Table              string
	Owner              string
	Tablespace         *string
	SchemaDescription  string
	TableDescription   string
	ColumnDescriptions []string
	ParentTables       []string
}
