package live

type TableEntry struct {
	Schema            string
	Table             string
	Owner             string
	Tablespace        *string
	SchemaDescription string
	TableDescription  string
	ParentTables      []string
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
