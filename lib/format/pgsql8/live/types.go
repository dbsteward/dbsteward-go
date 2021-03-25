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

type SequenceRelEntry struct {
	Name  string
	Owner string
}

type SequenceEntry struct {
	Cache     *int
	Start     *int
	Min       *int
	Max       *int
	Increment *int
	Cycled    bool
}

type ViewEntry struct {
	Schema     string
	Name       string
	Owner      string
	Definition string
}
