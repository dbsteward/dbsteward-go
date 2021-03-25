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
