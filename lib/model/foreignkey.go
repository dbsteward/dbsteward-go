package model

type ForeignKeyAction string

const (
	ForeignKeyActionNoAction   ForeignKeyAction = "NO_ACTION"
	ForeignKeyActionRestrict   ForeignKeyAction = "RESTRICT"
	ForeignKeyActionCascade    ForeignKeyAction = "CASCADE"
	ForeignKeyActionSetNull    ForeignKeyAction = "SET_NULL"
	ForeignKeyActionSetDefault ForeignKeyAction = "SET_DEFAULT"
)

type ForeignKey struct {
	Columns        DelimitedList    `xml:"columns,attr"`
	ForeignSchema  string           `xml:"foreignSchema,attr,omitempty"`
	ForeignTable   string           `xml:"foreignTable,attr"`
	ForeignColumns DelimitedList    `xml:"foreignColumns,attr,omitempty"`
	ConstraintName string           `xml:"constraintName,attr,omitempty"`
	IndexName      string           `xml:"indexName,attr,omitempty"`
	OnUpdate       ForeignKeyAction `xml:"onUpdate,attr,omitempty"`
	OnDelete       ForeignKeyAction `xml:"onDelete,attr,omitempty"`
}

func (self *ForeignKey) GetReferencedKey() KeyNames {
	cols := self.ForeignColumns
	if len(cols) == 0 {
		cols = self.Columns
	}
	return KeyNames{
		Schema:  self.ForeignSchema,
		Table:   self.ForeignTable,
		Columns: cols,
		KeyName: self.ConstraintName,
	}
}
