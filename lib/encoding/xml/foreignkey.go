package xml

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib/model"
)

type ForeignKey struct {
	Columns        DelimitedList `xml:"columns,attr"`
	ForeignSchema  string        `xml:"foreignSchema,attr,omitempty"`
	ForeignTable   string        `xml:"foreignTable,attr"`
	ForeignColumns DelimitedList `xml:"foreignColumns,attr,omitempty"`
	ConstraintName string        `xml:"constraintName,attr,omitempty"`
	IndexName      string        `xml:"indexName,attr,omitempty"`
	OnUpdate       string        `xml:"onUpdate,attr,omitempty"`
	OnDelete       string        `xml:"onDelete,attr,omitempty"`
}

func (fk *ForeignKey) ToModel() (*model.ForeignKey, error) {
	rv := model.ForeignKey{
		Columns:        fk.Columns,
		ForeignSchema:  fk.ForeignSchema,
		ForeignTable:   fk.ForeignTable,
		ForeignColumns: fk.ForeignColumns,
		ConstraintName: fk.ConstraintName,
		IndexName:      fk.IndexName,
	}
	var err error
	rv.OnUpdate, err = model.NewForeignKeyAction(fk.OnUpdate)
	if err != nil {
		return nil, err
	}
	rv.OnDelete, err = model.NewForeignKeyAction(fk.OnDelete)
	if err != nil {
		return nil, err
	}
	return &rv, nil
}

func (fk *ForeignKey) IdentityMatches(other *ForeignKey) bool {
	if fk == nil || other == nil {
		return false
	}
	// TODO(go,core) validate this constraint/index name matching behavior
	// TODO(feat) case sensitivity
	return strings.EqualFold(fk.ConstraintName, other.ConstraintName)
}
