package xml

import (
	"log/slog"
	"strings"

	"github.com/dbsteward/dbsteward/lib/ir"
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

func ForeignKeysFromIR(l *slog.Logger, ks []*ir.ForeignKey) ([]*ForeignKey, error) {
	if len(ks) == 0 {
		return nil, nil
	}
	var rv []*ForeignKey
	for _, k := range ks {
		if k != nil {
			rv = append(
				rv,
				&ForeignKey{
					Columns:        k.Columns,
					ForeignSchema:  k.ForeignSchema,
					ForeignTable:   k.ForeignTable,
					ForeignColumns: k.ForeignColumns,
					ConstraintName: k.ConstraintName,
					IndexName:      k.IndexName,
					OnUpdate:       string(k.OnUpdate),
					OnDelete:       string(k.OnDelete),
				},
			)
		}
	}
	return rv, nil
}

func (fk *ForeignKey) ToIR() (*ir.ForeignKey, error) {
	rv := ir.ForeignKey{
		Columns:        fk.Columns,
		ForeignSchema:  fk.ForeignSchema,
		ForeignTable:   fk.ForeignTable,
		ForeignColumns: fk.ForeignColumns,
		ConstraintName: fk.ConstraintName,
		IndexName:      fk.IndexName,
	}
	var err error
	rv.OnUpdate, err = ir.NewForeignKeyAction(fk.OnUpdate)
	if err != nil {
		return nil, err
	}
	rv.OnDelete, err = ir.NewForeignKeyAction(fk.OnDelete)
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
