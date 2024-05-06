package xml

import (
	"encoding/xml"
	"fmt"
	"log/slog"

	"github.com/dbsteward/dbsteward/lib/ir"
)

type Column struct {
	Name             string `xml:"name,attr,omitempty"`
	Type             string `xml:"type,attr,omitempty"`
	Nullable         bool   `xml:"null,attr"` // TODO(go,nth) find a way to omit this when true
	Default          string `xml:"default,attr,omitempty"`
	Description      string `xml:"description,attr,omitempty"`
	Unique           bool   `xml:"unique,attr,omitempty"`
	Check            string `xml:"check,attr,omitempty"`
	SerialStart      *int   `xml:"serialStart,attr,omitempty"`
	OldColumnName    string `xml:"oldColumnName,attr,omitempty"`
	ConvertUsing     string `xml:"convertUsing,attr,omitempty"`
	ForeignSchema    string `xml:"foreignSchema,attr,omitempty"`
	ForeignTable     string `xml:"foreignTable,attr,omitempty"`
	ForeignColumn    string `xml:"foreignColumn,attr,omitempty"`
	ForeignKeyName   string `xml:"foreignKeyName,attr,omitempty"`
	ForeignIndexName string `xml:"foreignIndexName,attr,omitempty"`
	ForeignOnUpdate  string `xml:"foreignOnUpdate,attr,omitempty"`
	ForeignOnDelete  string `xml:"foreignOnDelete,attr,omitempty"`
	Statistics       *int   `xml:"statistics,attr,omitempty"` // TODO(feat) this doesn't show up in the DTD
	BeforeAddStage1  string `xml:"beforeAddStage1,attr,omitempty"`
	AfterAddStage1   string `xml:"afterAddStage1,attr,omitempty"`
	BeforeAddStage2  string `xml:"beforeAddStage2,attr,omitempty"`
	AfterAddStage2   string `xml:"afterAddStage2,attr,omitempty"`
	BeforeAddStage3  string `xml:"beforeAddStage3,attr,omitempty"`
	AfterAddStage3   string `xml:"afterAddStage3,attr,omitempty"`

	// These are DEPRECATED, replaced by Before/AfterAddStageN. see ConvertStageDirectives
	AfterAddPreStage1  string `xml:"afterAddPreStage1,attr,omitempty"`
	AfterAddPostStage1 string `xml:"afterAddPostStage1,attr,omitempty"`
	AfterAddPreStage2  string `xml:"afterAddPreStage2,attr,omitempty"`
	AfterAddPostStage2 string `xml:"afterAddPostStage2,attr,omitempty"`
	AfterAddPreStage3  string `xml:"afterAddPreStage3,attr,omitempty"`
	AfterAddPostStage3 string `xml:"afterAddPostStage3,attr,omitempty"`
}

func ColumnsFromIR(l *slog.Logger, cols []*ir.Column) ([]*Column, error) {
	if len(cols) == 0 {
		return nil, nil
	}
	var rv []*Column
	for _, c := range cols {
		if c != nil {
			nc, err := ColumnFromIR(l, c)
			if err != nil {
				return nil, err
			}
			rv = append(rv, nc)
		}
	}
	return rv, nil
}

func ColumnFromIR(l *slog.Logger, col *ir.Column) (*Column, error) {
	if col == nil {
		return nil, nil
	}
	l = l.With(slog.String("column", col.Name))
	l.Debug("converting column")
	defer l.Debug("done converting column")
	rv := Column{
		Name:             col.Name,
		Type:             col.Type,
		Nullable:         col.Nullable,
		Default:          col.Default,
		Description:      col.Description,
		Unique:           col.Unique,
		Check:            col.Check,
		SerialStart:      col.SerialStart,
		OldColumnName:    col.OldColumnName,
		ConvertUsing:     col.ConvertUsing,
		ForeignSchema:    col.ForeignSchema,
		ForeignTable:     col.ForeignTable,
		ForeignColumn:    col.ForeignColumn,
		ForeignKeyName:   col.ForeignKeyName,
		ForeignIndexName: col.ForeignIndexName,
		ForeignOnUpdate:  string(col.ForeignOnUpdate),
		ForeignOnDelete:  string(col.ForeignOnDelete),
		Statistics:       col.Statistics,
		BeforeAddStage1:  col.BeforeAddStage1,
		AfterAddStage1:   col.AfterAddStage1,
		BeforeAddStage2:  col.BeforeAddStage2,
		AfterAddStage2:   col.AfterAddStage2,
		BeforeAddStage3:  col.BeforeAddStage3,
		AfterAddStage3:   col.AfterAddStage3,
		// Ignoring depricated fields for now
	}
	return &rv, nil
}

// Implement some custom unmarshalling behavior
func (col *Column) UnmarshalXML(decoder *xml.Decoder, start xml.StartElement) error {
	type colAlias Column // prevents recursion while decoding, as type aliases have no methods
	// set defaults
	ca := colAlias{
		Nullable: true, // as in SQL NULL
	}
	err := decoder.DecodeElement(&ca, &start)
	if err != nil {
		return err
	}
	*col = Column(ca)
	return nil
}

func (col *Column) ToIR() (*ir.Column, error) {
	// skipping DEPRICATED fields
	rv := ir.Column{
		Name:             col.Name,
		Type:             col.Type,
		Nullable:         col.Nullable,
		Default:          col.Default,
		Description:      col.Description,
		Unique:           col.Unique,
		Check:            col.Check,
		OldColumnName:    col.OldColumnName,
		ConvertUsing:     col.ConvertUsing,
		ForeignSchema:    col.ForeignSchema,
		ForeignTable:     col.ForeignTable,
		ForeignColumn:    col.ForeignColumn,
		ForeignKeyName:   col.ForeignKeyName,
		ForeignIndexName: col.ForeignIndexName,
		BeforeAddStage1:  col.BeforeAddStage1,
		AfterAddStage1:   col.AfterAddStage1,
		BeforeAddStage2:  col.BeforeAddStage2,
		AfterAddStage2:   col.AfterAddStage2,
		BeforeAddStage3:  col.BeforeAddStage3,
		AfterAddStage3:   col.AfterAddStage3,
		SerialStart:      col.SerialStart,
		Statistics:       col.Statistics,
	}
	var err error
	rv.ForeignOnUpdate, err = ir.NewForeignKeyAction(col.ForeignOnUpdate)
	if err != nil {
		return nil, fmt.Errorf("column '%s' invalid: %w", col.Name, err)
	}
	rv.ForeignOnDelete, err = ir.NewForeignKeyAction(col.ForeignOnDelete)
	if err != nil {
		return nil, fmt.Errorf("column '%s' invalid: %w", col.Name, err)
	}
	return &rv, nil
}
