package xml

import (
	"encoding/xml"
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

// Implement some custom unmarshalling behavior
func (self *Column) UnmarshalXML(decoder *xml.Decoder, start xml.StartElement) error {
	type colAlias Column // prevents recursion while decoding, as type aliases have no methods
	// set defaults
	col := &colAlias{
		Nullable: true, // as in SQL NULL
	}
	err := decoder.DecodeElement(col, &start)
	if err != nil {
		return err
	}
	*self = Column(*col)
	return nil
}
