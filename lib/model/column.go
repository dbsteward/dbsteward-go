package model

import (
	"encoding/xml"

	"github.com/dbsteward/dbsteward/lib/util"
)

type Column struct {
	Name             string           `xml:"name,attr"`
	Type             string           `xml:"type,attr"`
	Nullable         bool             `xml:"null,attr"`
	Default          string           `xml:"default,attr"`
	Description      string           `xml:"description,attr"`
	Unique           bool             `xml:"unique,attr"`
	Check            string           `xml:"check,attr"`
	SerialStart      *int             `xml:"serialStart,attr"`
	ForeignSchema    string           `xml:"foreignSchema,attr"`
	ForeignTable     string           `xml:"foreignTable,attr"`
	ForeignColumn    string           `xml:"foreignColumn,attr"`
	ForeignKeyName   string           `xml:"foreignKeyName,attr"`
	ForeignIndexName string           `xml:"foreignIndexName,attr"`
	ForeignOnUpdate  ForeignKeyAction `xml:"foreignOnUpdate,attr"`
	ForeignOnDelete  ForeignKeyAction `xml:"foreignOnDelete,attr"`
	Statistics       *int             `xml:"statistics,attr"` // TODO(feat) this doesn't show up in the DTD
	BeforeAddStage1  string           `xml:"beforeAddStage1,attr"`
	AfterAddStage1   string           `xml:"afterAddStage1,attr"`
	BeforeAddStage2  string           `xml:"beforeAddStage2,attr"`
	AfterAddStage2   string           `xml:"afterAddStage2,attr"`
	BeforeAddStage3  string           `xml:"beforeAddStage3,attr"`
	AfterAddStage3   string           `xml:"afterAddStage3,attr"`

	// These are DEPRECATED, replaced by Before/AfterAddStageN. see ConvertStageDirectives
	AfterAddPreStage1  string `xml:"afterAddPreStage1,attr"`
	AfterAddPostStage1 string `xml:"afterAddPostStage1,attr"`
	AfterAddPreStage2  string `xml:"afterAddPreStage2,attr"`
	AfterAddPostStage2 string `xml:"afterAddPostStage2,attr"`
	AfterAddPreStage3  string `xml:"afterAddPreStage3,attr"`
	AfterAddPostStage3 string `xml:"afterAddPostStage3,attr"`
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

func (self *Column) ConvertStageDirectives() {
	self.BeforeAddStage1 = util.CoalesceStr(self.BeforeAddStage1, self.AfterAddPreStage1)
	self.AfterAddStage1 = util.CoalesceStr(self.AfterAddStage1, self.AfterAddPostStage1)
	self.BeforeAddStage2 = util.CoalesceStr(self.BeforeAddStage2, self.AfterAddPreStage2)
	self.AfterAddStage2 = util.CoalesceStr(self.AfterAddStage2, self.AfterAddPostStage2)
	self.BeforeAddStage3 = util.CoalesceStr(self.BeforeAddStage3, self.AfterAddPreStage3)
	self.AfterAddStage3 = util.CoalesceStr(self.AfterAddStage3, self.AfterAddPostStage3)

	self.AfterAddPreStage1 = ""
	self.AfterAddPostStage1 = ""
	self.AfterAddPreStage2 = ""
	self.AfterAddPostStage2 = ""
	self.AfterAddPreStage3 = ""
	self.AfterAddPostStage3 = ""
}

func (self *Column) HasForeignKey() bool {
	return self.ForeignTable != ""
}

func (self *Column) TryGetReferencedKey() *KeyNames {
	if !self.HasForeignKey() {
		return nil
	}

	key := self.GetReferencedKey()
	return &key
}

func (self *Column) GetReferencedKey() KeyNames {
	util.Assert(self.HasForeignKey(), "GetReferencedKey without checking HasForeignKey")
	return KeyNames{
		Schema:  self.ForeignSchema,
		Table:   self.ForeignTable,
		Columns: []string{self.ForeignColumn},
		KeyName: self.ForeignKeyName,
	}
}

func (self *Column) Merge(overlay *Column) {
	self.Type = overlay.Type
	self.Nullable = overlay.Nullable
	self.Default = overlay.Default
	self.Description = overlay.Description
	self.SerialStart = overlay.SerialStart
	self.ForeignSchema = overlay.ForeignSchema
	self.ForeignTable = overlay.ForeignTable
	self.ForeignKeyName = overlay.ForeignKeyName
	self.ForeignOnUpdate = overlay.ForeignOnUpdate
	self.ForeignOnDelete = overlay.ForeignOnDelete
	self.Statistics = overlay.Statistics
}
