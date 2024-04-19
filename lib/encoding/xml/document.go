package xml

import (
	"encoding/xml"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/util"
	"github.com/pkg/errors"
)

type Document struct {
	XMLName        xml.Name          `xml:"dbsteward"`
	IncludeFiles   []*IncludeFile    `xml:"includeFile"`
	InlineAssembly []*InlineAssembly `xml:"inlineAssembly"`
	Database       *Database         `xml:"database"`
	Schemas        []*Schema         `xml:"schema"`
	Languages      []*Language       `xml:"language"`
	Sql            []*Sql            `xml:"sql"`
}

type IncludeFile struct {
	Name string `xml:"name,attr"`
}

type InlineAssembly struct {
	Name string `xml:"name,attr"`
}

type Sql struct {
	Author     string `xml:"author,attr"`
	Ticket     string `xml:"ticket,attr"`
	Version    string `xml:"version,attr"`
	Comment    string `xml:"comment,attr"`
	Stage      string `xml:"stage,attr"`
	SlonySetId *int   `xml:"slonySetId,attr"`
	Text       string `xml:",chardata"`
}

// ToIR converts this Document to a ir.Definition, if possible.
//
// Errors may arise if this operation cannot be completed for some reason.
// No semantic validation is performed at this point, as that's outside
// of the scope of an "xml" package - see `ir.Definition.Validate()`
func (self *Document) ToIR() (*ir.Definition, error) {
	includeFiles, err := util.MapErr(self.IncludeFiles, (*IncludeFile).ToIR)
	if err != nil {
		return nil, errors.Wrap(err, "could not process includeFile tags")
	}

	inlineAssembly, err := util.MapErr(self.InlineAssembly, (*InlineAssembly).ToIR)
	if err != nil {
		return nil, errors.Wrap(err, "could not process inlineAssembly tags")
	}

	database, err := self.Database.ToIR()
	if err != nil {
		return nil, errors.Wrap(err, "could not process database tag")
	}

	schemas, err := util.MapErr(self.Schemas, (*Schema).ToIR)
	if err != nil {
		return nil, errors.Wrap(err, "could not process schema tags")
	}

	languages, err := util.MapErr(self.Languages, (*Language).ToIR)
	if err != nil {
		return nil, errors.Wrap(err, "could not process language tags")
	}

	sql, err := util.MapErr(self.Sql, (*Sql).ToIR)
	if err != nil {
		return nil, errors.Wrap(err, "could not process sql tags")
	}

	return &ir.Definition{
		IncludeFiles:   includeFiles,
		InlineAssembly: inlineAssembly,
		Database:       database,
		Schemas:        schemas,
		Languages:      languages,
		Sql:            sql,
	}, nil
}

// FromModel builds a Document from a ir.Definition
//
// Errors may arise if this operation cannot be completed for some reason.
func (self *Document) FromModel(def *ir.Definition) error {
	// TODO
	return nil
}

// TODO should there be a ir.IncludeFile, or should we always overlay when converting to model?
func (self *IncludeFile) ToIR() (*ir.IncludeFile, error) {
	return &ir.IncludeFile{Name: self.Name}, nil
}

func (self *InlineAssembly) ToIR() (*ir.InlineAssembly, error) {
	return &ir.InlineAssembly{Name: self.Name}, nil
}

func (self *Sql) ToIR() (*ir.Sql, error) {
	panic("todo")
}
