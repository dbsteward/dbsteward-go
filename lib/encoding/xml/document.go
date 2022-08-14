package xml

import (
	"encoding/xml"

	"github.com/dbsteward/dbsteward/lib/model"
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

// ToModel converts this Document to a model.Definition, if possible.
//
// Errors may arise if this operation cannot be completed for some reason.
// No semantic validation is performed at this point, as that's outside
// of the scope of an "xml" package - see `model.Definition.Validate()`
func (self *Document) ToModel() (*model.Definition, error) {
	includeFiles, err := util.MapErr(self.IncludeFiles, (*IncludeFile).ToModel)
	if err != nil {
		return nil, errors.Wrap(err, "could not process includeFile tags")
	}

	inlineAssembly, err := util.MapErr(self.InlineAssembly, (*InlineAssembly).ToModel)
	if err != nil {
		return nil, errors.Wrap(err, "could not process inlineAssembly tags")
	}

	database, err := self.Database.ToModel()
	if err != nil {
		return nil, errors.Wrap(err, "could not process database tag")
	}

	schemas, err := util.MapErr(self.Schemas, (*Schema).ToModel)
	if err != nil {
		return nil, errors.Wrap(err, "could not process schema tags")
	}

	languages, err := util.MapErr(self.Languages, (*Language).ToModel)
	if err != nil {
		return nil, errors.Wrap(err, "could not process language tags")
	}

	sql, err := util.MapErr(self.Sql, (*Sql).ToModel)
	if err != nil {
		return nil, errors.Wrap(err, "could not process sql tags")
	}

	return &model.Definition{
		IncludeFiles:   includeFiles,
		InlineAssembly: inlineAssembly,
		Database:       database,
		Schemas:        schemas,
		Languages:      languages,
		Sql:            sql,
	}, nil
}

// FromModel builds a Document from a model.Definition
//
// Errors may arise if this operation cannot be completed for some reason.
func (self *Document) FromModel(def *model.Definition) error {
	// TODO
	return nil
}

// TODO should there be a model.IncludeFile, or should we always overlay when converting to model?
func (self *IncludeFile) ToModel() (*model.IncludeFile, error) {
	return &model.IncludeFile{Name: self.Name}, nil
}

func (self *InlineAssembly) ToModel() (*model.InlineAssembly, error) {
	return &model.InlineAssembly{Name: self.Name}, nil
}

func (self *Sql) ToModel() (*model.Sql, error) {
	panic("todo")
}
