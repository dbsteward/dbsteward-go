package xml

import (
	"encoding/xml"
	"log/slog"

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

func IncludeFilesFromIR(l *slog.Logger, files []*ir.IncludeFile) ([]*IncludeFile, error) {
	if len(files) == 0 {
		return nil, nil
	}
	var rv []*IncludeFile
	for _, file := range files {
		if file != nil {
			rv = append(
				rv,
				&IncludeFile{
					Name: file.Name,
				},
			)
		}
	}
	return rv, nil
}

type InlineAssembly struct {
	Name string `xml:"name,attr"`
}

func InlineAssemblyFromIR(l *slog.Logger, recs []*ir.InlineAssembly) ([]*InlineAssembly, error) {
	if len(recs) == 0 {
		return nil, nil
	}
	var rv []*InlineAssembly
	for _, rec := range recs {
		if rec != nil {
			rv = append(
				rv,
				&InlineAssembly{
					Name: rec.Name,
				},
			)
		}
	}
	return rv, nil
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
func (doc *Document) ToIR() (*ir.Definition, error) {
	includeFiles, err := util.MapErr(doc.IncludeFiles, (*IncludeFile).ToIR)
	if err != nil {
		return nil, errors.Wrap(err, "could not process includeFile tags")
	}

	inlineAssembly, err := util.MapErr(doc.InlineAssembly, (*InlineAssembly).ToIR)
	if err != nil {
		return nil, errors.Wrap(err, "could not process inlineAssembly tags")
	}

	database, err := doc.Database.ToIR()
	if err != nil {
		return nil, errors.Wrap(err, "could not process database tag")
	}

	schemas, err := util.MapErr(doc.Schemas, (*Schema).ToIR)
	if err != nil {
		return nil, errors.Wrap(err, "could not process schema tags")
	}

	languages, err := util.MapErr(doc.Languages, (*Language).ToIR)
	if err != nil {
		return nil, errors.Wrap(err, "could not process language tags")
	}

	sql, err := util.MapErr(doc.Sql, (*Sql).ToIR)
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

// FromIR builds an XML Document from an ir.Definition
func FromIR(l *slog.Logger, def *ir.Definition) (*Document, error) {
	l = l.With(slog.String("operation", "translate IR to XML"))
	l.Debug("starting conversion")
	defer l.Debug("complted conversion")
	doc := Document{
		Database: &Database{
			SqlFormat:    string(def.Database.SqlFormat),
			Roles:        RoleAssignmentFromIR(l, def.Database.Roles),
			ConfigParams: ConfigParamsFromIR(l, def.Database.ConfigParams),
		},
	}
	var err error
	doc.Schemas, err = SchemasFromIR(l, def.Schemas)
	if err != nil {
		return nil, err
	}
	doc.IncludeFiles, err = IncludeFilesFromIR(l, def.IncludeFiles)
	if err != nil {
		return nil, err
	}
	doc.InlineAssembly, err = InlineAssemblyFromIR(l, def.InlineAssembly)
	if err != nil {
		return nil, err
	}
	// Languages
	// SQL
	return &doc, nil
}

// TODO should there be a ir.IncludeFile, or should we always overlay when converting to model?
func (doc *IncludeFile) ToIR() (*ir.IncludeFile, error) {
	return &ir.IncludeFile{Name: doc.Name}, nil
}

func (doc *InlineAssembly) ToIR() (*ir.InlineAssembly, error) {
	return &ir.InlineAssembly{Name: doc.Name}, nil
}

func (sql *Sql) ToIR() (*ir.Sql, error) {
	panic("todo")
}
