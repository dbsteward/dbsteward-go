package xml

import (
	"encoding/xml"

	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/util"
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
	// TODO
	return nil, nil
}

// FromModel builds a Document from a model.Definition
//
// Errors may arise if this operation cannot be completed for some reason.
func (self *Document) FromModel(def *model.Definition) error {
	// TODO
	return nil
}

func (self *Document) AddSchema(schema *Schema) {
	// TODO(feat) sanity check duplicate name
	self.Schemas = append(self.Schemas, schema)
}

func (self *Document) AddLanguage(lang *Language) {
	// TODO(feat) sanity check
	self.Languages = append(self.Languages, lang)
}

func (self *Document) AddCustomRole(role string) {
	if self.Database == nil {
		// TODO(go,nth) incomplete construction
		self.Database = &Database{}
	}
	self.Database.AddCustomRole(role)
}

func (self *Document) AddSql(sql *Sql) {
	// TODO(feat) sanity check
	self.Sql = append(self.Sql, sql)
}

// Merge "overlays" a document on top of this one.
func (base *Document) Merge(overlay *Document) {
	if overlay == nil {
		return
	}
	base.IncludeFiles = append(base.IncludeFiles, overlay.IncludeFiles...)
	base.InlineAssembly = append(base.InlineAssembly, overlay.InlineAssembly...)

	if base.Database == nil {
		base.Database = &Database{}
	}
	base.Database.Merge(overlay.Database)

	for _, overlaySchema := range overlay.Schemas {
		if baseSchema, ok := util.FindMatching(base.Schemas, overlaySchema).Maybe(); ok {
			baseSchema.Merge(overlaySchema)
		} else {
			// TODO(go,core) we should probably clone this. Should we just make AddSchema take a value not pointer?
			base.AddSchema(overlaySchema)
		}
	}

	for _, overlayLang := range overlay.Languages {
		if baseLang, ok := util.FindMatching(base.Languages, overlayLang).Maybe(); ok {
			baseLang.Merge(overlayLang)
		} else {
			base.AddLanguage(overlayLang)
		}
	}

	for _, overlaySql := range overlay.Sql {
		if baseSql, ok := util.FindMatching(base.Sql, overlaySql).Maybe(); ok {
			baseSql.Merge(overlaySql)
		} else {
			base.AddSql(overlaySql)
		}
	}
}

func (self *Sql) IdentityMatches(other *Sql) bool {
	if self == nil || other == nil {
		return false
	}
	// TODO(feat) make this more sophisticated
	return self.Text == other.Text
}

func (self *Sql) Merge(overlay *Sql) {
	if overlay == nil {
		return
	}
	self.Author = overlay.Author
	self.Ticket = overlay.Ticket
	self.Version = overlay.Version
	self.Comment = overlay.Comment
	self.Stage = overlay.Stage
	self.SlonySetId = overlay.SlonySetId
}
