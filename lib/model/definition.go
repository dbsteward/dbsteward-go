package model

import (
	"github.com/dbsteward/dbsteward/lib/util"
	"github.com/pkg/errors"
)

// TODO(go,3) move most public fields to private, use accessors to better enable encapsulation, validation; "make invalid states unrepresentable"

type Definition struct {
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
	Author     string   `xml:"author,attr"`
	Ticket     string   `xml:"ticket,attr"`
	Version    string   `xml:"version,attr"`
	Comment    string   `xml:"comment,attr"`
	Stage      SqlStage `xml:"stage,attr"`
	SlonySetId int      `xml:"slonySetId,attr"`
	Text       string   `xml:",chardata"`
}

func (self *Definition) GetSchemaNamed(name string) (*Schema, error) {
	matching := []*Schema{}
	for _, schema := range self.Schemas {
		// TODO(feat) case insensitivity?
		if schema.Name == name {
			matching = append(matching, schema)
		}
	}
	if len(matching) == 0 {
		return nil, errors.Errorf("no schema named %s found", name)
	}
	if len(matching) > 1 {
		return nil, errors.Errorf("more than one schema named %s found", name)
	}
	return matching[0], nil
}

func (self *Definition) TryGetSchemaNamed(name string) *Schema {
	for _, schema := range self.Schemas {
		// TODO(feat) case insensitivity?
		if schema.Name == name {
			return schema
		}
	}
	return nil
}

func (self *Definition) GetOrCreateSchemaNamed(name string) *Schema {
	schema := self.TryGetSchemaNamed(name)
	if schema == nil {
		schema = &Schema{Name: name}
		self.AddSchema(schema)
	}
	return schema
}

func (self *Definition) AddSchema(schema *Schema) {
	// TODO(feat) sanity check duplicate name
	self.Schemas = append(self.Schemas, schema)
}

func (self *Definition) TryGetLanguageNamed(name string) *Language {
	for _, lang := range self.Languages {
		// TODO(feat) case insensitivity
		if lang.Name == name {
			return lang
		}
	}
	return nil
}
func (self *Definition) AddLanguage(lang *Language) {
	// TODO(feat) sanity check
	self.Languages = append(self.Languages, lang)
}

func (self *Definition) IsRoleDefined(role string) bool {
	if util.IIndexOfStr(role, MACRO_ROLES) >= 0 {
		return true
	}
	if self.Database == nil {
		return false
	}
	return self.Database.IsRoleDefined(role)
}

func (self *Definition) AddCustomRole(role string) {
	if self.Database == nil {
		// TODO(go,nth) incomplete construction
		self.Database = &Database{}
	}
	self.Database.AddCustomRole(role)
}

func (self *Definition) TryGetSqlMatching(target *Sql) *Sql {
	for _, sql := range self.Sql {
		if sql.IdentityMatches(target) {
			return sql
		}
	}
	return nil
}

func (self *Definition) AddSql(sql *Sql) {
	// TODO(feat) sanity check
	self.Sql = append(self.Sql, sql)
}

// Merge is the new implementation of xml_parser::xml_composite_children
// it merges items from the overlay into this definition
func (self *Definition) Merge(overlay *Definition) {
	if overlay == nil {
		return
	}
	self.IncludeFiles = append(self.IncludeFiles, overlay.IncludeFiles...)
	self.InlineAssembly = append(self.InlineAssembly, overlay.InlineAssembly...)

	if self.Database == nil {
		self.Database = &Database{}
	}
	self.Database.Merge(overlay.Database)

	for _, overlaySchema := range overlay.Schemas {
		if baseSchema := self.TryGetSchemaNamed(overlaySchema.Name); baseSchema != nil {
			baseSchema.Merge(overlaySchema)
		} else {
			// TODO(go,core) we should probably clone this. Should we just make AddSchema take a value not pointer?
			self.AddSchema(overlaySchema)
		}
	}

	for _, overlayLang := range overlay.Languages {
		if baseLang := self.TryGetLanguageNamed(overlayLang.Name); baseLang != nil {
			baseLang.Merge(overlayLang)
		} else {
			self.AddLanguage(overlayLang)
		}
	}

	for _, overlaySql := range overlay.Sql {
		if baseSql := self.TryGetSqlMatching(overlaySql); baseSql != nil {
			baseSql.Merge(overlaySql)
		} else {
			self.AddSql(overlaySql)
		}
	}
}

func (self *Sql) IdentityMatches(other *Sql) bool {
	if other == nil {
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
