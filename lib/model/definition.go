package model

import (
	"encoding/xml"
	"fmt"

	"github.com/pkg/errors"

	"github.com/dbsteward/dbsteward/lib/util"
)

// TODO(go,3) move most public fields to private, use accessors to better enable encapsulation, validation; "make invalid states unrepresentable"

type Definition struct {
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
	Author     string   `xml:"author,attr"`
	Ticket     string   `xml:"ticket,attr"`
	Version    string   `xml:"version,attr"`
	Comment    string   `xml:"comment,attr"`
	Stage      SqlStage `xml:"stage,attr"`
	SlonySetId *int     `xml:"slonySetId,attr"`
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
	if self == nil {
		return nil
	}
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
	if self == nil {
		return nil
	}
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
	if self == nil {
		return nil
	}
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

// Validate is the new implementation of the various validation operations
// that occur throughout the codebase. It detects issues with the database
// schema that a user will need to address. (This is NOT to detect an invalidly
// constructed Definition object / programming errors in dbsteward itself)
// This is initially intended to replace only the validations that occur in
// xml_parser::xml_composite_children()
// TODO(go,3) can we replace this with a more generic visitor pattern?
// TODO(go,3) should there be warnings?
func (self *Definition) Validate() []error {
	out := []error{}

	// no two objects should have the same identity (also, validate sub-objects)
	for i, schema := range self.Schemas {
		out = append(out, schema.Validate(self)...)
		for _, other := range self.Schemas[i+1:] {
			if schema.IdentityMatches(other) {
				out = append(out, fmt.Errorf("found two schemas with name %q", schema.Name))
			}
		}
	}

	for i, sql := range self.Sql {
		out = append(out, sql.Validate(self)...)
		for _, other := range self.Sql[i+1:] {
			if sql.IdentityMatches(other) {
				// TODO(go,nth) better identifier for sql?
				out = append(out, fmt.Errorf("found two sql elements with text %q", sql.Text))
			}
		}
	}

	return out
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

func (self *Sql) Validate(*Definition) []error {
	return nil
}
