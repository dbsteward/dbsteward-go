package lib

import (
	"encoding/xml"
	"os"
	"path/filepath"

	"github.com/dbsteward/dbsteward/lib/util"

	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/pkg/errors"
)

// TODO(go,3) no globals
var GlobalXmlParser *XmlParser = NewXmlParser()

type XmlParser struct{}

func NewXmlParser() *XmlParser {
	return &XmlParser{}
}

func (self *XmlParser) LoadDefintion(file string) (*model.Definition, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, errors.Wrapf(err, "could not read dbxml file %s", file)
	}
	defer f.Close()

	doc := &model.Definition{}
	err = xml.NewDecoder(f).Decode(doc)
	if err != nil {
		return nil, errors.Wrapf(err, "could not parse dbxml file %s", file)
	}
	return doc, nil
}

func (self *XmlParser) GetSqlFormat(files []string) model.SqlFormat {
	// TODO(go,core)
	return model.SqlFormatPgsql8
}

func (self *XmlParser) XmlComposite(files []string) *model.Definition {
	doc, _ := self.XmlCompositeAddendums(files, 0)
	return doc
}

func (self *XmlParser) XmlCompositeAddendums(files []string, addendums uint) (*model.Definition, *model.Definition) {
	var composite, addendumsDoc *model.Definition
	startAddendumsIdx := -1

	if addendums > 0 {
		addendumsDoc = &model.Definition{}
		startAddendumsIdx = len(files) - int(addendums)
	}

	for _, file := range files {
		GlobalDBSteward.Notice("Loading XML %s...", file)

		doc, err := self.LoadDefintion(file)
		GlobalDBSteward.FatalIfError(err, "Failed to load and parse xml file %s", file)
		GlobalDBSteward.Notice("Compositing XML %s", file)
		composite = self.CompositeDoc(composite, doc, file, startAddendumsIdx, addendumsDoc)
	}

	self.ValidateXml(self.FormatXml(composite))

	return nil, nil
}

func (self *XmlParser) CompositeDoc(base, overlay *model.Definition, file string, startAddendumsIdx int, addendumsDoc *model.Definition) *model.Definition {
	if base == nil {
		base = &model.Definition{}
	}

	overlay = self.expandIncludes(overlay, file)
	overlay = self.expandTabrowData(overlay)
	overlay = self.SqlFormatConvert(overlay)

	// TODO(go,core) data addendums
	// TODO(go,slony) slony composite aspects

	base.Merge(overlay)
	return base
}

func (self *XmlParser) expandTabrowData(doc *model.Definition) *model.Definition {
	for _, schema := range doc.Schemas {
		for _, table := range schema.Tables {
			if table.Rows != nil {
				table.Rows.ConvertTabRows()
			}
		}
	}
	return doc
}

func (self *XmlParser) expandIncludes(doc *model.Definition, file string) *model.Definition {
	for _, include := range doc.IncludeFiles {
		// if the include is relative, make it relative to the parent file
		if !filepath.IsAbs(include) {
			inc, err := filepath.Abs(filepath.Join(filepath.Dir(file), include))
			GlobalDBSteward.FatalIfError(err, "could not establish absolute path to file %s included from %s", include, file)
			include = inc
		}
		includeDoc, err := self.LoadDefintion(include)
		GlobalDBSteward.FatalIfError(err, "Failed to load and parse xml file %s included from %s", include, file)

		doc = self.CompositeDoc(doc, includeDoc, include, -1, nil)
	}
	doc.IncludeFiles = nil

	return doc
}

func (self *XmlParser) XmlCompositePgData(doc *model.Definition, dataFiles []string) *model.Definition {
	// TODO(go,core)
	return nil
}

func (self *XmlParser) SqlFormatConvert(doc *model.Definition) *model.Definition {
	// TODO(go,core)
	return nil
}

func (self *XmlParser) VendorParse(doc *model.Definition) {
	// TODO(go,core)
}

func (self *XmlParser) SaveDoc(filename string, doc *model.Definition) {
	// TODO(go,core)
}

func (self *XmlParser) SlonyIdNumber(doc *model.Definition) *model.Definition {
	// TODO(go,slony)
	return nil
}

func (self *XmlParser) FileSort(file, sortedFile string) {
	// TODO(go,xmlutil)
}

func (self *XmlParser) FormatXml(doc *model.Definition) string {
	d, err := xml.MarshalIndent(doc, "", "    ")
	GlobalDBSteward.FatalIfError(err, "could not marshal definition")
	return string(d)
}

func (self *XmlParser) ValidateXml(xmlstr string) {
	// TODO(go,core) validate the given xml against DTD. and/or, do we even need this now that we're serializing straight from structs?
}

func (self *XmlParser) TableDependencyOrder(doc *model.Definition) []*model.TableDepEntry {
	// TODO(go,core)
	return nil
}

func (self *XmlParser) InheritanceGetColumn(table *model.Table, columnName string) []*model.Column {
	// TODO(go,nth) definitely seems like this should return 0 or 1 columns, not a list, right?
	// TODO(go,nth) this should probably go directly on the table
	// TODO(go,core)
	return nil
}

func (self *XmlParser) RoleEnum(doc *model.Definition, role string) string {
	if role == model.RolePublic && GlobalDBSteward.SqlFormat == model.SqlFormatMysql5 {
		role = model.RoleApplication
		GlobalDBSteward.Warning("MySQL doesn't support the PUBLIC role, using ROLE_APPLICATION (%s) instead", role)
	}

	if doc.Database == nil {
		// TODO(go,nth) somehow was incompletely constructed
		doc.Database = &model.Database{
			Roles: &model.RoleAssignment{},
		}
	}
	roles := doc.Database.Roles

	switch role {
	case model.RolePublic, model.RolePgsql:
		// RolePublic, RolePgsql are their own constants
		return role
	case model.RoleApplication:
		return roles.Application
	case model.RoleOwner:
		return roles.Owner
	case model.RoleReadOnly:
		return roles.ReadOnly
	case model.RoleReplication:
		return roles.Replication
	}

	if util.IIndexOfStr(role, roles.CustomRoles) >= 0 {
		return role
	}

	if !GlobalDBSteward.IgnoreCustomRoles {
		GlobalDBSteward.Fatal("Failed to confirm custom role: %s", role)
	}

	GlobalDBSteward.Warning("Ignoring custom roles, Role '%s' is being overridden by ROLE_OWNER (%s)", role, roles.Owner)
	return roles.Owner
}
