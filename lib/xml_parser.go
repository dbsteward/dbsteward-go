package lib

import (
	"encoding/xml"
	"os"

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
	// TODO(go,core)
	return nil
}

func (self *XmlParser) XmlCompositeAddendums(files []string, addendums uint) (*model.Definition, *model.Definition) {
	// TODO(go,core)
	return nil, nil
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
	// TODO(go,xmlutil)
	return ""
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
