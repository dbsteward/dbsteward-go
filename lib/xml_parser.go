package lib

import (
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"

	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/util"
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

func (self *XmlParser) SaveDoc(filename string, doc *model.Definition) {
	f, err := os.Create(filename)
	GlobalDBSteward.FatalIfError(err, "Could not open file %s for writing", filename)
	defer f.Close()

	// TODO(go,nth) get rid of empty closing tags like <grant ...></grant> => <grant .../>
	// Go doesn't natively support this (yet?), and google is being google about it
	// https://github.com/golang/go/issues/21399
	enc := xml.NewEncoder(f)
	enc.Indent("", "  ")
	err = enc.Encode(doc)
	GlobalDBSteward.FatalIfError(err, "Could not marshal document")
}

func (self *XmlParser) FormatXml(doc *model.Definition) string {
	d, err := xml.MarshalIndent(doc, "", "  ")
	GlobalDBSteward.FatalIfError(err, "could not marshal definition")
	return string(d)
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
		composite, err = self.CompositeDoc(composite, doc, file, startAddendumsIdx, addendumsDoc)
		GlobalDBSteward.FatalIfError(err, "Failed to composite xml file %s", file)
	}

	self.ValidateXml(self.FormatXml(composite))

	return composite, addendumsDoc
}

func (self *XmlParser) CompositeDoc(base, overlay *model.Definition, file string, startAddendumsIdx int, addendumsDoc *model.Definition) (*model.Definition, error) {
	util.Assert(overlay != nil, "CompositeDoc overlay must not be nil, you probably want CompositeDoc(nil, doc, ...) instead")

	if base == nil {
		base = &model.Definition{}
	}

	overlay, err := self.expandIncludes(overlay, file)
	if err != nil {
		return base, err
	}
	overlay = self.expandTabrowData(overlay)
	overlay, err = self.SqlFormatConvert(overlay)
	if err != nil {
		return base, err
	}

	// TODO(go,core) data addendums
	// TODO(go,slony) slony composite aspects

	base.Merge(overlay)
	self.VendorParse(base)

	// NOTE: v1 had schema validation occur _during_ the merge, which arguably is more efficient,
	// but also is a very different operation. We're going to try a separate validation step in v2+
	errs := base.Validate()
	if len(errs) > 0 {
		// TODO(go,nth) can we find a better way to represent validation errors? should we actually validate _outside_ this function?
		return base, &multierror.Error{
			Errors: errs,
		}
	}

	return base, nil
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

func (self *XmlParser) expandIncludes(doc *model.Definition, file string) (*model.Definition, error) {
	for _, includeFile := range doc.IncludeFiles {
		include := includeFile.Name
		// if the include is relative, make it relative to the parent file
		if !filepath.IsAbs(include) {
			inc, err := filepath.Abs(filepath.Join(filepath.Dir(file), include))
			if err != nil {
				return doc, fmt.Errorf("could not establish absolute path to file %s included from %s", include, file)
			}
			include = inc
		}
		includeDoc, err := self.LoadDefintion(include)
		if err != nil {
			return doc, fmt.Errorf("Failed to load and parse xml file %s included from %s", include, file)
		}

		doc, err = self.CompositeDoc(doc, includeDoc, include, -1, nil)
		if err != nil {
			return doc, errors.Wrapf(err, "while compositing included file %s from %s", include, file)
		}
	}
	doc.IncludeFiles = nil

	return doc, nil
}

func (self *XmlParser) XmlCompositePgData(doc *model.Definition, dataFiles []string) *model.Definition {
	// TODO(go,pgsql) pgdata compositing
	return nil
}

func (self *XmlParser) SqlFormatConvert(doc *model.Definition) (*model.Definition, error) {
	// legacy 1.0 column add directive attribute conversion
	for _, schema := range doc.Schemas {
		for _, table := range schema.Tables {
			for _, column := range table.Columns {
				column.ConvertStageDirectives()
			}
		}
	}

	// mssql10 sql format conversions
	// TODO(feat) apply mssql10_type_convert to function parameters/returns as well. see below mysql5 impl
	if GlobalDBSteward.SqlFormat == model.SqlFormatMssql10 {
		for _, schema := range doc.Schemas {
			// if objects are being placed in the public schema, move the schema definition to dbo
			// TODO(go,4) can we use a "SCHEMA_PUBLIC" macro or something to simplify this?
			if strings.EqualFold(schema.Name, "public") {
				if dbo := doc.TryGetSchemaNamed("dbo"); dbo != nil {
					return doc, fmt.Errorf("Attempting to rename schema 'public' to 'dbo' but schema 'dbo' already exists")
				}
				schema.Name = "dbo"
			}

			for _, table := range schema.Tables {
				for _, column := range table.Columns {
					if strings.EqualFold(column.ForeignSchema, "public") {
						column.ForeignSchema = "dbo"
					}

					if column.Type != "" {
						self.mssql10TypeConvert(column)
					}
				}
			}
			// TODO(go,mssql) do we need to check function types like we do for mysql?
		}
	}

	// mysql5 format conversions
	if GlobalDBSteward.SqlFormat == model.SqlFormatMysql5 {
		for _, schema := range doc.Schemas {
			for _, table := range schema.Tables {
				for _, column := range table.Columns {
					if column.Type != "" {
						typ, def := self.mysql5TypeConvert(column.Type, column.Default)
						column.Type = typ
						column.Default = def
					}
				}
			}

			for _, function := range schema.Functions {
				typ, _ := self.mysql5TypeConvert(function.Returns, "")
				function.Returns = typ
				for _, param := range function.Parameters {
					typ, _ := self.mysql5TypeConvert(param.Type, "")
					param.Type = typ
				}
			}
		}
	}

	return doc, nil
}

// TODO(go,3) push this to mssql package
// TODO(go,3) should we defer this to sql generation time instead?
func (self *XmlParser) mssql10TypeConvert(column *model.Column) {
	// all arrays to varchar(896) - our accepted varchar key max for mssql databases
	// the reason this is done to varchar(896) instead of varchar(MAX)
	// is that mssql will not allow binary blobs or long string types to be keys of indexes and foreign keys
	// attempting to do so results in errors like
	// Column 'app_mode' in table 'dbo.registration_step_list' is of a type that is invalid for use as a key column in an index.
	if strings.HasSuffix(column.Type, "[]") {
		column.Type = "varchar(896)"
	}

	switch strings.ToLower(column.Type) {
	case "boolean", "bool":
		column.Type = "char(1)"
		if column.Default != "" {
			switch strings.ToLower(column.Default) {
			case "t", "'t'", "true":
				column.Default = "'t'"
			case "f", "'f'", "false":
				column.Default = "'f'"
			default:
				GlobalDBSteward.Fatal("unknown column type bool default %s", column.Default)
			}
		}
	case "inet":
		column.Type = "varchar(16)"
	case "interval", "character varying", "varchar", "text":
		column.Type = "varchar(MAX)"
	case "timestamp", "timestamp without time zone":
		column.Type = "datetime2"
	case "timestamp with time zone":
		column.Type = "datetimeoffset(7)"
	case "time with time zone":
		column.Type = "time"
	case "serial":
		// pg serial = ms int identity
		// see http://msdn.microsoft.com/en-us/library/ms186775.aspx
		column.Type = "int identity(1, 1)"
		if column.SerialStart != nil {
			column.Type = fmt.Sprintf("int identity(%d, 1)", *column.SerialStart)
		}
	case "bigserial":
		column.Type = "bigint identity(1, 1)"
		if column.SerialStart != nil {
			column.Type = fmt.Sprintf("bigint identity(%d, 1)", *column.SerialStart)
		}
	case "uuid":
		// PostgreSQL's type uuid adhering to RFC 4122 -- see http://www.postgresql.org/docs/8.4/static/datatype-uuid.html
		// MSSQL almost equivalent known as uniqueidentifier -- see http://msdn.microsoft.com/en-us/library/ms187942.aspx
		// the column type is "a 16-byte GUID", 36 characters in length -- it does not claim to be, but appears to be their RFC 4122 implementation
		column.Type = "uniqueidentifier"
	default:
		// no match to postgresql built-in types
		// check for custom types in the public schema
		// these should be changed to dbo
		column.Type = util.IReplaceAll(column.Type, "public.", "dbo.")
	}

	// mssql doesn't understand epoch
	if strings.EqualFold(column.Default, "'epoch'") {
		column.Default = "'1970-01-01'"
	}
}

func (self *XmlParser) mysql5TypeConvert(typ, def string) (string, string) {
	// TODO(go,mysql)
	return typ, def
}

func (self *XmlParser) VendorParse(doc *model.Definition) {
	if parser := GlobalDBSteward.Lookup().XmlParser; parser != nil {
		parser.Process(doc)
	}
}

func (self *XmlParser) SlonyIdNumber(doc *model.Definition) *model.Definition {
	// TODO(go,slony)
	return nil
}

func (self *XmlParser) FileSort(file, sortedFile string) {
	// TODO(go,xmlutil)
}

func (self *XmlParser) ValidateXml(xmlstr string) {
	// TODO(go,core) validate the given xml against DTD. and/or, do we even need this now that we're serializing straight from structs?
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

	// NEW: if role matches any of the specific role assignments, don't consider it to be an error
	// this is basically the case where the user has manually resolved the role
	if strings.EqualFold(roles.Application, role) ||
		strings.EqualFold(roles.Owner, role) ||
		strings.EqualFold(roles.ReadOnly, role) ||
		strings.EqualFold(roles.Replication, role) ||
		util.IStrsContains(roles.CustomRoles, role) {
		return role
	}

	if !GlobalDBSteward.IgnoreCustomRoles {
		GlobalDBSteward.Fatal("Failed to confirm custom role: %s", role)
	}

	GlobalDBSteward.Warning("Ignoring custom roles, Role '%s' is being overridden by ROLE_OWNER (%s)", role, roles.Owner)
	return roles.Owner
}
