package lib

import (
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"strings"

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

	return composite, addendumsDoc
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
	for _, includeFile := range doc.IncludeFiles {
		include := includeFile.Name
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
					GlobalDBSteward.Fatal("Attempting to rename schema 'public' to 'dbo' but schema 'dbo' already exists")
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

	return doc
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
		if column.SerialStart != "" {
			column.Type = fmt.Sprintf("int identity(%s, 1)", column.SerialStart)
		}
	case "bigserial":
		column.Type = "bigint identity(1, 1)"
		if column.SerialStart != "" {
			column.Type = fmt.Sprintf("bigint identity(%s, 1)", column.SerialStart)
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

func (self *XmlParser) TableDependencyOrder(doc *model.Definition) []*model.TableRef {
	// first, build forward and reverse adjacency lists
	// forwards: a mapping of local table => foreign tables that it references
	// reverse: a mapping of foreign table => local tables that reference it
	forward := map[model.TableRef][]model.TableRef{}
	reverse := map[model.TableRef][]model.TableRef{}
	for _, schema := range doc.Schemas {
		for _, table := range schema.Tables {
			curr := model.TableRef{schema, table}
			// initialize them so we know the node is there, even if it has no dependencies
			if len(forward[curr]) == 0 {
				forward[curr] = []model.TableRef{}
			}
			if len(reverse[curr]) == 0 {
				reverse[curr] = []model.TableRef{}
			}

			for _, dep := range self.getTableDependencies(doc, schema, table) {
				forward[curr] = append(forward[curr], dep)
				reverse[dep] = append(reverse[dep], curr)
			}
		}
	}

	/*
		our goal is to produce a list of tables in an order such that creating the tables in order
		does not depend on any uncreated tables. we also need to fail out when a cycle is detected

		e.g. with a table graph like a -> b -> c
		                             d -<  >-> g   (d depends on both b and f, both b and f depend on g)
		                             e -> f
		then we might return: c, g, b, f, a, d, e
		                  or: g, c, b, a, f, e, d

		in this example, `forward` will contain what each table "points to"
		  a => b
		  b => c, g
		  c =>
		  d => b, f
		  e => f
		  f => g
		  g =>
		and `reverse` will contain what "points at" each table
		  a =>
		  b => a, d
		  c => b
		  d =>
		  e =>
		  f => d, e
		  g => b, f

		we know we can safely create any table that doesn't have any dependencies (which has no entries in `forward`)
		so, we add those to the list (c and g in this case), and remove it from the graph,
		using `reverse` to efficiently inform us which keys in `forward` to remove it from

		after one iteration we're left with `forward`:
		  a => b
		  b =>
		  d => b, f
		  e => f
		  f =>
		and `reverse`:
		  a =>
		  b => a, d
		  d =>
		  e =>
		  f => d, e

		now just rinse and repeat until there are no more tables in the adjacency lists.

		if at any point there are no entries in `forward` with len = 0, there is a cycle
	*/

	// a quick helper to cut down on complexity below, see https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	// HACK: this is, IMHO, a really bullshit and footgunny method to do this efficiently
	remove := func(target model.TableRef, slice []model.TableRef) []model.TableRef {
		b := slice[:0]
		for _, x := range slice {
			if x != target {
				b = append(b, x)
			}
		}
		// garbage collect
		for i := len(b); i < len(slice); i++ {
			slice[i] = model.TableRef{}
		}
		return b
	}

	out := []*model.TableRef{}
	i := 0
	for len(forward) > 0 {
		// fmt.Printf("%d ----\n", i)
		// fmt.Printf("forward:\n", name)
		// for key, vals := range forward {
		// 	fmt.Printf("  %s => %v\n", key, vals)
		// }
		// fmt.Printf("reverse:\n", name)
		// for key, vals := range reverse {
		// 	fmt.Printf("  %s => %v\n", key, vals)
		// }
		i += 1
		atLeastOne := false
		for local, foreigns := range forward {
			if len(foreigns) == 0 {
				// fmt.Printf("%s has no foreigns\n", local)
				// GOTCHA: go reuses the same memory for loop iteration variables,
				// so we need to copy it before we make a pointer to it
				clone := local
				out = append(out, &clone)
				atLeastOne = true

				// remove it from the graph now
				delete(forward, local)
				for _, dependent := range reverse[local] {
					forward[dependent] = remove(local, forward[dependent])
				}
				delete(reverse, local)
			}
		}
		if !atLeastOne {
			// TODO(go,core) add diagnostics about what the cycle is
			GlobalDBSteward.Fatal("Dependency cycle detected!")
		}
		// fmt.Printf("current order: %v\n", out)
	}
	return out
}

func (self *XmlParser) getTableDependencies(doc *model.Definition, schema *model.Schema, table *model.Table) []model.TableRef {
	out := []model.TableRef{}
	// gather foreign keys on the columns
	for _, column := range table.Columns {
		if column.ForeignTable != "" {
			fSchema, fTable := GlobalDBX.ResolveSchemaTable(doc, schema, column.ForeignSchema, column.ForeignTable, "column foreignKey")
			out = append(out, model.TableRef{fSchema, fTable})
		}
	}

	// gather explicit foreign keys
	for _, fk := range table.ForeignKeys {
		fSchema, fTable := GlobalDBX.ResolveSchemaTable(doc, schema, fk.ForeignSchema, fk.ForeignTable, "foreignKey element")
		out = append(out, model.TableRef{fSchema, fTable})
	}

	// gather constraints
	for _, constraint := range table.Constraints {
		if constraint.ForeignTable != "" {
			fSchema, fTable := GlobalDBX.ResolveSchemaTable(doc, schema, constraint.ForeignSchema, constraint.ForeignTable, "FOREIGN KEY constraint")
			out = append(out, model.TableRef{fSchema, fTable})
		}
	}

	// TODO(feat) examine <constraint type="FOREIGN KEY">
	// TODO(feat) any other dependencies from a table? sequences? inheritance?
	// TODO(feat) can we piggyback on Constraint.GetTableConstraints?
	return out
}

func (self *XmlParser) InheritanceGetColumn(doc *model.Definition, schema *model.Schema, table *model.Table, columnName string) *model.Column {
	// TODO(go,3) move to model
	column := table.TryGetColumnNamed(columnName)

	// just keep walking up the inheritance chain so long as there's a link
	for column == nil && table.InheritsTable != "" {
		schema, table = GlobalDBX.ResolveSchemaTable(doc, schema, table.InheritsSchema, table.InheritsTable, "inheritance")
		column = table.TryGetColumnNamed(columnName)
	}

	return column
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
