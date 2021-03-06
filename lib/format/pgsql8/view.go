package pgsql8

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

type View struct {
}

func NewView() *View {
	return &View{}
}

func (self *View) GetCreationSql(schema *model.Schema, view *model.View) []output.ToSql {
	if view.SlonySetId == nil {
		GlobalOperations.SetContextReplicaSetId(schema.SlonySetId)
	} else {
		GlobalOperations.SetContextReplicaSetId(view.SlonySetId)
	}

	ref := sql.ViewRef{schema.Name, view.Name}
	query := view.TryGetViewQuery(model.SqlFormatPgsql8)
	util.Assert(query != nil, "Calling View.GetCreationSql for a view not defined for this sqlformat")

	out := []output.ToSql{
		&sql.ViewCreate{
			View:  ref,
			Query: query.GetNormalizedText(),
		},
	}

	if view.Description != "" {
		out = append(out, &sql.ViewSetComment{
			View:    ref,
			Comment: view.Description,
		})
	}
	if view.Owner != "" {
		out = append(out, &sql.ViewAlterOwner{
			View: ref,
			Role: lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, view.Owner),
		})
	}

	return out
}

func (self *View) GetDropSql(schema *model.Schema, view *model.View) []output.ToSql {
	if view.SlonySetId == nil {
		GlobalOperations.SetContextReplicaSetId(schema.SlonySetId)
	} else {
		GlobalOperations.SetContextReplicaSetId(view.SlonySetId)
	}
	return []output.ToSql{
		&sql.ViewDrop{
			View: sql.ViewRef{schema.Name, view.Name},
		},
	}
}

func (self *View) GetGrantSql(doc *model.Definition, schema *model.Schema, view *model.View, grant *model.Grant) []output.ToSql {
	// NOTE: pgsql views use table grants!
	GlobalOperations.SetContextReplicaSetId(view.SlonySetId)

	roles := make([]string, len(grant.Roles))
	for i, role := range grant.Roles {
		roles[i] = lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, role)
	}

	perms := util.IIntersectStrs(grant.Permissions, model.PermissionListAllPgsql8)
	if len(perms) == 0 {
		lib.GlobalDBSteward.Fatal("No format-compatible permissions on view %s.%s grant: %v", schema.Name, view.Name, grant.Permissions)
	}
	invalidPerms := util.IDifferenceStrs(perms, model.PermissionListValidView)
	if len(invalidPerms) > 0 {
		lib.GlobalDBSteward.Fatal("Invalid permissions on view %s.%s grant: %v", schema.Name, view.Name, invalidPerms)
	}

	ddl := []output.ToSql{
		&sql.TableGrant{
			Table:    sql.TableRef{schema.Name, view.Name},
			Perms:    []string(grant.Permissions),
			Roles:    roles,
			CanGrant: grant.CanGrant(),
		},
	}

	// TODO(feat) implicit readonly grant like on tables?

	return ddl
}

func (self *View) GetDependencies(doc *model.Definition, schema *model.Schema, view *model.View) []model.ViewRef {
	out := []model.ViewRef{}
	for _, viewName := range view.DependsOnViews {
		parts := strings.Split(viewName, ".")
		depSchema := schema
		var depViewName string

		if len(parts) == 2 {
			depSchema = doc.TryGetSchemaNamed(parts[0])
			if depSchema == nil {
				lib.GlobalDBSteward.Fatal("Could not find schema %s depended on by view %s.%s", parts[0], schema.Name, view.Name)
			}
			depViewName = parts[1]
		} else if len(parts) == 1 {
			depViewName = parts[0]
		} else {
			lib.GlobalDBSteward.Fatal("Could not parse view dependency '%s' of view %s.%s", viewName, schema.Name, view.Name)
		}

		depView := depSchema.TryGetViewNamed(depViewName)
		if depView == nil {
			lib.GlobalDBSteward.Fatal("Could not find view %s.%s depended on by view %s.%s", depSchema.Name, depViewName, schema.Name, view.Name)
		}

		out = append(out, model.ViewRef{depSchema, depView})
	}
	return out
}
