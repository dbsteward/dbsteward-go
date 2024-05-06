package pgsql8

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

func getCreateViewSql(l *slog.Logger, schema *ir.Schema, view *ir.View) ([]output.ToSql, error) {
	ref := sql.ViewRef{Schema: schema.Name, View: view.Name}
	query := view.TryGetViewQuery(ir.SqlFormatPgsql8)
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
		role, err := roleEnum(l, lib.GlobalDBSteward.NewDatabase, view.Owner)
		if err != nil {
			return nil, err
		}
		out = append(out, &sql.ViewAlterOwner{
			View: ref,
			Role: role,
		})
	}

	return out, nil
}

func getDropViewSql(schema *ir.Schema, view *ir.View) []output.ToSql {
	return []output.ToSql{
		&sql.ViewDrop{
			View: sql.ViewRef{Schema: schema.Name, View: view.Name},
		},
	}
}

func getViewGrantSql(l *slog.Logger, doc *ir.Definition, schema *ir.Schema, view *ir.View, grant *ir.Grant) ([]output.ToSql, error) {
	// NOTE: pgsql views use table grants!
	roles := make([]string, len(grant.Roles))
	var err error
	for i, role := range grant.Roles {
		roles[i], err = roleEnum(l, lib.GlobalDBSteward.NewDatabase, role)
		if err != nil {
			return nil, err
		}
	}

	perms := util.IIntersectStrs(grant.Permissions, ir.PermissionListAllPgsql8)
	if len(perms) == 0 {
		return nil, fmt.Errorf("no format-compatible permissions on view %s.%s grant: %v", schema.Name, view.Name, grant.Permissions)
	}
	invalidPerms := util.IDifferenceStrs(perms, ir.PermissionListValidView)
	if len(invalidPerms) > 0 {
		return nil, fmt.Errorf("invalid permissions on view %s.%s grant: %v", schema.Name, view.Name, invalidPerms)
	}

	ddl := []output.ToSql{
		&sql.TableGrant{
			Table:    sql.TableRef{Schema: schema.Name, Table: view.Name},
			Perms:    []string(grant.Permissions),
			Roles:    roles,
			CanGrant: grant.CanGrant(),
		},
	}

	// TODO(feat) implicit readonly grant like on tables?

	return ddl, nil
}

func getViewDependencies(doc *ir.Definition, schema *ir.Schema, view *ir.View) ([]ir.ViewRef, error) {
	out := []ir.ViewRef{}
	for _, viewName := range view.DependsOnViews {
		parts := strings.Split(viewName, ".")
		depSchema := schema
		var depViewName string

		if len(parts) == 2 {
			depSchema = doc.TryGetSchemaNamed(parts[0])
			if depSchema == nil {
				return nil, fmt.Errorf("could not find schema %s depended on by view %s.%s", parts[0], schema.Name, view.Name)
			}
			depViewName = parts[1]
		} else if len(parts) == 1 {
			depViewName = parts[0]
		} else {
			return nil, fmt.Errorf("could not parse view dependency '%s' of view %s.%s", viewName, schema.Name, view.Name)
		}

		depView := depSchema.TryGetViewNamed(depViewName)
		if depView == nil {
			return nil, fmt.Errorf("could not find view %s.%s depended on by view %s.%s", depSchema.Name, depViewName, schema.Name, view.Name)
		}

		out = append(out, ir.ViewRef{Schema: depSchema, View: depView})
	}
	return out, nil
}
