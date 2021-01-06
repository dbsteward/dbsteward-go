package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

var GlobalView *View = NewView()

type View struct {
}

func NewView() *View {
	return &View{}
}

func (self *View) GetCreationSql(schema *model.Schema, view *model.View) []output.ToSql {
	// TODO(go,pgsql)
	return nil
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

	return ddl
}
