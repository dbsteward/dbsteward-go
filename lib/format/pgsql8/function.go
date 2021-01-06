package pgsql8

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

var GlobalFunction *Function = NewFunction()

type Function struct {
	IncludeColumnDefaultNextvalInCreateSql bool
}

func NewFunction() *Function {
	return &Function{}
}

func (self *Function) GetCreationSql(schema *model.Schema, function *model.Function) []output.ToSql {
	// TODO(go,pgsql)
	return nil
}

func (self *Function) GetGrantSql(doc *model.Definition, schema *model.Schema, fn *model.Function, grant *model.Grant) []output.ToSql {
	GlobalOperations.SetContextReplicaSetId(fn.SlonySetId)

	roles := make([]string, len(grant.Roles))
	for i, role := range grant.Roles {
		roles[i] = lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, role)
	}

	perms := util.IIntersectStrs(grant.Permissions, model.PermissionListAllPgsql8)
	if len(perms) == 0 {
		lib.GlobalDBSteward.Fatal("No format-compatible permissions on function %s.%s(%s) grant: %v", schema.Name, fn.Name, grant.Permissions, strings.Join(fn.ParamTypes(), ","))
	}
	invalidPerms := util.IDifferenceStrs(perms, model.PermissionListValidFunction)
	if len(invalidPerms) > 0 {
		lib.GlobalDBSteward.Fatal("Invalid permissions on sequence grant: %v", invalidPerms)
	}

	ddl := []output.ToSql{
		&sql.FunctionGrant{
			Function: sql.FunctionRef{schema.Name, fn.Name, fn.ParamTypes()},
			Perms:    []string(grant.Permissions),
			Roles:    roles,
			CanGrant: grant.CanGrant(),
		},
	}

	// TODO(feat) should there be an implicit read-only grant?

	return ddl
}
