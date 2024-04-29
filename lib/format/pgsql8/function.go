package pgsql8

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

func functionDefinitionReferencesTable(definition *ir.FunctionDefinition) *lib.QualifiedTable {
	// TODO(feat) a function could reference many tables, but this only returns the first; make it understand many tables
	// TODO(feat) this won't detect quoted table names
	// TODO(go,pgsql) test this
	// TODO(go,3) upgrade this to properly parse the sql, check transitive deps (e.g. views, other functions)
	validTableName := `[\w\.]+`
	table := ""
	if matches := util.IMatch(fmt.Sprintf(`SELECT\s+.+\s+FROM\s+(%s)`, validTableName), definition.Text); matches != nil {
		table = matches[1]
	} else if matches := util.IMatch(fmt.Sprintf(`INSERT\s+INTO\s+(%s)`, validTableName), definition.Text); matches != nil {
		table = matches[1]
	} else if matches := util.IMatch(fmt.Sprintf(`DELETE\s+FROM\s+(?:ONLY)?\s*(%s)`, validTableName), definition.Text); matches != nil {
		table = matches[1]
	} else if matches := util.IMatch(fmt.Sprintf(`UPDATE\s+(?:ONLY)?\s*(%s)`, validTableName), definition.Text); matches != nil {
		table = matches[1]
	}
	if table == "" {
		return nil
	}
	parsed := lib.GlobalSqlParser.ParseQualifiedTableName(table)
	return &parsed
}

func getFunctionCreationSql(schema *ir.Schema, function *ir.Function) []output.ToSql {
	ref := sql.FunctionRef{Schema: schema.Name, Function: function.Name, Params: function.ParamSigs()}
	def := function.TryGetDefinition(ir.SqlFormatPgsql8)
	out := []output.ToSql{
		&sql.FunctionCreate{
			Function:        ref,
			Returns:         function.Returns,
			Definition:      strings.TrimSpace(def.Text),
			Language:        def.Language,
			CachePolicy:     function.CachePolicy,
			SecurityDefiner: function.SecurityDefiner,
		},
	}

	if function.Owner != "" {
		out = append(out, &sql.FunctionAlterOwner{
			Function: ref,
			Role:     lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, function.Owner),
		})
	}
	if function.Description != "" {
		out = append(out, &sql.FunctionSetComment{
			Function: ref,
			Comment:  function.Description,
		})
	}

	return out
}

func getFunctionDropSql(schema *ir.Schema, function *ir.Function) []output.ToSql {
	types := function.ParamTypes()
	for i, paramType := range types {
		// TODO(feat) there's evidence in get_drop_sql that postgres only recognizes the normalized typenames here.
		// we should look for other cases and validate behavior
		types[i] = normalizeFunctionParameterType(paramType)
	}

	return []output.ToSql{
		&sql.FunctionDrop{
			Function: sql.FunctionRef{
				Schema:   schema.Name,
				Function: function.Name,
				Params:   types,
			}},
	}
}

func normalizeFunctionParameterType(paramType string) string {
	if strings.EqualFold(paramType, "character varying") || strings.EqualFold(paramType, "varying") {
		return "varchar"
	}
	return paramType
}

func getFunctionGrantSql(schema *ir.Schema, fn *ir.Function, grant *ir.Grant) []output.ToSql {
	roles := make([]string, len(grant.Roles))
	for i, role := range grant.Roles {
		roles[i] = lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, role)
	}

	perms := util.IIntersectStrs(grant.Permissions, ir.PermissionListAllPgsql8)
	if len(perms) == 0 {
		lib.GlobalDBSteward.Fatal("No format-compatible permissions on function %s.%s(%s) grant: %v", schema.Name, fn.Name, grant.Permissions, strings.Join(fn.ParamTypes(), ","))
	}
	invalidPerms := util.IDifferenceStrs(perms, ir.PermissionListValidFunction)
	if len(invalidPerms) > 0 {
		lib.GlobalDBSteward.Fatal("Invalid permissions on sequence grant: %v", invalidPerms)
	}

	ddl := []output.ToSql{
		&sql.FunctionGrant{
			Function: sql.FunctionRef{Schema: schema.Name, Function: fn.Name, Params: fn.ParamTypes()},
			Perms:    []string(grant.Permissions),
			Roles:    roles,
			CanGrant: grant.CanGrant(),
		},
	}

	// TODO(feat) should there be an implicit read-only grant?

	return ddl
}

// TODO(go,3) move this to model
func functionDependsOnType(fn *ir.Function, typeSchema *ir.Schema, datatype *ir.TypeDef) bool {
	// TODO(feat) what about composite/domain types that are also dependent on the type? further refinement needed
	qualifiedName := typeSchema.Name + "." + datatype.Name
	returns := strings.TrimRight(fn.Returns, "[] ") // allow for arrays
	if strings.EqualFold(returns, datatype.Name) || strings.EqualFold(returns, qualifiedName) {
		return true
	}
	for _, param := range fn.Parameters {
		paramType := strings.TrimRight(param.Type, "[] ")
		if strings.EqualFold(paramType, datatype.Name) || strings.EqualFold(paramType, qualifiedName) {
			return true
		}
	}
	return false
}
