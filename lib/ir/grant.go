package ir

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib/util"
)

// TODO(go,nth) can we make this a dedicated type? it makes some other code icky though
// Taken from https://www.postgresql.org/docs/13/ddl-priv.html
// TODO(go,mysql) add mysql perms
// TODO(go,mssql) add mssql perms
const (
	PermissionAll = "ALL"

	PermissionSelect     = "SELECT"
	PermissionInsert     = "INSERT"
	PermissionUpdate     = "UPDATE"
	PermissionDelete     = "DELETE"
	PermissionTruncate   = "TRUNCATE"
	PermissionReferences = "REFERENCES"
	PermissionTrigger    = "TRIGGER"
	PermissionCreate     = "CREATE"
	PermissionConnect    = "CONNECT"
	PermissionTemporary  = "TEMPORARY"
	PermissionExecute    = "EXECUTE"
	PermissionUsage      = "USAGE"

	PermissionCreateTable = "CREATE TABLE"
	PermissionAlter       = "ALTER"
)

var PermissionListAllPgsql8 = []string{
	PermissionAll,
	PermissionSelect,
	PermissionInsert,
	PermissionUpdate,
	PermissionDelete,
	PermissionTruncate,
	PermissionReferences,
	PermissionTrigger,
	PermissionCreate,
	PermissionConnect,
	PermissionTemporary,
	PermissionExecute,
	PermissionUsage,
}

var PermissionListAllMssql10 = []string{
	PermissionAll,
	PermissionCreateTable,
	PermissionAlter,
}

var PermissionListAllMysql5 = []string{
	// TODO(go,mysql)
}

var PermissionListSqlFormatMap = map[SqlFormat][]string{
	SqlFormatPgsql8:  PermissionListAllPgsql8,
	SqlFormatMysql5:  PermissionListAllMysql5,
	SqlFormatMssql10: PermissionListAllMssql10,
}

var PermissionListValidSchema = []string{
	PermissionAll,
	PermissionUsage,
	PermissionCreate,
}

var PermissionListValidTable = []string{
	PermissionAll,
	PermissionSelect,
	PermissionInsert,
	PermissionUpdate,
	PermissionDelete,
	PermissionTruncate,
	PermissionReferences,
	PermissionTrigger,
	PermissionCreateTable,
	PermissionAlter,
}

// TODO(feat) can views handle other permissions??
var PermissionListValidView = []string{
	PermissionAll,
	PermissionSelect,
}

var PermissionListValidSequence = []string{
	PermissionAll,
	PermissionUsage,
	PermissionSelect,
	PermissionUpdate,
}

var PermissionListValidFunction = []string{
	PermissionAll,
	PermissionExecute,
}

const PermOptionGrant = "GRANT"

// Identifies things that have Grants
type HasGrants interface {
	GetGrants() []*Grant
}

type Grant struct {
	Roles       []string
	Permissions []string
	With        string
}

func (self *Grant) AddPermission(op string) {
	self.Permissions = append(self.Permissions, op)
}

func (self *Grant) CanGrant() bool {
	return strings.EqualFold(self.With, PermOptionGrant)
}

func (self *Grant) SetCanGrant(canGrant bool) {
	if canGrant {
		self.With = PermOptionGrant
	} else {
		self.With = ""
	}
}

func HasPermissionsOf(object HasGrants, target *Grant, sqlFormat SqlFormat) bool {
	formatPerms := PermissionListSqlFormatMap[sqlFormat]
	targetPerms := util.IIntersectStrs(target.Permissions, formatPerms)

	// first, catalog the effective permissions provided by the target grant
	rolePerms := map[string][]string{}
	roleWith := map[string]string{}
	for _, role := range target.Roles {
		rolePerms[role] = make([]string, len(targetPerms))
		copy(rolePerms[role], targetPerms)
		roleWith[role] = target.With
	}

	// then for each grant in the object, remove that grant's permissions from the role
	// signalling that that permission has been accounted for
	for _, grant := range object.GetGrants() {
		for _, role := range grant.Roles {
			rolePerms[role] = util.IDifferenceStrs(rolePerms[role], grant.Permissions)
			if grant.With == roleWith[role] {
				roleWith[role] = ""
			}
		}
	}

	// finally, if all roles have had all permissions accounted for, then yes, object HasPermissionsOf target
	// if any have unaccounted-for permissions, then no, object does not HasPermissionsOf target
	for _, perms := range rolePerms {
		if len(perms) > 0 {
			return false
		}
	}
	for _, with := range roleWith {
		if with != "" {
			return false
		}
	}
	return true
}
