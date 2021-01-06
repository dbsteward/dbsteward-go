package model

import (
	"strings"
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

type Grant struct {
	Roles       DelimitedList      `xml:"role,attr"`
	Permissions CommaDelimitedList `xml:"operation,attr"`
	With        string             `xml:"with,attr"`
}

type Revoke struct {
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
