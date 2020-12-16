package model

const (
	RolePgsql       = "PGSQL"
	RolePublic      = "PUBLIC"
	RoleOwner       = "ROLE_OWNER"
	RoleApplication = "ROLE_APPLICATION"
	RoleReplication = "ROLE_REPLICATION"
	RoleReadOnly    = "ROLE_READONLY"
)

var MACRO_ROLES = []string{RolePgsql, RolePublic, RoleOwner, RoleApplication, RoleReplication, RoleReadOnly}
