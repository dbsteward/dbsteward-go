package model

type SqlFormat string

const (
	SqlFormatUnknown SqlFormat = ""
	SqlFormatPgsql8  SqlFormat = "pgsql8"
	SqlFormatMssql10 SqlFormat = "mssql10"
	SqlFormatMysql5  SqlFormat = "mysql5"
)

type SqlStage string

const (
	SqlStageNone    SqlStage = ""
	SqlStage1Before SqlStage = "STAGE1BEFORE"
	SqlStage2Before SqlStage = "STAGE2BEFORE"
	SqlStage1       SqlStage = "STAGE1"
	SqlStage2       SqlStage = "STAGE2"
	SqlStage3       SqlStage = "STAGE3"
	SqlStage4       SqlStage = "STAGE4"
)

// Not making these a type because they need to live alongside non-constant roles
const (
	RolePgsql       = "PGSQL"
	RolePublic      = "PUBLIC"
	RoleOwner       = "ROLE_OWNER"
	RoleApplication = "ROLE_APPLICATION"
	RoleReplication = "ROLE_REPLICATION"
	RoleReadOnly    = "ROLE_READONLY"
)

var MACRO_ROLES = []string{RolePgsql, RolePublic, RoleOwner, RoleApplication, RoleReplication, RoleReadOnly}
