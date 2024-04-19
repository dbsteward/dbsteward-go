package ir

import (
	"fmt"
	"strings"
)

type SqlFormat string

const (
	SqlFormatUnknown SqlFormat = ""
	SqlFormatPgsql8  SqlFormat = "pgsql8"
	SqlFormatMssql10 SqlFormat = "mssql10"
	SqlFormatMysql5  SqlFormat = "mysql5"
)

func NewSqlFormat(from string) (SqlFormat, error) {
	to := SqlFormat(from)
	if to.Equals(SqlFormatUnknown) || to.Equals(SqlFormatPgsql8) || to.Equals(SqlFormatMysql5) || to.Equals(SqlFormatMssql10) {
		return to, nil
	}
	return to, fmt.Errorf("unknown SqlFormat: '%s'", from)
}

func (sf SqlFormat) Equals(other SqlFormat) bool {
	return strings.EqualFold(string(sf), string(other))
}

type SqlStage string

const (
	SqlStageNone    SqlStage = ""
	SqlStage1Before SqlStage = "STAGE1BEFORE"
	SqlStage2Before SqlStage = "STAGE2BEFORE"
	SqlStage3Before SqlStage = "STAGE3BEFORE"
	SqlStage4Before SqlStage = "STAGE4BEFORE"
	SqlStage1       SqlStage = "STAGE1"
	SqlStage2       SqlStage = "STAGE2"
	SqlStage3       SqlStage = "STAGE3"
	SqlStage4       SqlStage = "STAGE4"
)

func (self SqlStage) Equals(other SqlStage) bool {
	return strings.EqualFold(string(self), string(other))
}

// Not making these a type because they need to live alongside non-constant roles
const (
	RolePgsql       = "PGSQL"
	RolePublic      = "PUBLIC"
	RoleOwner       = "ROLE_OWNER"
	RoleApplication = "ROLE_APPLICATION"
	RoleReplication = "ROLE_SLONY" // TODO(go,3) make more agnostic
	RoleReadOnly    = "ROLE_READONLY"
)

var MACRO_ROLES = []string{RolePgsql, RolePublic, RoleOwner, RoleApplication, RoleReplication, RoleReadOnly}
