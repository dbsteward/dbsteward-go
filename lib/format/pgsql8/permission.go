package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/model"
)

var GlobalPermission *Permission = NewPermission()

type Permission struct {
}

func NewPermission() *Permission {
	return &Permission{}
}

func (self *Permission) GetGrantSql(doc *model.Definition, schema *model.Schema, object interface{}, grant *model.Grant) []lib.ToSql {
	// TODO(go,pgsql)
	return nil
}

func (self *Permission) GetRevokeSql(doc *model.Definition, schema *model.Schema, object interface{}, revoke *model.Revoke) []lib.ToSql {
	// TODO(go,pgsql)
	return nil
}
