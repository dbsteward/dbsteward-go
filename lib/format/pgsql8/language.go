package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/model"
)

var GlobalLanguage *Language = NewLanguage()

type Language struct {
}

func NewLanguage() *Language {
	return &Language{}
}

func (self *Language) GetCreationSql(lang *model.Language) []lib.ToSql {
	return nil
}
