package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Language struct {
}

func NewLanguage() *Language {
	return &Language{}
}

func (self *Language) GetCreationSql(lang *model.Language) []output.ToSql {
	return nil
}
