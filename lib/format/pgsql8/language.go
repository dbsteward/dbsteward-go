package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Language struct {
}

func NewLanguage() *Language {
	return &Language{}
}

func (self *Language) GetCreationSql(lang *model.Language) []output.ToSql {
	out := []output.ToSql{
		&sql.LanguageCreate{
			Language:   lang.Name,
			Trusted:    lang.Trusted,
			Procedural: lang.Procedural,
			Handler:    lang.Handler,
			Validator:  lang.Validator,
		},
	}

	if lang.Owner != "" {
		out = append(out, &sql.LanguageAlterOwner{
			Language:   lang.Name,
			Procedural: lang.Procedural,
			Role:       lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, lang.Owner),
		})
	}

	return out
}

func (self *Language) GetDropSql(lang *model.Language) []output.ToSql {
	return []output.ToSql{
		&sql.LanguageDrop{
			Language:   lang.Name,
			Procedural: lang.Procedural,
		},
	}
}
