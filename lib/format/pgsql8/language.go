package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

func getCreateLanguageSql(lang *ir.Language) []output.ToSql {
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
			Role:       roleEnum(lib.GlobalDBSteward.NewDatabase, lang.Owner),
		})
	}

	return out
}

func getDropLanguageSql(lang *ir.Language) []output.ToSql {
	return []output.ToSql{
		&sql.LanguageDrop{
			Language:   lang.Name,
			Procedural: lang.Procedural,
		},
	}
}
