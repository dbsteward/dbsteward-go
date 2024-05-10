package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

func getCreateLanguageSql(conf lib.Config, lang *ir.Language) ([]output.ToSql, error) {
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
		role, err := roleEnum(conf.Logger, conf.NewDatabase, lang.Owner, conf.IgnoreCustomRoles)
		if err != nil {
			return nil, err
		}
		out = append(out, &sql.LanguageAlterOwner{
			Language:   lang.Name,
			Procedural: lang.Procedural,
			Role:       role,
		})
	}

	return out, nil
}

func getDropLanguageSql(lang *ir.Language) []output.ToSql {
	return []output.ToSql{
		&sql.LanguageDrop{
			Language:   lang.Name,
			Procedural: lang.Procedural,
		},
	}
}
