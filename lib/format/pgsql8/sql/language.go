package sql

import (
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

type LanguageCreate struct {
	Language   string
	Trusted    bool
	Procedural bool
	Handler    string
	Validator  string
}

func (self *LanguageCreate) ToSql(q output.Quoter) string {
	return util.CondJoin(
		" ",
		"CREATE",
		util.MaybeStr(self.Trusted, "TRUSTED"),
		util.MaybeStr(self.Procedural, "PROCEDURAL"),
		"LANGUAGE",
		q.QuoteObject(self.Language),
		util.MaybeStr(self.Handler != "", "HANDLER "+self.Handler),
		util.MaybeStr(self.Validator != "", "VALIDATOR "+self.Validator),
	)
}

type LanguageDrop struct {
	Language   string
	Procedural bool
}

func (self *LanguageDrop) ToSql(q output.Quoter) string {
	return util.CondJoin(
		" ",
		"DROP",
		util.MaybeStr(self.Procedural, "PROCEDURAL"),
		"LANGAUGE",
		q.QuoteObject(self.Language),
	)
}

type LanguageAlterOwner struct {
	Language   string
	Procedural bool
	Role       string
}

func (self *LanguageAlterOwner) ToSql(q output.Quoter) string {
	return util.CondJoin(
		" ",
		"ALTER",
		util.MaybeStr(self.Procedural, "PROCEDURAL"),
		"LANGUAGE",
		q.QuoteObject(self.Language),
		"OWNER TO",
		q.QuoteRole(self.Role),
	)
}
