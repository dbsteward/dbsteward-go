package pgsql8

import (
	"log/slog"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/ir"
)

var commonSchema = NewSchema()

func init() {
	lib.RegisterFormat(ir.SqlFormatPgsql8, NewOperations)
}

var DefaultConfig = lib.Config{
	Logger:                         slog.Default(),
	SqlFormat:                      ir.SqlFormatPgsql8,
	CreateLanguages:                false,
	RequireSlonyId:                 false,
	RequireSlonySetId:              false,
	GenerateSlonik:                 false,
	SlonyIdStartValue:              1,
	SlonyIdSetValue:                1,
	OutputFileStatementLimit:       999999,
	IgnoreCustomRoles:              false,
	IgnorePrimaryKeyErrors:         false,
	RequireVerboseIntervalNotation: false,
	QuoteSchemaNames:               false,
	QuoteObjectNames:               false,
	QuoteTableNames:                false,
	QuoteFunctionNames:             false,
	QuoteColumnNames:               false,
	QuoteAllNames:                  false,
	QuoteIllegalIdentifiers:        true,
	QuoteReservedIdentifiers:       true,
	OnlySchemaSql:                  false,
	OnlyDataSql:                    false,
	LimitToTables:                  map[string][]string{},
	SingleStageUpgrade:             false,
	FileOutputDirectory:            "",
	FileOutputPrefix:               "",
	IgnoreOldNames:                 false,
	AlwaysRecreateViews:            true,
	OldDatabase:                    nil,
	NewDatabase:                    nil,
}
