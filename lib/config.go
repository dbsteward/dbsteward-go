package lib

import (
	"log/slog"

	"github.com/dbsteward/dbsteward/lib/ir"
)

// Config is a structure containing all configuration information
// for any execution of code.
type Config struct {
	Logger                         *slog.Logger
	SqlFormat                      ir.SqlFormat
	CreateLanguages                bool
	RequireSlonyId                 bool
	RequireSlonySetId              bool
	GenerateSlonik                 bool
	SlonyIdStartValue              uint
	SlonyIdSetValue                uint
	OutputFileStatementLimit       uint
	IgnoreCustomRoles              bool
	IgnorePrimaryKeyErrors         bool
	RequireVerboseIntervalNotation bool
	QuoteSchemaNames               bool
	QuoteObjectNames               bool
	QuoteTableNames                bool
	QuoteFunctionNames             bool
	QuoteColumnNames               bool
	QuoteAllNames                  bool
	QuoteIllegalIdentifiers        bool
	QuoteReservedIdentifiers       bool
	OnlySchemaSql                  bool
	OnlyDataSql                    bool
	LimitToTables                  map[string][]string
	SingleStageUpgrade             bool
	FileOutputDirectory            string
	FileOutputPrefix               string
	IgnoreOldNames                 bool
	AlwaysRecreateViews            bool
	OldDatabase                    *ir.Definition
	NewDatabase                    *ir.Definition
}
