package config

import (
	"github.com/dbsteward/dbsteward/lib/model"
)

// sourced from dbsteward::arg_parse() and usage.php

type Args struct {
	// Global Switches and Flags
	SqlFormat model.SqlFormat `arg:"--sqlformat" help:"change the SQL dialect to operate in. If not specified or cannot be derived"`
	Verbose   []bool          `arg:"-v" help:"see more detail (verbose). -vvv is not advised for normal use."`
	Quiet     []bool          `arg:"-q" help:"see less detail (quiet)."`
	Debug     bool            `arg:"--debug" help:"display extended information about errors. Automatically implies -vv."`
	// Handled by go-arg
	// Help bool `arg:"-h,--help" help:"show this usage information"`
	QuoteSchemaNames bool `arg:"--quoteschemanames" help:"quote schema names in SQL output"`
	QuoteTableNames  bool `arg:"--quotetablenames" help:"quote table names in SQL output"`
	// TODO(go,core): fill out rest of arg/help tags, this is very tedious
	QuoteColumnNames   bool
	QuoteAllNames      bool
	QuoteIllegalNames  bool
	QuoteReservedNames bool

	// Generating SQL DDL/DML/DCL
	XmlFiles  []string `arg:"--xml"`
	PgDataXml []string `arg:"--pgdataxml"`

	// Generating SQL DDL/DML/DCL to upgrade old to new
	OldXmlFiles            []string `arg:"--oldxml"`
	NewXmlFiles            []string `arg:"--newxml"`
	OnlySchemaSql          bool
	OnlyDataSql            bool
	OnlyTables             []string
	SingleStageUpgrade     bool
	MaxStatementsPerFile   uint
	IgnoreOldNames         bool
	IgnoreCustomRoles      bool
	IgnorePrimaryKeyErrors bool

	// Database definition extraction utilities
	DbSchemaDump bool
	DbDataDiff   []string
	DbHost       string
	DbPort       uint
	DbName       string
	DbUser       string
	DbPassword   *string

	// XML utilities
	XmlSort                 []string
	XmlConvert              []string
	XmlDataInsert           string
	XmlCollectDataAddendums uint

	// Output options
	OutputDir        string
	OutputFilePrefix string

	// SQL diffing
	OldSql     []string
	NewSql     []string
	OutputFile string

	// Slony utils
	RequireSlonyId    bool
	RequireSlonySetId bool
	GenerateSlonik    bool
	SlonikConvert     string
	SlonyCompare      string
	SlonyDiffOld      string
	SlonyDiffNew      string
	SlonyIdIn         []string
	SlonyIdOut        string
	SlonyIdStartValue uint
	SlonyIdSetValue   uint

	// Format-specific options
	UseAutoIncrementOptions bool
	UseSchemaPrefix         bool `arg:"--useschemaprefix"`
}
