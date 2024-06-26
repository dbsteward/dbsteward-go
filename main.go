package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path"
	"strings"

	"github.com/alexflint/go-arg"
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/config"
	"github.com/dbsteward/dbsteward/lib/encoding/xml"
	_ "github.com/dbsteward/dbsteward/lib/format/pgsql8"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/util"
	"github.com/hashicorp/go-multierror"
	"github.com/rs/zerolog"
)

func main() {
	dbsteward := NewDBSteward()
	dbsteward.ArgParse()
	dbsteward.Info("Done")
}

// NOTE: 2.0.0 is the intended golang release. 3.0.0 is the intended refactor/modernization
const Version = "2.0.0"

// NOTE: we're attempting to maintain "api" compat with legacy dbsteward for now
const ApiVersion = "1.4"

type Mode uint

const (
	ModeUnknown       Mode = 0
	ModeXmlDataInsert Mode = 1
	ModeXmlSort       Mode = 2
	ModeXmlConvert    Mode = 4
	ModeBuild         Mode = 8
	ModeDiff          Mode = 16
	ModeExtract       Mode = 32
	ModeDbDataDiff    Mode = 64
	ModeXmlSlonyId    Mode = 73
	ModeSqlDiff       Mode = 128
	ModeSlonikConvert Mode = 256
	ModeSlonyCompare  Mode = 512
	ModeSlonyDiff     Mode = 1024
)

type DBSteward struct {
	logger zerolog.Logger
	config lib.Config
}

func NewDBSteward() *DBSteward {
	dbsteward := &DBSteward{
		logger: zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger(),
		config: lib.Config{
			SqlFormat:                      lib.DefaultSqlFormat,
			CreateLanguages:                false,
			RequireSlonyId:                 false,
			RequireSlonySetId:              false,
			GenerateSlonik:                 false,
			SlonyIdStartValue:              1,
			SlonyIdSetValue:                1,
			OutputFileStatementLimit:       900,
			IgnoreCustomRoles:              false,
			IgnorePrimaryKeyErrors:         false,
			RequireVerboseIntervalNotation: false,
			QuoteSchemaNames:               false,
			QuoteObjectNames:               false,
			QuoteTableNames:                false,
			QuoteFunctionNames:             false,
			QuoteColumnNames:               false,
			QuoteAllNames:                  false,
			QuoteIllegalIdentifiers:        false,
			QuoteReservedIdentifiers:       false,
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
		},
	}

	return dbsteward
}

// correlates to dbsteward->arg_parse()
func (dbsteward *DBSteward) ArgParse() {
	// TODO(go,nth): deck this out with better go-arg config
	args := &config.Args{}
	arg.MustParse(args)

	dbsteward.setVerbosity(args)

	// XML file parameter sanity checks
	if len(args.XmlFiles) > 0 {
		if len(args.OldXmlFiles) > 0 {
			dbsteward.fatal("Parameter error: xml and oldxml options are not to be mixed. Did you mean newxml?")
		}
		if len(args.NewXmlFiles) > 0 {
			dbsteward.fatal("Parameter error: xml and newxml options are not to be mixed. Did you mean oldxml?")
		}
	}
	if len(args.OldXmlFiles) > 0 && len(args.NewXmlFiles) == 0 {
		dbsteward.fatal("Parameter error: oldxml needs newxml specified for differencing to occur")
	}
	if len(args.NewXmlFiles) > 0 && len(args.OldXmlFiles) == 0 {
		dbsteward.fatal("Parameter error: oldxml needs newxml specified for differencing to occur")
	}
	dbsteward.config.Logger = slog.New(newLogHandler(dbsteward))
	// database connectivity values
	// dbsteward.dbHost = args.DbHost
	// dbsteward.dbPort = args.DbPort
	// dbsteward.dbName = args.DbName
	// dbsteward.dbUser = args.DbUser
	// dbsteward.dbPass = args.DbPassword

	// SQL DDL DML DCL output flags
	dbsteward.config.OnlySchemaSql = args.OnlySchemaSql
	dbsteward.config.OnlyDataSql = args.OnlyDataSql
	for _, onlyTable := range args.OnlyTables {
		table := lib.ParseQualifiedTableName(onlyTable)
		dbsteward.config.LimitToTables[table.Schema] = append(dbsteward.config.LimitToTables[table.Schema], table.Table)
	}

	// XML parsing switches
	dbsteward.config.SingleStageUpgrade = args.SingleStageUpgrade
	if dbsteward.config.SingleStageUpgrade {
		// don't recreate views when in single stage upgrade mode
		// TODO(feat) make view diffing smart enough that this doesn't need to be done
		dbsteward.config.AlwaysRecreateViews = false
	}
	dbsteward.config.IgnoreOldNames = args.IgnoreOldNames
	dbsteward.config.IgnoreCustomRoles = args.IgnoreCustomRoles
	dbsteward.config.IgnorePrimaryKeyErrors = args.IgnorePrimaryKeyErrors
	dbsteward.config.RequireSlonyId = args.RequireSlonyId
	dbsteward.config.RequireSlonySetId = args.RequireSlonySetId
	dbsteward.config.GenerateSlonik = args.GenerateSlonik
	dbsteward.config.SlonyIdStartValue = args.SlonyIdStartValue
	dbsteward.config.SlonyIdSetValue = args.SlonyIdSetValue

	// determine operation and check arguments for each
	mode := ModeUnknown
	switch {
	case len(args.XmlDataInsert) > 0:
		mode = ModeXmlDataInsert
	case len(args.XmlSort) > 0:
		mode = ModeXmlSort
	case len(args.XmlConvert) > 0:
		mode = ModeXmlConvert
	case len(args.XmlFiles) > 0:
		mode = ModeBuild
	case len(args.NewXmlFiles) > 0:
		mode = ModeDiff
	case args.DbSchemaDump:
		mode = ModeExtract
	case len(args.DbDataDiff) > 0:
		mode = ModeDbDataDiff
	case len(args.OldSql) > 0 || len(args.NewSql) > 0:
		mode = ModeSqlDiff
	case len(args.SlonikConvert) > 0:
		mode = ModeSlonikConvert
	case len(args.SlonyCompare) > 0:
		mode = ModeSlonyCompare
	case len(args.SlonyDiffOld) > 0:
		mode = ModeSlonyDiff
	case len(args.SlonyIdIn) > 0:
		mode = ModeXmlSlonyId
	}

	// validate mode parameters
	if mode == ModeXmlDataInsert {
		if len(args.XmlFiles) == 0 {
			dbsteward.fatal("xmldatainsert needs xml parameter defined")
		} else if len(args.XmlFiles) > 1 {
			dbsteward.fatal("xmldatainsert only supports one xml file")
		}
	}
	if mode == ModeExtract || mode == ModeDbDataDiff {
		if len(args.DbHost) == 0 {
			dbsteward.fatal("dbhost not specified")
		}
		if len(args.DbName) == 0 {
			dbsteward.fatal("dbname not specified")
		}
		if len(args.DbUser) == 0 {
			dbsteward.fatal("dbuser not specified")
		}
		if args.DbPassword == nil {
			p, err := util.PromptPassword("[DBSteward] Enter password for postgres://%s@%s:%d/%s: ", args.DbUser, args.DbHost, args.DbPort, args.DbName)
			dbsteward.fatalIfError(err, "Could not read password input")
			args.DbPassword = &p
		}
	}
	if mode == ModeExtract || mode == ModeSqlDiff {
		if len(args.OutputFile) == 0 {
			dbsteward.fatal("output file not specified")
		}
	}
	if mode == ModeXmlSlonyId {
		if len(args.SlonyIdOut) > 0 {
			if args.SlonyIdIn[0] == args.SlonyIdOut {
				// TODO(go,nth) resolve filepaths to do this correctly
				// TODO(go,nth) check all SlonyIdIn elements
				dbsteward.fatal("slonyidin and slonyidout file paths should not be the same")
			}
		}
	}

	if len(args.OutputDir) > 0 {
		if !util.IsDir(args.OutputDir) {
			dbsteward.fatal("outputdir is not a directory, must be a writable directory")
		}
		dbsteward.config.FileOutputDirectory = args.OutputDir
	}
	dbsteward.config.FileOutputPrefix = args.OutputFilePrefix

	if args.XmlCollectDataAddendums > 0 {
		if mode != ModeDbDataDiff {
			dbsteward.fatal("--xmlcollectdataaddendums is only supported for fresh builds")
		}
		// dammit go
		// invalid operation: args.XmlCollectDataAddendums > len(args.XmlFiles) (mismatched types uint and int)
		if int(args.XmlCollectDataAddendums) > len(args.XmlFiles) {
			dbsteward.fatal("Cannot collect more data addendums than files provided")
		}
	}

	dbsteward.Info("DBSteward Version %s", Version)

	// set the global sql format
	dbsteward.config.SqlFormat = dbsteward.reconcileSqlFormat(ir.SqlFormatPgsql8, args.SqlFormat)
	dbsteward.Info("Using sqlformat=%s", dbsteward.config.SqlFormat)
	dbsteward.defineSqlFormatDefaultValues(dbsteward.config.SqlFormat, args)

	dbsteward.config.QuoteSchemaNames = args.QuoteSchemaNames
	dbsteward.config.QuoteTableNames = args.QuoteTableNames
	dbsteward.config.QuoteColumnNames = args.QuoteColumnNames
	dbsteward.config.QuoteAllNames = args.QuoteAllNames
	dbsteward.config.QuoteIllegalIdentifiers = args.QuoteIllegalNames
	dbsteward.config.QuoteReservedIdentifiers = args.QuoteReservedNames

	// TODO(go,3) move all of these to separate subcommands
	switch mode {
	case ModeXmlDataInsert:
		dbsteward.doXmlDataInsert(args.XmlFiles[0], args.XmlDataInsert)
	case ModeXmlSort:
		dbsteward.doXmlSort(args.XmlSort)
	case ModeXmlConvert:
		dbsteward.doXmlConvert(args.XmlConvert)
	case ModeXmlSlonyId:
		dbsteward.doXmlSlonyId(args.SlonyIdIn, args.SlonyIdOut)
	case ModeBuild:
		dbsteward.doBuild(args.XmlFiles, args.PgDataXml, args.XmlCollectDataAddendums)
	case ModeDiff:
		dbsteward.doDiff(args.OldXmlFiles, args.NewXmlFiles, args.PgDataXml)
	case ModeExtract:
		dbsteward.doExtract(args.DbHost, args.DbPort, args.DbName, args.DbUser, *args.DbPassword, args.OutputFile)
	case ModeDbDataDiff:
		dbsteward.doDbDataDiff(args.XmlFiles, args.PgDataXml, args.XmlCollectDataAddendums, args.DbHost, args.DbPort, args.DbName, args.DbUser, *args.DbPassword)
	case ModeSqlDiff:
		dbsteward.doSqlDiff(args.OldSql, args.NewSql, args.OutputFile)
	case ModeSlonikConvert:
		dbsteward.doSlonikConvert(args.SlonikConvert, args.OutputFile)
	case ModeSlonyCompare:
		dbsteward.doSlonyCompare(args.SlonyCompare)
	case ModeSlonyDiff:
		dbsteward.doSlonyDiff(args.SlonyDiffOld, args.SlonyDiffNew)
	default:
		dbsteward.fatal("No operation specified")
	}
}

// Logger returns an *slog.Logger pointed at the console
func (dbsteward *DBSteward) Logger() *slog.Logger {
	if dbsteward == nil {
		panic("dbsteward is nil")
	}
	if dbsteward.config.Logger == nil {
		dbsteward.config.Logger = slog.New(newLogHandler(dbsteward))
	}
	return dbsteward.config.Logger
}

func (dbsteward *DBSteward) fatal(s string, args ...interface{}) {
	dbsteward.logger.Fatal().Msgf(s, args...)
}
func (dbsteward *DBSteward) fatalIfError(err error, s string, args ...interface{}) {
	if err != nil {
		dbsteward.logger.Fatal().Err(err).Msgf(s, args...)
	}
}

func (dbsteward *DBSteward) warning(s string, args ...interface{}) {
	dbsteward.logger.Warn().Msgf(s, args...)
}

func (dbsteward *DBSteward) Info(s string, args ...interface{}) {
	dbsteward.logger.Info().Msgf(s, args...)
}

// dbsteward::set_verbosity($options)
func (dbsteward *DBSteward) setVerbosity(args *config.Args) {
	// TODO(go,nth): differentiate between notice and info

	// remember, lower level is higher verbosity
	// we're abusing the fact that zerolog.LogLevel is defined as an int8
	level := zerolog.InfoLevel

	if args.Debug {
		level = zerolog.TraceLevel
	}

	for _, v := range args.Verbose {
		if v {
			level -= 1
		} else {
			level += 1
		}
	}
	for _, q := range args.Quiet {
		if q {
			level += 1
		} else {
			level -= 1
		}
	}

	// clamp it to valid values
	if level > zerolog.PanicLevel {
		level = zerolog.PanicLevel
	}
	if level < zerolog.TraceLevel {
		level = zerolog.TraceLevel
	}

	dbsteward.logger = dbsteward.logger.Level(level)
}

func (dbsteward *DBSteward) reconcileSqlFormat(target, requested ir.SqlFormat) ir.SqlFormat {
	if target != ir.SqlFormatUnknown {
		if requested != ir.SqlFormatUnknown {
			if target == requested {
				return target
			}

			dbsteward.warning("XML is targeted for %s but you are forcing %s. Things will probably break!", target, requested)
			return requested
		}

		dbsteward.Info("XML file(s) are targetd for sqlformat=%s", target)
		return target
	}

	if requested != ir.SqlFormatUnknown {
		return requested
	}

	return lib.DefaultSqlFormat
}

func (dbsteward *DBSteward) defineSqlFormatDefaultValues(SqlFormat ir.SqlFormat, args *config.Args) {
	switch SqlFormat {
	case ir.SqlFormatPgsql8:
		dbsteward.config.CreateLanguages = true
		dbsteward.config.QuoteSchemaNames = false
		dbsteward.config.QuoteTableNames = false
		dbsteward.config.QuoteColumnNames = false
		if args.DbPort == 0 {
			args.DbPort = 5432
		}
	}

	if SqlFormat != ir.SqlFormatPgsql8 {
		if len(args.PgDataXml) > 0 {
			dbsteward.fatal("pgdataxml parameter is not supported by %s driver", SqlFormat)
		}
	}
}

func (dbsteward *DBSteward) calculateFileOutputPrefix(files []string) string {
	return path.Join(
		dbsteward.calculateFileOutputDirectory(files[0]),
		util.CoalesceStr(dbsteward.config.FileOutputPrefix, util.Basename(files[0], ".xml")),
	)
}
func (dbsteward *DBSteward) calculateFileOutputDirectory(file string) string {
	return util.CoalesceStr(dbsteward.config.FileOutputDirectory, path.Dir(file))
}

// Append columns in a table's rows collection, based on a simplified XML definition of what to insert
func (dbsteward *DBSteward) doXmlDataInsert(defFile string, dataFile string) {
	// TODO(go,xmlutil) verify this behavior is correct, add tests. need to change fatals to returns
	dbsteward.Info("Automatic insert data into %s from %s", defFile, dataFile)
	defDoc, err := xml.LoadDefintion(defFile)
	dbsteward.fatalIfError(err, "Failed to load %s", defFile)

	dataDoc, err := xml.LoadDefintion(dataFile)
	dbsteward.fatalIfError(err, "Failed to load %s", dataFile)

	for _, dataSchema := range dataDoc.Schemas {
		defSchema, err := defDoc.GetSchemaNamed(dataSchema.Name)
		dbsteward.fatalIfError(err, "while searching %s", defFile)
		for _, dataTable := range dataSchema.Tables {
			defTable, err := defSchema.GetTableNamed(dataTable.Name)
			dbsteward.fatalIfError(err, "while searching %s", defFile)

			dataRows := dataTable.Rows
			if dataRows == nil {
				dbsteward.fatal("table %s in %s does not have a <rows> element", dataTable.Name, dataFile)
			}

			if len(dataRows.Columns) == 0 {
				dbsteward.fatal("Unexpected: no rows[columns] found in table %s in file %s", dataTable.Name, dataFile)
			}

			if len(dataRows.Rows) > 1 {
				dbsteward.fatal("Unexpected: more than one rows->row found in table %s in file %s", dataTable.Name, dataFile)
			}

			if len(dataRows.Rows[0].Columns) != len(dataRows.Columns) {
				dbsteward.fatal("Unexpected: Table %s in %s defines %d colums but has %d <col> elements",
					dataTable.Name, dataFile, len(dataRows.Columns), len(dataRows.Rows[0].Columns))
			}

			for i, newColumn := range dataRows.Columns {
				dbsteward.Info("Adding rows column %s to definition table %s", newColumn, defTable.Name)

				if defTable.Rows == nil {
					defTable.Rows = &ir.DataRows{}
				}
				err = defTable.Rows.AddColumn(newColumn, dataRows.Columns[i])
				dbsteward.fatalIfError(err, "Could not add column %s to %s in %s", newColumn, dataTable.Name, dataFile)
			}
		}
	}

	defFileModified := defFile + ".xmldatainserted"
	dbsteward.Info("Saving modified dbsteward definition as %s", defFileModified)
	err = xml.SaveDefinition(dbsteward.Logger(), defFileModified, defDoc)
	dbsteward.fatalIfError(err, "saving file")
}
func (dbsteward *DBSteward) doXmlSort(files []string) {
	for _, file := range files {
		sortedFileName := file + ".xmlsorted"
		dbsteward.Info("Sorting XML definition file: %s", file)
		dbsteward.Info("Sorted XML output file: %s", sortedFileName)
		xml.FileSort(file, sortedFileName)
	}
}
func (dbsteward *DBSteward) doXmlConvert(files []string) {
	for _, file := range files {
		convertedFileName := file + ".xmlconverted"
		dbsteward.Info("Upconverting XML definition file: %s", file)
		dbsteward.Info("Upconvert XML output file: %s", convertedFileName)

		doc, err := xml.LoadDefintion(file)
		dbsteward.fatalIfError(err, "Could not load %s", file)
		xml.SqlFormatConvert(doc)
		convertedXml, err := xml.FormatXml(dbsteward.Logger(), doc)
		dbsteward.fatalIfError(err, "formatting xml")
		convertedXml = strings.Replace(convertedXml, "pgdbxml>", "dbsteward>", -1)
		err = util.WriteFile(convertedXml, convertedFileName)
		dbsteward.fatalIfError(err, "Could not write converted xml to %s", convertedFileName)
	}
}
func (dbsteward *DBSteward) doXmlSlonyId(files []string, slonyOut string) {
	dbsteward.Info("Compositing XML file for Slony ID processing")
	dbDoc, err := xml.XmlComposite(dbsteward.Logger(), files)
	dbsteward.fatalIfError(err, "compositing files: %v", files)
	dbsteward.Info("Xml files %s composited", strings.Join(files, " "))

	outputPrefix := dbsteward.calculateFileOutputPrefix(files)
	compositeFile := outputPrefix + "_composite.xml"
	dbsteward.Info("Saving composite as %s", compositeFile)
	err = xml.SaveDefinition(dbsteward.Logger(), compositeFile, dbDoc)
	dbsteward.fatalIfError(err, "saving file")

	dbsteward.Info("Slony ID numbering any missing attributes")
	dbsteward.Info("slonyidstartvalue = %d", dbsteward.config.SlonyIdStartValue)
	dbsteward.Info("slonyidsetvalue = %d", dbsteward.config.SlonyIdSetValue)
	slonyIdDoc := xml.SlonyIdNumber(dbDoc)
	slonyIdNumberedFile := outputPrefix + "_slonyid_numbered.xml"
	if len(slonyOut) > 0 {
		slonyIdNumberedFile = slonyOut
	}
	dbsteward.Info("Saving Slony ID numbered XML as %s", slonyIdNumberedFile)
	err = xml.SaveDefinition(dbsteward.Logger(), slonyIdNumberedFile, slonyIdDoc)
	dbsteward.fatalIfError(err, "saving file")
}
func (dbsteward *DBSteward) doBuild(files []string, dataFiles []string, addendums uint) {
	dbsteward.Info("Compositing XML files...")
	if addendums > 0 {
		dbsteward.Info("Collecting %d data addendums", addendums)
	}
	dbDoc, addendumsDoc, err := xml.XmlCompositeAddendums(dbsteward.Logger(), files, addendums)
	if err != nil {
		mErr, isMErr := err.(*multierror.Error)
		if isMErr {
			for _, e := range mErr.Errors {
				log.Println(e.Error())
			}
		} else {
			log.Println(err.Error())
		}
		os.Exit(1)
	}
	if len(dataFiles) > 0 {
		dbsteward.Info("Compositing pgdata XML files on top of XML composite...")
		xml.XmlCompositePgData(dbDoc, dataFiles)
		dbsteward.Info("postgres data XML files [%s] composited", strings.Join(dataFiles, " "))
	}

	dbsteward.Info("XML files %s composited", strings.Join(files, " "))

	outputPrefix := dbsteward.calculateFileOutputPrefix(files)
	compositeFile := outputPrefix + "_composite.xml"
	dbsteward.Info("Saving composite as %s", compositeFile)
	err = xml.SaveDefinition(dbsteward.Logger(), compositeFile, dbDoc)
	dbsteward.fatalIfError(err, "saving file")

	if addendumsDoc != nil {
		addendumsFile := outputPrefix + "_addendums.xml"
		dbsteward.Info("Saving addendums as %s", addendumsFile)
		err = xml.SaveDefinition(dbsteward.Logger(), compositeFile, addendumsDoc)
		dbsteward.fatalIfError(err, "saving file")
	}

	ops, err := lib.Format(lib.DefaultSqlFormat)
	dbsteward.fatalIfError(err, "loading default format")
	err = ops(dbsteward.config).Build(outputPrefix, dbDoc)
	dbsteward.fatalIfError(err, "building")
}
func (dbsteward *DBSteward) doDiff(oldFiles []string, newFiles []string, dataFiles []string) {
	dbsteward.Info("Compositing old XML files...")
	oldDbDoc, err := xml.XmlComposite(dbsteward.Logger(), oldFiles)
	dbsteward.fatalIfError(err, "compositing")
	dbsteward.Info("Old XML files %s composited", strings.Join(oldFiles, " "))

	dbsteward.Info("Compositing new XML files...")
	newDbDoc, err := xml.XmlComposite(dbsteward.Logger(), newFiles)
	dbsteward.fatalIfError(err, "compositing")
	if len(dataFiles) > 0 {
		dbsteward.Info("Compositing pgdata XML files on top of new XML composite...")
		xml.XmlCompositePgData(newDbDoc, dataFiles)
		dbsteward.Info("postgres data XML files [%s] composited", strings.Join(dataFiles, " "))
	}
	dbsteward.Info("New XML files %s composited", strings.Join(newFiles, " "))

	oldOutputPrefix := dbsteward.calculateFileOutputPrefix(oldFiles)
	oldCompositeFile := oldOutputPrefix + "_composite.xml"
	dbsteward.Info("Saving composite as %s", oldCompositeFile)
	err = xml.SaveDefinition(dbsteward.Logger(), oldCompositeFile, oldDbDoc)
	dbsteward.fatalIfError(err, "saving file")

	newOutputPrefix := dbsteward.calculateFileOutputPrefix(newFiles)
	newCompositeFile := newOutputPrefix + "_composite.xml"
	dbsteward.Info("Saving composite as %s", newCompositeFile)
	err = xml.SaveDefinition(dbsteward.Logger(), newCompositeFile, newDbDoc)
	dbsteward.fatalIfError(err, "saving file")

	ops, err := lib.Format(lib.DefaultSqlFormat)
	dbsteward.fatalIfError(err, "loading default format")
	err = ops(dbsteward.config).BuildUpgrade(
		oldOutputPrefix, oldCompositeFile, oldDbDoc, oldFiles,
		newOutputPrefix, newCompositeFile, newDbDoc, newFiles,
	)
	dbsteward.fatalIfError(err, "building upgrade")
}
func (dbsteward *DBSteward) doExtract(dbHost string, dbPort uint, dbName, dbUser, dbPass string, outputFile string) {
	ops, err := lib.Format(lib.DefaultSqlFormat)
	dbsteward.fatalIfError(err, "loading default format")
	output, err := ops(dbsteward.config).ExtractSchema(dbHost, dbPort, dbName, dbUser, dbPass)
	dbsteward.fatalIfError(err, "extracting")
	dbsteward.Info("Saving extracted database schema to %s", outputFile)
	err = xml.SaveDefinition(dbsteward.Logger(), outputFile, output)
	dbsteward.fatalIfError(err, "saving file")
}
func (dbsteward *DBSteward) doDbDataDiff(files []string, dataFiles []string, addendums uint, dbHost string, dbPort uint, dbName, dbUser, dbPass string) {
	dbsteward.Info("Compositing XML files...")
	if addendums > 0 {
		dbsteward.Info("Collecting %d data addendums", addendums)
	}
	// TODO(feat) can this just be XmlComposite(files)? why do we need addendums?
	dbDoc, _, err := xml.XmlCompositeAddendums(dbsteward.Logger(), files, addendums)
	dbsteward.fatalIfError(err, "compositing addendums")

	if len(dataFiles) > 0 {
		dbsteward.Info("Compositing pgdata XML files on top of XML composite...")
		xml.XmlCompositePgData(dbDoc, dataFiles)
		dbsteward.Info("postgres data XML files [%s] composited", strings.Join(dataFiles, " "))
	}

	dbsteward.Info("XML files %s composited", strings.Join(files, " "))

	outputPrefix := dbsteward.calculateFileOutputPrefix(files)
	compositeFile := outputPrefix + "_composite.xml"
	dbsteward.Info("Saving composite as %s", compositeFile)
	err = xml.SaveDefinition(dbsteward.Logger(), compositeFile, dbDoc)
	dbsteward.fatalIfError(err, "saving file")

	ops, err := lib.Format(lib.DefaultSqlFormat)
	dbsteward.fatalIfError(err, "loading default format")
	output, err := ops(dbsteward.config).CompareDbData(dbDoc, dbHost, dbPort, dbName, dbUser, dbPass)
	dbsteward.fatalIfError(err, "comparing data")
	err = xml.SaveDefinition(dbsteward.Logger(), compositeFile, output)
	dbsteward.fatalIfError(err, "saving file")
}
func (dbsteward *DBSteward) doSqlDiff(oldSql, newSql []string, outputFile string) {
	ops, err := lib.Format(lib.DefaultSqlFormat)
	dbsteward.fatalIfError(err, "loading default format")
	ops(dbsteward.config).SqlDiff(oldSql, newSql, outputFile)
}
func (dbsteward *DBSteward) doSlonikConvert(file string, outputFile string) {
	// TODO(go,nth) is there a nicer way to handle this output idiom?
	output := lib.NewSlonik().Convert(file)
	if len(outputFile) > 0 {
		err := util.WriteFile(output, outputFile)
		dbsteward.fatalIfError(err, "Failed to save slonikconvert output to %s", outputFile)
	} else {
		fmt.Println(output)
	}
}
func (dbsteward *DBSteward) doSlonyCompare(file string) {
	ops, err := lib.Format(lib.DefaultSqlFormat)
	dbsteward.fatalIfError(err, "loading default format")
	ops(dbsteward.config).(lib.SlonyOperations).SlonyCompare(file)
}
func (dbsteward *DBSteward) doSlonyDiff(oldFile string, newFile string) {
	ops, err := lib.Format(lib.DefaultSqlFormat)
	dbsteward.fatalIfError(err, "loading default format")
	ops(dbsteward.config).(lib.SlonyOperations).SlonyDiff(oldFile, newFile)
}

func newLogHandler(dbs *DBSteward) slog.Handler {
	buf := bytes.Buffer{}
	f := slog.NewTextHandler(&buf, nil)
	return &logHandler{
		dbsteward: dbs,
		formatter: f,
		output:    &buf,
	}
}

// logHandler is an intermediate step to support both slog logging
// and the old method of dbsteward logging
type logHandler struct {
	dbsteward *DBSteward
	formatter slog.Handler
	output    *bytes.Buffer
}

// Enabled always returns true and let zerolog decide
func (h *logHandler) Enabled(_ context.Context, level slog.Level) bool {
	return true
}

func (h *logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &logHandler{
		dbsteward: h.dbsteward,
		output:    h.output,
		formatter: h.formatter.WithAttrs(attrs),
	}
}

func (h *logHandler) WithGroup(name string) slog.Handler {
	return &logHandler{
		dbsteward: h.dbsteward,
		output:    h.output,
		formatter: h.formatter.WithGroup(name),
	}
}

// Handle is a bit of a hack. Just using TextFormatter to do the actual
// handling and and then extracting the result from the byte buffer to
// send it to zerolog as an intermediate step that maintains nearly
// the same behavior as previous while still supporting all of slog's
// features
func (h *logHandler) Handle(ctx context.Context, r slog.Record) error {
	h.formatter.Handle(ctx, r)
	msg := strings.TrimSpace(h.output.String())
	if msg == "" {
		msg = "<<logHander received empty message>>"
	}
	switch r.Level {
	case slog.LevelDebug:
		h.dbsteward.logger.Debug().Msgf(msg)
	case slog.LevelInfo:
		h.dbsteward.logger.Info().Msgf(msg)
	case slog.LevelWarn:
		h.dbsteward.logger.Warn().Msgf(msg)
	default:
		// Should be Error, but in case other levels get define at
		// least nothing gets lost
		h.dbsteward.logger.Error().Msgf(msg)
	}
	h.output.Reset()
	return nil
}
