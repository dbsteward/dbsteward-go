package lib

import (
	"fmt"
	"os"
	"strings"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8"

	"github.com/dbsteward/dbsteward/lib/format"

	"github.com/alexflint/go-arg"
	"github.com/rs/zerolog"
)

// NOTE: 2.0.0 is the intended golang release. 3.0.0 is the intended refactor/modernization
var Version = "2.0.0"

// NOTE: we're attempting to maintain "api" compat with legacy dbsteward for now
var ApiVersion = "1.4"

// TODO(go,3) no globals
var GlobalDBSteward *DBSteward = NewDBSteward()

type DBSteward struct {
	logger zerolog.Logger

	sqlFormat format.SqlFormat

	createLanguages                bool
	requireSlonyId                 bool
	requireSlonySetId              bool
	generateSlonik                 bool
	slonyIdStartValue              uint
	slonyIdSetValue                uint
	outputFileStatementLimit       uint
	ignoreCustomRoles              bool
	ignorePrimaryKeyErrors         bool
	requireVerboseIntervalNotation bool
	quoteSchemaNames               bool
	quoteObjectNames               bool
	quoteTableNames                bool
	quoteFunctionNames             bool
	quoteColumnNames               bool
	quoteAllNames                  bool
	quoteIllegalIdentifiers        bool
	quoteReservedIdentifiers       bool
	onlySchemaSql                  bool
	onlyDataSql                    bool
	limitToTables                  map[string][]string
	singleStageUpgrade             bool
	fileOutputDirectory            string
	fileOutputPrefix               string
	ignoreOldNames                 bool
	allowFunctionRedefinition      bool
	alwaysRecreateViews            bool

	dbHost string
	dbPort uint
	dbName string
	dbUser string
	dbPass string

	oldDatabase interface{} // TODO(go,core)
	newDatabase interface{} // TODO(go,core)
}

func NewDBSteward() *DBSteward {
	dbsteward := &DBSteward{
		logger: zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger(),

		sqlFormat: format.SqlFormatUnknown,

		createLanguages:                false,
		requireSlonyId:                 false,
		requireSlonySetId:              false,
		generateSlonik:                 false,
		slonyIdStartValue:              1,
		slonyIdSetValue:                1,
		outputFileStatementLimit:       900,
		ignoreCustomRoles:              false,
		ignorePrimaryKeyErrors:         false,
		requireVerboseIntervalNotation: false,
		quoteSchemaNames:               false,
		quoteObjectNames:               false,
		quoteTableNames:                false,
		quoteFunctionNames:             false,
		quoteColumnNames:               false,
		quoteAllNames:                  false,
		quoteIllegalIdentifiers:        false,
		quoteReservedIdentifiers:       false,
		onlySchemaSql:                  false,
		onlyDataSql:                    false,
		limitToTables:                  map[string][]string{},
		singleStageUpgrade:             false,
		fileOutputDirectory:            "",
		fileOutputPrefix:               "",
		ignoreOldNames:                 false,
		allowFunctionRedefinition:      false,
		alwaysRecreateViews:            true,

		dbHost: "",
		dbPort: 0,
		dbName: "",
		dbUser: "",
		dbPass: "",

		oldDatabase: nil, // TODO(go,core)
		newDatabase: nil, // TODO(go,core)
	}

	return dbsteward
}

func (self *DBSteward) FormatGeneral() format.GeneralOperations {
	return format.GlobalGeneralOperations[self.sqlFormat]
}

// correlates to dbsteward->arg_parse()
func (self *DBSteward) ArgParse() {
	// TODO(go,nth): deck this out with better go-arg config
	args := &Args{}
	arg.MustParse(args)

	self.setVerbosity(args)

	// XML file parameter sanity checks
	if len(args.XmlFiles) > 0 {
		if len(args.OldXmlFiles) > 0 {
			self.Fatal("Parameter error: xml and oldxml options are not to be mixed. Did you mean newxml?")
		}
		if len(args.NewXmlFiles) > 0 {
			self.Fatal("Parameter error: xml and newxml options are not to be mixed. Did you mean oldxml?")
		}
	}
	if len(args.OldXmlFiles) > 0 && len(args.NewXmlFiles) == 0 {
		self.Fatal("Parameter error: oldxml needs newxml specified for differencing to occur")
	}
	if len(args.NewXmlFiles) > 0 && len(args.OldXmlFiles) == 0 {
		self.Fatal("Parameter error: oldxml needs newxml specified for differencing to occur")
	}

	// database connectivity values
	self.dbHost = args.DbHost
	self.dbPort = args.DbPort
	self.dbName = args.DbName
	self.dbUser = args.DbUser
	self.dbPass = args.DbPassword

	// SQL DDL DML DCL output flags
	self.onlySchemaSql = args.OnlySchemaSql
	self.onlyDataSql = args.OnlyDataSql
	for _, onlyTable := range args.OnlyTables {
		table := ParseQualifiedTableName(onlyTable)
		self.limitToTables[table.Schema] = append(self.limitToTables[table.Schema], table.Table)
	}

	// XML parsing switches
	self.singleStageUpgrade = args.SingleStageUpgrade
	if self.singleStageUpgrade {
		// don't recreate views when in single stage upgrade mode
		// TODO make view diffing smart enough that this doesn't need to be done
		self.alwaysRecreateViews = false
	}
	self.ignoreOldNames = args.IgnoreOldNames
	self.ignoreCustomRoles = args.IgnoreCustomRoles
	self.ignorePrimaryKeyErrors = args.IgnorePrimaryKeyErrors
	self.requireSlonyId = args.RequireSlonyId
	self.requireSlonySetId = args.RequireSlonySetId
	self.generateSlonik = args.GenerateSlonik
	self.slonyIdStartValue = args.SlonyIdStartValue
	self.slonyIdSetValue = args.SlonyIdSetValue

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
			self.Fatal("xmldatainsert needs xml parameter defined")
		} else if len(args.XmlFiles) > 1 {
			self.Fatal("xmldatainsert only supports one xml file")
		}
	}
	if mode == ModeExtract || mode == ModeDbDataDiff {
		if len(args.DbHost) == 0 {
			self.Fatal("dbhost not specified")
		}
		if len(args.DbName) == 0 {
			self.Fatal("dbname not specified")
		}
		if len(args.DbUser) == 0 {
			self.Fatal("dbuser not specified")
		}
	}
	if mode == ModeExtract || mode == ModeSqlDiff {
		if len(args.OutputFile) == 0 {
			self.Fatal("output file not specified")
		}
	}
	if mode == ModeXmlSlonyId {
		if len(args.SlonyIdOut) > 0 {
			if args.SlonyIdIn[0] == args.SlonyIdOut {
				// TODO resolve filepaths to do this correctly
				// TODO check all SlonyIdIn elements
				self.Fatal("slonyidin and slonyidout file paths should not be the same")
			}
		}
	}

	if len(args.OutputDir) > 0 {
		if !IsDir(args.OutputDir) {
			self.Fatal("outputdir is not a directory, must be a writable directory")
		}
		self.fileOutputDirectory = args.OutputDir
	}
	self.fileOutputPrefix = args.OutputFilePrefix

	// For the appropriate modes, composite the input xml
	// and figure out the sql format of it
	targetSqlFormat := format.SqlFormatUnknown
	switch mode {
	case ModeBuild:
		targetSqlFormat = GlobalXmlParser.GetSqlFormat(args.XmlFiles)
	case ModeDiff:
		// prefer new format over old
		targetSqlFormat = GlobalXmlParser.GetSqlFormat(args.NewXmlFiles)
		if targetSqlFormat == format.SqlFormatUnknown {
			targetSqlFormat = GlobalXmlParser.GetSqlFormat(args.OldXmlFiles)
		}
	}

	if args.XmlCollectDataAddendums > 0 {
		if mode != ModeDbDataDiff {
			self.Fatal("--xmlcollectdataaddendums is only supported for fresh builds")
		}
		// dammit go
		// invalid operation: args.XmlCollectDataAddendums > len(args.XmlFiles) (mismatched types uint and int)
		if int(args.XmlCollectDataAddendums) > len(args.XmlFiles) {
			self.Fatal("Cannot collect more data addendums than files provided")
		}
	}

	self.Notice("DBSteward Version %s", Version)

	// set the global sql format
	self.sqlFormat = self.reconcileSqlFormat(targetSqlFormat, args.SqlFormat)
	self.Notice("Using sqlformat=%s", self.sqlFormat)

	if self.dbPort == 0 {
		// TODO(go,nth) this is just super jank
		self.dbPort = self.defineSqlFormatDefaultValues(self.sqlFormat, args)
	}

	self.quoteSchemaNames = args.QuoteSchemaNames
	self.quoteTableNames = args.QuoteTableNames
	self.quoteColumnNames = args.QuoteColumnNames
	self.quoteAllNames = args.QuoteAllNames
	self.quoteIllegalIdentifiers = args.QuoteIllegalNames
	self.quoteReservedIdentifiers = args.QuoteReservedNames

	// TODO(go,3) move all of these to separate subcommands
	switch mode {
	case ModeXmlDataInsert:
		self.doXmlDataInsert(args.XmlFiles[0], args.XmlDataInsert)
	case ModeXmlSort:
		self.doXmlSort(args.XmlSort)
	case ModeXmlConvert:
		self.doXmlConvert(args.XmlConvert)
	case ModeXmlSlonyId:
		self.doXmlSlonyId(args.SlonyIdIn, args.SlonyIdOut)
	case ModeBuild:
		self.doBuild(args.XmlFiles, args.PgDataXml, args.XmlCollectDataAddendums)
	case ModeDiff:
		self.doDiff(args.OldXmlFiles, args.NewXmlFiles, args.PgDataXml)
	case ModeExtract:
		self.doExtract(args.DbHost, args.DbPort, args.DbName, args.DbUser, args.DbPassword, args.OutputFile)
	case ModeDbDataDiff:
		self.doDbDataDiff(args.XmlFiles, args.PgDataXml, args.XmlCollectDataAddendums, args.DbHost, args.DbPort, args.DbName, args.DbUser, args.DbPassword)
	case ModeSqlDiff:
		self.doSqlDiff(args.OldSql, args.NewSql, args.OutputFile)
	case ModeSlonikConvert:
		self.doSlonikConvert(args.SlonikConvert, args.OutputFile)
	case ModeSlonyCompare:
		self.doSlonyCompare(args.SlonyCompare)
	case ModeSlonyDiff:
		self.doSlonyDiff(args.SlonyDiffOld, args.SlonyDiffNew)
	default:
		self.Fatal("No operation specified")
	}
}

func (self *DBSteward) Fatal(s string, args ...interface{}) {
	self.logger.Fatal().Msgf(s, args...)
}

func (self *DBSteward) Warning(s string, args ...interface{}) {
	self.logger.Warn().Msgf(s, args...)
}
func (self *DBSteward) Notice(s string, args ...interface{}) {
	// TODO(go,nth) differentiate between notice and info
	self.Info(s, args...)
}
func (self *DBSteward) Info(s string, args ...interface{}) {
	self.logger.Info().Msgf(s, args...)
}

// dbsteward::set_verbosity($options)
func (self *DBSteward) setVerbosity(args *Args) {
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

	self.logger = self.logger.Level(level)
}

func (self *DBSteward) reconcileSqlFormat(target, requested format.SqlFormat) format.SqlFormat {
	if target != format.SqlFormatUnknown {
		if requested != format.SqlFormatUnknown {
			if target == requested {
				return target
			}

			self.Warning("XML is targeted for %s but you are forcing %s. Things will probably break!", target, requested)
			return requested
		}

		self.Notice("XML file(s) are targetd for sqlformat=%s", target)
		return target
	}

	if requested != format.SqlFormatUnknown {
		return requested
	}

	return format.DefaultSqlFormat
}

func (self *DBSteward) defineSqlFormatDefaultValues(sqlFormat format.SqlFormat, args *Args) uint {
	var dbPort uint
	switch sqlFormat {
	case format.SqlFormatPgsql8:
		self.createLanguages = true
		self.quoteSchemaNames = false
		self.quoteTableNames = false
		self.quoteColumnNames = false
		dbPort = 5432

		// TODO(go,mssql)
		// case format.SqlFormatMssql10:
		// 	self.quoteTableNames = true
		// 	self.quoteColumnNames = true
		// 	dbPort = 1433

		// TODO(go,mysql)
		// case format.SqlFormatMysql5:
		// 	self.quoteSchemaNames = true
		// 	self.quoteTableNames = true
		// 	self.quoteColumnNames = true
		// 	dbPort = 3306

		// 	mysql5.GlobalMysql5.UseAutoIncrementTableOptions = args.UseAutoIncrementOptions
		// 	mysql5.GlobalMysql5.UseSchemaNamePrefix = args.UseSchemaPrefix
	}

	if sqlFormat != format.SqlFormatPgsql8 {
		if len(args.PgDataXml) > 0 {
			self.Fatal("pgdataxml parameter is not supported by %s driver", sqlFormat)
		}
	}

	return dbPort
}

func (self *DBSteward) calculateFileOutputPrefix(files []string) string {
	// TODO(go,core)
	return ""
}

func (self *DBSteward) doXmlDataInsert(defFile string, dataFile string) {
	// TODO(go,core)
}
func (self *DBSteward) doXmlSort(files []string) {
	// TODO(go,core)
}
func (self *DBSteward) doXmlConvert(files []string) {
	// TODO(go,core)
}
func (self *DBSteward) doXmlSlonyId(files []string, slonyOut string) {
	self.Info("Compositing XML file for Slony ID processing")
	dbDoc := GlobalXmlParser.XmlComposite(files)
	self.Info("Xml files %s composited", strings.Join(files, " "))

	outputPrefix := self.calculateFileOutputPrefix(files)
	compositeFile := outputPrefix + "_composite.xml"
	dbDoc = GlobalXmlParser.SqlFormatConvert(dbDoc)
	GlobalXmlParser.VendorParse(dbDoc)
	self.Notice("Saving composite as %s", compositeFile)
	GlobalXmlParser.SaveDoc(compositeFile, dbDoc)

	self.Notice("Slony ID numbering any missing attributes")
	self.Info("slonyidstartvalue = %d", self.slonyIdStartValue)
	self.Info("slonyidsetvalue = %d", self.slonyIdSetValue)
	slonyIdDoc := GlobalXmlParser.SlonyIdNumber(dbDoc)
	slonyIdNumberedFile := outputPrefix + "_slonyid_numbered.xml"
	if len(slonyOut) > 0 {
		slonyIdNumberedFile = slonyOut
	}
	self.Notice("Saving Slony ID numbered XML as %s", slonyIdNumberedFile)
	GlobalXmlParser.SaveDoc(slonyIdNumberedFile, slonyIdDoc)
}
func (self *DBSteward) doBuild(files []string, dataFiles []string, addendums uint) {
	self.Info("Compositing XML files...")
	if addendums > 0 {
		self.Info("Collecting %d data addendums", addendums)
	}
	dbDoc, addendumsDoc := GlobalXmlParser.XmlCompositeAddendums(files, addendums)

	if len(dataFiles) > 0 {
		self.Info("Compositing pgdata XML files on top of XML composite...")
		GlobalXmlParser.XmlCompositePgData(dbDoc, dataFiles)
		self.Info("postgres data XML files [%s] composited", strings.Join(dataFiles, " "))
	}

	self.Info("XML files %s composited", strings.Join(files, " "))

	outputPrefix := self.calculateFileOutputPrefix(files)
	compositeFile := outputPrefix + "_composite.xml"
	dbDoc = GlobalXmlParser.SqlFormatConvert(dbDoc)
	GlobalXmlParser.VendorParse(dbDoc)
	self.Notice("Saving composite as %s", compositeFile)
	GlobalXmlParser.SaveDoc(compositeFile, dbDoc)

	if addendumsDoc != nil {
		addendumsFile := outputPrefix + "_addendums.xml"
		self.Notice("Saving addendums as %s", addendumsFile)
		GlobalXmlParser.SaveDoc(compositeFile, addendumsDoc)
	}

	self.FormatGeneral().Build(outputPrefix, dbDoc)
}
func (self *DBSteward) doDiff(oldFiles []string, newFiles []string, dataFiles []string) {
	self.Info("Compositing old XML files...")
	oldDbDoc := GlobalXmlParser.XmlComposite(oldFiles)
	self.Info("Old XML files %s composited", strings.Join(oldFiles, " "))

	self.Info("Compositing new XML files...")
	newDbDoc := GlobalXmlParser.XmlComposite(newFiles)
	if len(dataFiles) > 0 {
		self.Info("Compositing pgdata XML files on top of new XML composite...")
		GlobalXmlParser.XmlCompositePgData(newDbDoc, dataFiles)
		self.Info("postgres data XML files [%s] composited", strings.Join(dataFiles, " "))
	}
	self.Info("New XML files %s composited", strings.Join(newFiles, " "))

	oldOutputPrefix := self.calculateFileOutputPrefix(oldFiles)
	oldCompositeFile := oldOutputPrefix + "_composite.xml"
	oldDbDoc = GlobalXmlParser.SqlFormatConvert(oldDbDoc)
	GlobalXmlParser.VendorParse(oldDbDoc)
	self.Notice("Saving composite as %s", oldCompositeFile)
	GlobalXmlParser.SaveDoc(oldCompositeFile, oldDbDoc)

	newOutputPrefix := self.calculateFileOutputPrefix(newFiles)
	newCompositeFile := newOutputPrefix + "_composite.xml"
	newDbDoc = GlobalXmlParser.SqlFormatConvert(newDbDoc)
	GlobalXmlParser.VendorParse(newDbDoc)
	self.Notice("Saving composite as %s", newCompositeFile)
	GlobalXmlParser.SaveDoc(newCompositeFile, newDbDoc)

	self.FormatGeneral().BuildUpgrade(
		oldOutputPrefix, oldCompositeFile, oldDbDoc, oldFiles,
		newOutputPrefix, newCompositeFile, newDbDoc, newFiles,
	)
}
func (self *DBSteward) doExtract(dbHost string, dbPort uint, dbName, dbUser, dbPass string, outputFile string) {
	output := self.FormatGeneral().ExtractSchema(dbHost, dbPort, dbName, dbUser, dbPass)
	self.Notice("Saving extracted database schema to %s", outputFile)
	GlobalXmlParser.SaveDoc(outputFile, output)
}
func (self *DBSteward) doDbDataDiff(files []string, dataFiles []string, addendums uint, dbHost string, dbPort uint, dbName, dbUser, dbPass string) {
	self.Info("Compositing XML files...")
	if addendums > 0 {
		self.Info("Collecting %d data addendums", addendums)
	}
	// TODO(go,nth) can this just be XmlComposite(files)? why do we need addendums?
	dbDoc, _ := GlobalXmlParser.XmlCompositeAddendums(files, addendums)

	if len(dataFiles) > 0 {
		self.Info("Compositing pgdata XML files on top of XML composite...")
		GlobalXmlParser.XmlCompositePgData(dbDoc, dataFiles)
		self.Info("postgres data XML files [%s] composited", strings.Join(dataFiles, " "))
	}

	self.Info("XML files %s composited", strings.Join(files, " "))

	outputPrefix := self.calculateFileOutputPrefix(files)
	compositeFile := outputPrefix + "_composite.xml"
	dbDoc = GlobalXmlParser.SqlFormatConvert(dbDoc)
	GlobalXmlParser.VendorParse(dbDoc)
	self.Notice("Saving composite as %s", compositeFile)
	GlobalXmlParser.SaveDoc(compositeFile, dbDoc)

	output := self.FormatGeneral().CompareDbData(dbDoc, dbHost, dbPort, dbName, dbUser, dbPass)
	GlobalXmlParser.SaveDoc(compositeFile, output)
}
func (self *DBSteward) doSqlDiff(oldSql string, newSql string, outputFile string) {
	self.FormatGeneral().SqlDiff(oldSql, newSql, outputFile)
}
func (self *DBSteward) doSlonikConvert(file string, outputFile string) {
	// TODO(go,nth) is there a nicer way to handle this output idiom?
	output := GlobalSlonik.Convert(file)
	if len(outputFile) > 0 {
		err := WriteFile(output, outputFile)
		if err != nil {
			self.Fatal("Failed to save slonikconvert output to %s: %s", outputFile, err.Error())
		}
	} else {
		fmt.Println(output)
	}
}
func (self *DBSteward) doSlonyCompare(file string) {
	pgsql8.GlobalPgsql8.SlonyCompare(file)
}
func (self *DBSteward) doSlonyDiff(oldFile string, newFile string) {
	pgsql8.GlobalPgsql8.SlonyDiff(oldFile, newFile)
}
