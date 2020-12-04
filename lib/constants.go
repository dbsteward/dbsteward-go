package lib

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
