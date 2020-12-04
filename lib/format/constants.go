package format

type SqlFormat string

const (
	SqlFormatUnknown SqlFormat = ""
	SqlFormatPgsql8  SqlFormat = "pgsql8"
)
