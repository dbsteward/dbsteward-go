package format

type SqlFormat string

const (
	SqlFormatUnknown SqlFormat = ""
	SqlFormatPgsql8  SqlFormat = "pgsql8"
	SqlFormatMssql10 SqlFormat = "mssql10"
	SqlFormatMysql5  SqlFormat = "mysql5"
)

const DefaultSqlFormat = SqlFormatPgsql8
