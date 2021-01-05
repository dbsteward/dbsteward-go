package util

type Logger interface {
	FatalIfError(error, string, ...interface{})
	Fatal(string, ...interface{})
	ErrorIfError(error, string, ...interface{})
	Error(string, ...interface{})
	Warning(string, ...interface{})
	Notice(string, ...interface{})
	Info(string, ...interface{})
	Trace(string, ...interface{})
}
