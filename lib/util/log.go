package util

type Logger interface {
	FatalIfError(error, string, ...interface{})
	Fatal(string, ...interface{})
	ErrorIfError(error, string, ...interface{})
	Error(string, ...interface{})
	Warning(string, ...interface{})
	Info(string, ...interface{})
}
