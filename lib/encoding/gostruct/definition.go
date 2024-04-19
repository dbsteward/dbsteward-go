package gostruct

type Database struct {
	Name    string
	Schemas []Schema
}

type Schema struct {
	Name   string
	Tables []Table
}

type Table struct {
	Name        string
	Description string
	Fields      any
}
