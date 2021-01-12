package pgsql8

import "github.com/dbsteward/dbsteward/lib/model"

type XmlParser struct {
}

func NewXmlParser() *XmlParser {
	return &XmlParser{}
}

func (self *XmlParser) Process(doc *model.Definition) {
	// TODO(go,pgsql) pgsql8_xml_parser::process
}
