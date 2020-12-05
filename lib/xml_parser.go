package lib

import (
	"github.com/dbsteward/dbsteward/lib/format"
	"github.com/dbsteward/dbsteward/lib/xml"
)

// TODO(go,3) no globals
var GlobalXmlParser *XmlParser = NewXmlParser()

type XmlParser struct{}

func NewXmlParser() *XmlParser {
	return &XmlParser{}
}

func (self *XmlParser) GetSqlFormat(files []string) format.SqlFormat {
	// TODO(go,core)
	return format.SqlFormatPgsql8
}

func (self *XmlParser) XmlComposite(files []string) xml.DocumentTBD {
	// TODO(go,core)
	return nil
}

func (self *XmlParser) XmlCompositeAddendums(files []string, addendums uint) (xml.DocumentTBD, xml.DocumentTBD) {
	// TODO(go,core)
	return nil, nil
}

func (self *XmlParser) XmlCompositePgData(doc xml.DocumentTBD, dataFiles []string) xml.DocumentTBD {
	// TODO(go,core)
	return nil
}

func (self *XmlParser) SqlFormatConvert(doc xml.DocumentTBD) xml.DocumentTBD {
	// TODO(go,core)
	return nil
}

func (self *XmlParser) VendorParse(doc xml.DocumentTBD) {
	// TODO(go,core)
}

func (self *XmlParser) SaveDoc(filename string, doc xml.DocumentTBD) {
	// TODO(go,core)
}

func (self *XmlParser) SlonyIdNumber(doc xml.DocumentTBD) xml.DocumentTBD {
	// TODO(go,slony)
	return nil
}

func (self *XmlParser) FileSort(file, sortedFile string) {
	// TODO(go,xmlutil)
}

func (self *XmlParser) FormatXml(doc xml.DocumentTBD) string {
	// TODO(go,xmlutil)
	return ""
}
