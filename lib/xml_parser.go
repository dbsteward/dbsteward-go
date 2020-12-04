package lib

// TODO(go,3) no globals
var GlobalXmlParser *XmlParser = NewXmlParser()

type XmlParser struct{}

type DocumentTBD = interface{}

func NewXmlParser() *XmlParser {
	return &XmlParser{}
}

func (self *XmlParser) GetSqlFormat(files []string) Dialect {
	// TODO(go,core)
	return DialectPgsql
}

func (self *XmlParser) XmlComposite(files []string) DocumentTBD {
	// TODO(go,core)
	return nil
}

func (self *XmlParser) XmlCompositeAddendums(files []string, addendums uint) (DocumentTBD, DocumentTBD) {
	// TODO(go,core)
	return nil, nil
}

func (self *XmlParser) XmlCompositePgData(doc DocumentTBD, dataFiles []string) DocumentTBD {
	// TODO(go,core)
	return nil
}

func (self *XmlParser) SqlFormatConvert(doc DocumentTBD) DocumentTBD {
	// TODO(go,core)
	return nil
}

func (self *XmlParser) VendorParse(doc DocumentTBD) {
	// TODO(go,core)
}

func (self *XmlParser) SaveDoc(filename string, doc DocumentTBD) {
	// TODO(go,core)
}

func (self *XmlParser) SlonyIdNumber(doc DocumentTBD) DocumentTBD {
	// TODO(go,core)
	return nil
}
