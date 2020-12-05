package xml

import (
	"encoding/xml"
	"os"

	"github.com/pkg/errors"
)

func LoadDataXmlFile(file string) (*DataDocument, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, errors.Wrapf(err, "could not read dataxml file %s", file)
	}
	defer f.Close()

	doc := &DataDocument{}
	err = xml.NewDecoder(f).Decode(doc)
	if err != nil {
		return nil, errors.Wrapf(err, "could not parse dataxml file %s", file)
	}
	return doc, nil
}

// An XML document, as used by ModeXmlDataInsert
// For example:
// ```xml
// <dbsteward>
//   <schema name="public">
//     <table name="course_list">
//       <rows columns="app_mode">
//         <row>
//           <col>{SCHD,CMS}</col>
//         </row>
//       </rows>
//     </table>
//   </schema>
// </dbsteward>
// ````
type DataDocument struct {
	Schemas []*DataSchema `xml:"schema"`
}

type DataSchema struct {
	Name   string       `xml:"name,attr"`
	Tables []*DataTable `xml:"table"`
}

type DataTable struct {
	Name string    `xml:"name,attr"`
	Rows *DataRows `xml:"rows"`
}

type DataRows struct {
	Columns DelimitedList `xml:"columns,attr"`
	Rows    []*DataRow    `xml:"row"`
}

type DataRow struct {
	Columns []string `xml:"col"`
}
