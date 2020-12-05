package model

type Table struct {
	Name string    `xml:"name,attr"`
	Rows *DataRows `xml:"rows"`
}
