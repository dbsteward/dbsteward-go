package model

type Table struct {
	Name   string    `xml:"name,attr"`
	Grants []*Grant  `xml:"grant"`
	Rows   *DataRows `xml:"rows"`
}

func (self *Table) HasDefaultNextVal() bool {
	// TODO(go,core)
	return false
}
