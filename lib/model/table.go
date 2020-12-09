package model

type Table struct {
	Name       string        `xml:"name,attr"`
	PrimaryKey DelimitedList `xml:"primaryKey,attr"`
	Grants     []*Grant      `xml:"grant"`
	Rows       *DataRows     `xml:"rows"`
}

type Column struct {
	Name        string `xml:"name,attr"`
	Type        string `xml:"type,attr"`
	SerialStart string `xml:"serialStart,attr"`
}

func (self *Table) HasDefaultNextVal() bool {
	// TODO(go,core)
	return false
}
