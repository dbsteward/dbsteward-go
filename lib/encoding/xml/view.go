package xml

import "github.com/dbsteward/dbsteward/lib/model"

type View struct {
	Name           string        `xml:"name,attr"`
	Description    string        `xml:"description,attr,omitempty"`
	Owner          string        `xml:"owner,attr,omitempty"`
	DependsOnViews DelimitedList `xml:"dependsOnViews,attr,omitempty"`
	SlonySetId     *int          `xml:"slonySetId,attr,omitempty"`
	Grants         []*Grant      `xml:"grant"`
	Queries        []*ViewQuery  `xml:"viewQuery"`
}

type ViewQuery struct {
	SqlFormat string `xml:"sqlFormat,attr,omitempty"`
	Text      string `xml:",cdata"`
}

func (self *View) ToModel() (*model.View, error) {
	panic("todo")
}
