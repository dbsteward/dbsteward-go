package xml

import (
	"encoding/xml"
	"regexp"
)

type DelimitedList []string

var spaceCommaRegex = regexp.MustCompile("[\\,\\s]+")

func (self DelimitedList) UnmarshalXMLAttr(attr xml.Attr) error {
	copy(spaceCommaRegex.Split(attr.Value, -1), self)
	return nil
}
