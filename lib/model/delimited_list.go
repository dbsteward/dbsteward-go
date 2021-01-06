package model

import (
	"encoding/xml"
	"regexp"
)

var spaceCommaRegex = regexp.MustCompile(`[\,\s]+`)
var commaRegex = regexp.MustCompile(`\s*,+\s*`)

type DelimitedList []string

func (self *DelimitedList) UnmarshalXMLAttr(attr xml.Attr) error {
	items := spaceCommaRegex.Split(attr.Value, -1)
	*self = make(DelimitedList, len(items))
	for i, item := range items {
		(*self)[i] = item
	}
	return nil
}

type CommaDelimitedList []string

func (self *CommaDelimitedList) UnmarshalXMLAttr(attr xml.Attr) error {
	items := commaRegex.Split(attr.Value, -1)
	*self = make(CommaDelimitedList, len(items))
	for i, item := range items {
		(*self)[i] = item
	}
	return nil
}
