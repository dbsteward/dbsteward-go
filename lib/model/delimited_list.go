package model

import (
	"encoding/xml"
	"regexp"
	"strings"
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

func (self *DelimitedList) MarshalXMLAttr(name xml.Name) (xml.Attr, error) {
	// TODO(go,nth) retain the original separators used and reuse them when marshalling
	return xml.Attr{
		Name:  name,
		Value: strings.Join([]string(*self), ", "),
	}, nil
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

func (self *CommaDelimitedList) MarshalXMLAttr(name xml.Name) (xml.Attr, error) {
	// TODO(go,nth) retain the original separators used and reuse them when marshalling
	return xml.Attr{
		Name:  name,
		Value: strings.Join([]string(*self), ", "),
	}, nil
}
