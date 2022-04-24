package xml

import (
	"encoding/xml"
	"regexp"
	"strings"
)

var spaceCommaRegex = regexp.MustCompile(`[\,\s]+`)
var commaRegex = regexp.MustCompile(`\s*,+\s*`)

type DelimitedList []string

func ParseDelimitedList(str string) DelimitedList {
	return DelimitedList(spaceCommaRegex.Split(str, -1))
}

func (self *DelimitedList) Append(item string) {
	*self = append(*self, item)
}

func (self *DelimitedList) Joined() string {
	return strings.Join([]string(*self), ", ")
}

func (self *DelimitedList) UnmarshalXMLAttr(attr xml.Attr) error {
	*self = ParseDelimitedList(attr.Value)
	return nil
}

func (self *DelimitedList) MarshalXMLAttr(name xml.Name) (xml.Attr, error) {
	// TODO(go,nth) retain the original separators used and reuse them when marshalling
	return xml.Attr{
		Name:  name,
		Value: self.Joined(),
	}, nil
}

func (self *DelimitedList) UnmarshalXML(decoder *xml.Decoder, start xml.StartElement) error {
	var value string
	err := decoder.DecodeElement(&value, &start)
	if err != nil {
		return err
	}
	*self = ParseDelimitedList(value)
	return nil
}

func (self *DelimitedList) MarshalXML(encoder *xml.Encoder, start xml.StartElement) error {
	return encoder.EncodeElement(self.Joined(), start)
}

type CommaDelimitedList []string

func ParseCommaDelimitedList(str string) CommaDelimitedList {
	return CommaDelimitedList(commaRegex.Split(str, -1))
}

func (self *CommaDelimitedList) Append(item string) {
	*self = append(*self, item)
}

func (self *CommaDelimitedList) Joined() string {
	return strings.Join([]string(*self), ",")
}

func (self *CommaDelimitedList) UnmarshalXMLAttr(attr xml.Attr) error {
	*self = ParseCommaDelimitedList(attr.Value)
	return nil
}

func (self *CommaDelimitedList) MarshalXMLAttr(name xml.Name) (xml.Attr, error) {
	// TODO(go,nth) retain the original separators used and reuse them when marshalling
	return xml.Attr{
		Name:  name,
		Value: self.Joined(),
	}, nil
}

func (self *CommaDelimitedList) UnmarshalXML(decoder *xml.Decoder, start xml.StartElement) error {
	var value string
	err := decoder.DecodeElement(&value, &start)
	if err != nil {
		return err
	}
	*self = ParseCommaDelimitedList(value)
	return nil
}

func (self *CommaDelimitedList) MarshalXML(encoder *xml.Encoder, start xml.StartElement) error {
	return encoder.EncodeElement(self.Joined(), start)
}
