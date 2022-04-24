package xml

import (
	"regexp"
	"strings"
)

// A DelimitedList is a list of strings parsed from a string delimited by spaces or commas
type DelimitedList []string

var spaceCommaRegex = regexp.MustCompile(`[\,\s]+`)

func ParseDelimitedList(str string) DelimitedList {
	return DelimitedList(spaceCommaRegex.Split(str, -1))
}

func (self *DelimitedList) Append(item string) {
	*self = append(*self, item)
}

func (self *DelimitedList) Joined() string {
	return strings.Join([]string(*self), ", ")
}

func (self *DelimitedList) UnmarshalText(text []byte) error {
	*self = ParseDelimitedList(string(text))
	return nil
}

func (self *DelimitedList) MarshalText() ([]byte, error) {
	return []byte(self.Joined()), nil
}

// A DelimitedList is a list of strings parsed from a string delimited by commas possibly surrounded by spaces
type CommaDelimitedList []string

var commaRegex = regexp.MustCompile(`\s*,+\s*`)

func ParseCommaDelimitedList(str string) CommaDelimitedList {
	return CommaDelimitedList(commaRegex.Split(str, -1))
}

func (self *CommaDelimitedList) Append(item string) {
	*self = append(*self, item)
}

func (self *CommaDelimitedList) Joined() string {
	return strings.Join([]string(*self), ",")
}

func (self *CommaDelimitedList) UnmarshalText(text []byte) error {
	*self = ParseCommaDelimitedList(string(text))
	return nil
}

func (self *CommaDelimitedList) MarshalText() ([]byte, error) {
	return []byte(self.Joined()), nil
}
