package model

import (
	"github.com/pkg/errors"
)

type Definition struct {
	Schemas   []*Schema   `xml:"schema"`
	Languages []*Language `xml:"language"`
}

func (self *Definition) GetSchemaNamed(name string) (*Schema, error) {
	matching := []*Schema{}
	for _, schema := range self.Schemas {
		if schema.Name == name {
			matching = append(matching, schema)
		}
	}
	if len(matching) == 0 {
		return nil, errors.Errorf("no schema named %s found", name)
	}
	if len(matching) > 1 {
		return nil, errors.Errorf("more than one schema named %s found", name)
	}
	return matching[0], nil
}
