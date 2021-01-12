package format

import "github.com/dbsteward/dbsteward/lib/model"

type LookupMap map[model.SqlFormat]*Lookup

type Lookup struct {
	Operations Operations
	XmlParser  XmlParser
}
