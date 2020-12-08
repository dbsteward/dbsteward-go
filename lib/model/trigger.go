package model

import (
	"github.com/dbsteward/dbsteward/lib/format"
)

type Trigger struct {
	SqlFormat format.SqlFormat `xml:"sqlFormat"`
}
