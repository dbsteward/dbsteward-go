package live

import (
	"database/sql"
)

type maybeStr struct {
	str *string
}

func (self *maybeStr) Scan(value interface{}) error {
	s := sql.NullString{}
	err := s.Scan(value)
	if err != nil {
		return err
	}
	*self.str = s.String
	return nil
}
