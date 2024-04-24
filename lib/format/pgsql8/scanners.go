package pgsql8

import (
	"database/sql"
	"fmt"
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

// For reasons unknown, pgx doesn't know how to scan a pgsql char into a go string
type char2str struct {
	str *string
}

func (self *char2str) Scan(value interface{}) error {
	switch v := value.(type) {
	case []uint8:
		*self.str = string(v)
	default:
		return fmt.Errorf("unexpected underlying pgx type %T (%v) for a pgsql char", v, v)
	}
	return nil
}
