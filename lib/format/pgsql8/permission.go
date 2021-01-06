package pgsql8

var GlobalPermission *Permission = NewPermission()

type Permission struct {
}

func NewPermission() *Permission {
	return &Permission{}
}
