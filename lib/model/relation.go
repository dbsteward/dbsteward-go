package model

type Relation interface {
	GetGrantsForRole(string) []*Grant
	AddGrant(*Grant)
}
