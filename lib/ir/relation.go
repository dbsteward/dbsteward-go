package ir

type Relation interface {
	GetOwner() string
	GetGrantsForRole(string) []*Grant
	AddGrant(*Grant)
}
