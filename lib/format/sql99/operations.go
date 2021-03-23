package sql99

import (
	"github.com/dbsteward/dbsteward/lib/format"
)

type Operations struct {
	format.Operations
}

// NOTE: Sql99.OperationsIface will need to be provided after invoking:
//     parent := &sql99.Sql99{}
//     child := &pgsql8.Pgsql8{parent}
//     child.sql99.OperationsIface = child
// Yes, this is super weird, and a holdover from PHP. TODO(go,3) get rid of this
func NewOperations() *Operations {
	return &Operations{}
}
