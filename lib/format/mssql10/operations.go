package mssql10

import (
	"github.com/dbsteward/dbsteward/lib/format/sql99"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Operations struct {
	*sql99.Operations
}

func NewOperations() *Operations {
	ops := &Operations{
		Operations: sql99.NewOperations(),
	}
	ops.Operations.Operations = ops
	return ops
}

func (self *Operations) Build(outputPrefix string, dbDoc *ir.Definition) {
	// TODO(go,mssql) implement me
}
func (self *Operations) BuildUpgrade(
	oldOutputPrefix string, oldCompositeFile string, oldDoc *ir.Definition, oldFiles []string,
	newOutputPrefix string, newCompositeFile string, newDoc *ir.Definition, newFiles []string,
) {
	// TODO(go,mssql) implement me
}

func (self *Operations) ExtractSchema(host string, port uint, name, user, pass string) *ir.Definition {
	// TODO(go,mssql) implement me
	return nil
}
func (self *Operations) CompareDbData(doc *ir.Definition, host string, port uint, name, user, pass string) *ir.Definition {
	// TODO(go,mssql) implement me
	return nil
}

func (self *Operations) GetQuoter() output.Quoter {
	// TODO(go,core) why is this part of public interface? can it not be?
	// TODO(go,mssql) implement me
	return nil
}

func (self *Operations) SqlDiff(old, new []string, upgradePrefix string) {
	// TODO(go,mssql) implement me
}
