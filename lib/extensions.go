package lib

import (
	"fmt"
	"sync"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Operations interface {
	Build(outputPrefix string, dbDoc *ir.Definition) error
	BuildUpgrade(
		oldOutputPrefix, oldCompositeFile string, oldDbDoc *ir.Definition, oldFiles []string,
		newOutputPrefix, newCompositeFile string, newDbDoc *ir.Definition, newFiles []string,
	) error
	ExtractSchema(host string, port uint, name, user, pass string) (*ir.Definition, error)
	CompareDbData(dbDoc *ir.Definition, host string, port uint, name, user, pass string) (*ir.Definition, error)
	SqlDiff(old, new []string, outputFile string)

	GetQuoter() output.Quoter
}

type Encoding interface {
}

var formats = make(map[ir.SqlFormat]func(Config) Operations)

var formatMutex sync.Mutex

func RegisterFormat(id ir.SqlFormat, constructor func(Config) Operations) {
	formatMutex.Lock()
	defer formatMutex.Unlock()
	formats[id] = constructor
}

func Format(id ir.SqlFormat) (func(Config) Operations, error) {
	formatMutex.Lock()
	defer formatMutex.Unlock()
	constructor, exists := formats[id]
	if !exists {
		return nil, fmt.Errorf("no such format as %s", id)
	}
	return constructor, nil
}
