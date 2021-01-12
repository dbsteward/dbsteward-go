package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib/output"
)

type DiffLanguages struct {
}

func NewDiffLanguages() *DiffLanguages {
	return &DiffLanguages{}
}

func (self *DiffLanguages) DiffLanguages(ofs output.OutputFileSegmenter) {
	// TODO(go,pgsql)
}
