package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib/xml"
)

var GlobalPgsql8 *Pgsql8 = NewPgsql8()

type Pgsql8 struct {
}

func NewPgsql8() *Pgsql8 {
	return &Pgsql8{}
}

func (self *Pgsql8) Build(outputPrefix string, dbDoc xml.DocumentTBD) {
	// TODO(go,pgsql)
}
func (self *Pgsql8) BuildUpgrade(
	oldOutputPrefix string, oldCompositeFile string, oldDbDoc xml.DocumentTBD, oldFiles []string,
	newOutputPrefix string, newCompositeFile string, newDbDoc xml.DocumentTBD, newFiles []string,
) {
	// TODO(go,pgsql)
}
func (self *Pgsql8) ExtractSchema(host string, port uint, name, user, pass string) xml.DocumentTBD {
	// TODO(go,pgsql)
	return nil
}
func (self *Pgsql8) CompareDbData(dbDoc xml.DocumentTBD, host string, port uint, name, user, pass string) xml.DocumentTBD {
	// TODO(go,pgsql)
	return nil
}
func (self *Pgsql8) SqlDiff(old, new, outputFile string) {
	// TODO(go,pgsql)
}

func (self *Pgsql8) SlonyCompare(file string) {
	// TODO(go,slony)
}
func (self *Pgsql8) SlonyDiff(oldFile string, newFile string) {
	// TODO(go,slony)
}
