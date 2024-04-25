package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/output"
)

type DiffLanguages struct {
}

func NewDiffLanguages() *DiffLanguages {
	return &DiffLanguages{}
}

func (self *DiffLanguages) DiffLanguages(ofs output.OutputFileSegmenter) {
	// TODO(go,pgsql) this is a different flow than old dbsteward:
	// we do equality comparison inside these two methods, instead of a separate loop
	// need to validate that this behavior is still correct

	self.dropLanguages(ofs)
	self.createLanguages(ofs)
}

func (self *DiffLanguages) dropLanguages(ofs output.OutputFileSegmenter) {
	newDoc := lib.GlobalDBSteward.NewDatabase
	oldDoc := lib.GlobalDBSteward.OldDatabase

	// drop languages that either do not exist in the new schema or have changed
	if oldDoc != nil {
		for _, oldLang := range oldDoc.Languages {
			newLang := newDoc.TryGetLanguageNamed(oldLang.Name)
			if newLang == nil || !oldLang.Equals(newLang) {
				ofs.WriteSql(getDropLanguageSql(oldLang)...)
			}
		}
	}
}

func (self *DiffLanguages) createLanguages(ofs output.OutputFileSegmenter) {
	newDoc := lib.GlobalDBSteward.NewDatabase
	oldDoc := lib.GlobalDBSteward.OldDatabase

	// create languages that either do not exist in the old schema or have changed
	for _, newLang := range newDoc.Languages {
		oldLang := oldDoc.TryGetLanguageNamed(newLang.Name)
		if oldLang == nil || !oldLang.Equals(newLang) {
			ofs.WriteSql(getCreateLanguageSql(newLang)...)
		}
	}
}
