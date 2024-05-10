package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/output"
)

func diffLanguages(conf lib.Config, ofs output.OutputFileSegmenter) error {
	// TODO(go,pgsql) this is a different flow than old dbsteward:
	// we do equality comparison inside these two methods, instead of a separate loop
	// need to validate that this behavior is still correct

	dropLanguages(conf, ofs)
	return createLanguages(conf, ofs)
}

func dropLanguages(conf lib.Config, ofs output.OutputFileSegmenter) {
	newDoc := conf.NewDatabase
	oldDoc := conf.OldDatabase

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

func createLanguages(conf lib.Config, ofs output.OutputFileSegmenter) error {
	newDoc := conf.NewDatabase
	oldDoc := conf.OldDatabase

	// create languages that either do not exist in the old schema or have changed
	for _, newLang := range newDoc.Languages {
		oldLang := oldDoc.TryGetLanguageNamed(newLang.Name)
		if oldLang == nil || !oldLang.Equals(newLang) {
			s, err := getCreateLanguageSql(conf, newLang)
			if err != nil {
				return err
			}
			ofs.WriteSql(s...)
		}
	}
	return nil
}
