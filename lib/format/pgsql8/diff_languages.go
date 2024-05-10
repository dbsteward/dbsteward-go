package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/output"
)

func diffLanguages(dbs *lib.DBSteward, ofs output.OutputFileSegmenter) error {
	// TODO(go,pgsql) this is a different flow than old dbsteward:
	// we do equality comparison inside these two methods, instead of a separate loop
	// need to validate that this behavior is still correct

	dropLanguages(dbs, ofs)
	return createLanguages(dbs, ofs)
}

func dropLanguages(dbs *lib.DBSteward, ofs output.OutputFileSegmenter) {
	newDoc := dbs.NewDatabase
	oldDoc := dbs.OldDatabase

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

func createLanguages(dbs *lib.DBSteward, ofs output.OutputFileSegmenter) error {
	newDoc := dbs.NewDatabase
	oldDoc := dbs.OldDatabase

	// create languages that either do not exist in the old schema or have changed
	for _, newLang := range newDoc.Languages {
		oldLang := oldDoc.TryGetLanguageNamed(newLang.Name)
		if oldLang == nil || !oldLang.Equals(newLang) {
			s, err := getCreateLanguageSql(dbs, newLang)
			if err != nil {
				return err
			}
			ofs.WriteSql(s...)
		}
	}
	return nil
}
