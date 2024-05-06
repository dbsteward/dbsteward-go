package pgsql8

import (
	"log/slog"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/output"
)

func diffLanguages(l *slog.Logger, ofs output.OutputFileSegmenter) error {
	// TODO(go,pgsql) this is a different flow than old dbsteward:
	// we do equality comparison inside these two methods, instead of a separate loop
	// need to validate that this behavior is still correct

	dropLanguages(ofs)
	return createLanguages(l, ofs)
}

func dropLanguages(ofs output.OutputFileSegmenter) {
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

func createLanguages(l *slog.Logger, ofs output.OutputFileSegmenter) error {
	newDoc := lib.GlobalDBSteward.NewDatabase
	oldDoc := lib.GlobalDBSteward.OldDatabase

	// create languages that either do not exist in the old schema or have changed
	for _, newLang := range newDoc.Languages {
		oldLang := oldDoc.TryGetLanguageNamed(newLang.Name)
		if oldLang == nil || !oldLang.Equals(newLang) {
			s, err := getCreateLanguageSql(l, newLang)
			if err != nil {
				return err
			}
			ofs.WriteSql(s...)
		}
	}
	return nil
}
