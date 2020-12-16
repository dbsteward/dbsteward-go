package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/sql99"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

var GlobalDiff *Diff = NewDiff()

type Diff struct {
	*sql99.Diff
	AsTransaction      bool
	OldTableDependency []*model.TableDepEntry
	NewTableDependency []*model.TableDepEntry
}

func NewDiff() *Diff {
	diff := &Diff{
		AsTransaction: true, // TODO(go,pgsql8) where does this get set from?
		Diff:          sql99.NewDiff(),
	}
	diff.Diff.Diff = diff
	return diff
}

func (self *Diff) UpdateDatabaseConfigParameters(ofs output.OutputFileSegmenter, oldDoc *model.Definition, newDoc *model.Definition) {
	// TODO(go,pgsql)
}

func (self *Diff) DiffDoc(oldFile, newFile string, oldDoc, newDoc *model.Definition, upgradePrefix string) {
	if !lib.GlobalDBSteward.GenerateSlonik {
		// if we are not generating slonik, defer to parent
		self.Diff.DiffDoc(oldFile, newFile, oldDoc, newDoc, upgradePrefix)
	}

	// TODO(go,slony)
}

func (self *Diff) DiffDocWork(stage1, stage2, stage3, stage4 output.OutputFileSegmenter) {
	dbsteward := lib.GlobalDBSteward
	dbx := lib.GlobalDBX

	// this shouldn't be called if we're not generating slonik, it looks for
	// a slony element in <database> which most likely won't be there if
	// we're not interested in slony replication
	if dbsteward.GenerateSlonik {
		// TODO(go,slony)
	}

	if self.AsTransaction {
		// stage 1 and 3 should not be in a transaction as they will be submitted via slonik EXECUTE SCRIPT
		if !dbsteward.GenerateSlonik {
			stage1.AppendHeader("\nBEGIN;\n\n")
			stage1.AppendFooter("\nCOMMIT;\n")
		} else {
			stage1.AppendHeader("\n-- generateslonik specified: pgsql8 STAGE1 upgrade omitting BEGIN. slonik EXECUTE SCRIPT will wrap stage 1 DDL and DCL in a transaction\n")
		}

		if !dbsteward.SingleStageUpgrade {
			stage2.AppendHeader("\nBEGIN;\n\n")
			stage2.AppendFooter("\nCOMMIT;\n")
			stage4.AppendHeader("\nBEGIN;\n\n")
			stage4.AppendFooter("\nCOMMIT;\n")

			// stage 1 and 3 should not be in a transaction as they will be submitted via slonik EXECUTE SCRIPT
			if !dbsteward.GenerateSlonik {
				stage3.AppendHeader("\nBEGIN;\n\n")
				stage3.AppendFooter("\nCOMMIT;\n")
			} else {
				stage3.AppendHeader("\n-- generateslonik specified: pgsql8 STAGE3 upgrade omitting BEGIN. slonik EXECUTE SCRIPT will wrap stage 3 DDL and DCL in a transaction\n")
			}
		}
	}

	// start with pre-upgrade sql statements that prepare the database to take on its changes
	dbx.BuildStagedSql(dbsteward.NewDatabase, stage1, "STAGE1BEFORE")
	dbx.BuildStagedSql(dbsteward.NewDatabase, stage2, "STAGE2BEFORE")

	dbsteward.Info("Drop Old Schemas")
	self.DropOldSchemas(stage3)

	dbsteward.Info("Create New Schemas")
	self.CreateNewSchemas(stage1)

	dbsteward.Info("Update Structure")
	self.updateStructure(stage1, stage3, self.NewTableDependency)

	dbsteward.Info("Update Permissions")
	self.updatePermissions(stage1, stage3)

	self.UpdateDatabaseConfigParameters(stage1, dbsteward.NewDatabase, dbsteward.OldDatabase)

	dbsteward.Info("Update data")
	if dbsteward.GenerateSlonik {
		// TODO(go,slony)
	}
	self.updateData(stage2, true)
	self.updateData(stage4, false)

	// append any literal sql in new not in old at the end of data stage 1
	// TODO(feat) this relies on exact string match - is there a better way?
	for _, newSql := range dbsteward.NewDatabase.Sql {
		// ignore upgrade staged sql elements
		if newSql.Stage != "" {
			continue
		}

		found := false
		for _, oldSql := range dbsteward.OldDatabase.Sql {
			// ignore upgrade staged sql elements
			if oldSql.Stage != "" {
				continue
			}
			if newSql.Text == oldSql.Text {
				found = true
				break
			}
		}

		if !found {
			// TODO(feat) need to ensure newSql ends with semicolon
			stage2.Write(newSql.Text + "\n")
		}
	}

	// append stage defined sql statements to appropriate stage file
	if dbsteward.GenerateSlonik {
		// TODO(go,slony)
	}

	dbx.BuildStagedSql(dbsteward.NewDatabase, stage1, "STAGE1")
	dbx.BuildStagedSql(dbsteward.NewDatabase, stage2, "STAGE2")
	dbx.BuildStagedSql(dbsteward.NewDatabase, stage3, "STAGE3")
	dbx.BuildStagedSql(dbsteward.NewDatabase, stage4, "STAGE4")
}

func (self *Diff) DiffSql(old, new []string, upgradePrefix string) {
	// TODO(go,sqldiff)
}

func (self *Diff) updateStructure(stage1 output.OutputFileSegmenter, stage3 output.OutputFileSegmenter, tableDepOrder []*model.TableDepEntry) {
	// TODO(go,pgsql8)
}
func (self *Diff) updatePermissions(stage1 output.OutputFileSegmenter, stage3 output.OutputFileSegmenter) {
	// TODO(go,pgsql8)
}
func (self *Diff) updateData(ofs output.OutputFileSegmenter, deleteMode bool) {
	// TODO(go,pgsql8)
}
