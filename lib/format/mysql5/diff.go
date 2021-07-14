package mysql5

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/sql99"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Diff struct {
	*sql99.Diff
	OldTableDependency []*model.TableRef
	NewTableDependency []*model.TableRef
}

func NewDiff() *Diff {
	diff := &Diff{
		Diff: sql99.NewDiff(GlobalLookup),
	}
	diff.Diff.Diff = diff
	return diff
}

// DiffDoc implemented by sql99.Diff

func (self *Diff) DiffDocWork(stage1, stage2, stage3, stage4 output.OutputFileSegmenter) {
	dbsteward := lib.GlobalDBSteward
	dbx := lib.GlobalDBX

	// start with pre-upgrade sql statements that prepare the database to take on its changes
	dbx.BuildStagedSql(dbsteward.NewDatabase, stage1, "STAGE1BEFORE")
	dbx.BuildStagedSql(dbsteward.NewDatabase, stage2, "STAGE2BEFORE")

	dbsteward.Info("Revoke Permissions")
	self.revokePermissions(stage1, stage3)

	dbsteward.Info("Update Structure")
	self.updateStructure(stage1, stage3)

	dbsteward.Info("Update Permissions")
	self.updatePermissions(stage1, stage3)

	dbsteward.Info("Update Data")
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

	dbx.BuildStagedSql(dbsteward.NewDatabase, stage1, "STAGE1")
	dbx.BuildStagedSql(dbsteward.NewDatabase, stage2, "STAGE2")
	dbx.BuildStagedSql(dbsteward.NewDatabase, stage3, "STAGE3")
	dbx.BuildStagedSql(dbsteward.NewDatabase, stage4, "STAGE4")
}

func (self *Diff) revokePermissions(stage1, stage3 output.OutputFileSegmenter) {
	// TODO(go,mysql) implement me
}

func (self *Diff) updateStructure(stage1, stage3 output.OutputFileSegmenter) {
	// TODO(go,mysql) implement me
}

func (self *Diff) updatePermissions(stage1, stage3 output.OutputFileSegmenter) {
	// TODO(go,mysql) implement me
}

func (self *Diff) updateData(ofs output.OutputFileSegmenter, deleteMode bool) {
	// TODO(go,mysql) implement me
}
