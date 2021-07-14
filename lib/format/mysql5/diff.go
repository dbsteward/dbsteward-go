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

	// TODO(go,nth) document why mysql revokes and updates permissions but pgsql doesn't
	//              or: why does pgsql never revoke?

	dbsteward.Info("Revoke Permissions")
	self.revokePermissions(stage1)

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

func (self *Diff) revokePermissions(stage1 output.OutputFileSegmenter) {
	// TODO(feat) do granular diffing instead of wholesale revoke+regrant
	// TODO(feat) what if objects were renamed
	oldDoc := lib.GlobalDBSteward.OldDatabase
	for _, oldSchema := range oldDoc.Schemas {
		for _, oldGrant := range oldSchema.Grants {
			stage1.WriteSql(GlobalSchema.GetRevokeSql(oldDoc, oldSchema, oldGrant)...)
		}
		for _, oldTable := range oldSchema.Tables {
			for _, oldGrant := range oldSchema.Grants {
				stage1.WriteSql(GlobalTable.GetRevokeSql(oldDoc, oldSchema, oldTable, oldGrant)...)
			}
		}
		stage1.WriteSql(GlobalSequence.GetMultiRevokeSql(oldDoc, oldSchema, oldSchema.Sequences)...)
		for _, oldFunc := range oldSchema.Functions {
			for _, oldGrant := range oldFunc.Grants {
				stage1.WriteSql(GlobalFunction.GetRevokeSql(oldDoc, oldSchema, oldFunc, oldGrant)...)
			}
		}
		for _, oldView := range oldSchema.Views {
			for _, oldGrant := range oldView.Grants {
				stage1.WriteSql(GlobalView.GetRevokeSql(oldDoc, oldSchema, oldView, oldGrant)...)
			}
		}
	}
}

func (self *Diff) updateStructure(stage1, stage3 output.OutputFileSegmenter) {
	// TODO(go,mysql) implement me
}

func (self *Diff) updatePermissions(stage1, stage3 output.OutputFileSegmenter) {
	// TODO(feat) do granular diffing instead of wholesale revoke+regrant
	// TODO(feat) what if objects were renamed
	newDoc := lib.GlobalDBSteward.NewDatabase
	for _, newSchema := range newDoc.Schemas {
		for _, newGrant := range newSchema.Grants {
			stage1.WriteSql(GlobalSchema.GetGrantSql(newDoc, newSchema, newGrant)...)
		}
		for _, newTable := range newSchema.Tables {
			for _, newGrant := range newSchema.Grants {
				stage1.WriteSql(GlobalTable.GetGrantSql(newDoc, newSchema, newTable, newGrant)...)
			}
		}
		stage1.WriteSql(GlobalSequence.GetMultiGrantSql(newDoc, newSchema, newSchema.Sequences)...)
		for _, newFunc := range newSchema.Functions {
			for _, newGrant := range newFunc.Grants {
				stage1.WriteSql(GlobalFunction.GetGrantSql(newDoc, newSchema, newFunc, newGrant)...)
			}
		}
		for _, newView := range newSchema.Views {
			for _, newGrant := range newView.Grants {
				stage1.WriteSql(GlobalView.GetGrantSql(newDoc, newSchema, newView, newGrant)...)
			}
		}
	}
}

func (self *Diff) updateData(ofs output.OutputFileSegmenter, deleteMode bool) {
	// TODO(go,mysql) implement me
}
