package sql99

import (
	"fmt"
	"os"
	"time"

	"github.com/dbsteward/dbsteward/lib"

	"github.com/dbsteward/dbsteward/lib/format"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type Diff struct {
	format.Diff

	lookup *format.Lookup
}

func NewDiff(lookup *format.Lookup) *Diff {
	return &Diff{
		lookup: lookup,
	}
}

func (self *Diff) DiffDoc(oldFile, newFile string, oldDoc, newDoc *model.Definition, upgradePrefix string) {
	dbsteward := lib.GlobalDBSteward
	timestamp := time.Now().Format(time.RFC1123Z)
	oldSetNewSet := fmt.Sprintf("-- Old definition: %s\n-- New definition %s\n", oldFile, newFile)

	var stage1, stage2, stage3, stage4 output.OutputFileSegmenter

	if dbsteward.SingleStageUpgrade {
		fileName := upgradePrefix + "_single_stage.sql"
		file, err := os.Create(fileName)
		dbsteward.FatalIfError(err, "failed to open single stage output file %s for write", fileName)

		stage1 = output.NewOutputFileSegmenterToFile(dbsteward, self.lookup.Operations, fileName, 1, file, fileName, dbsteward.OutputFileStatementLimit)
		stage1.SetHeader("-- DBsteward single stage upgrade changes - generated %s\n%s", timestamp, oldSetNewSet)
		defer stage1.Close()
		stage2 = stage1
		stage3 = stage1
		stage4 = stage1
	} else {
		stage1 = output.NewOutputFileSegmenter(dbsteward, self.lookup.Operations, upgradePrefix+"_stage1_schema", 1, dbsteward.OutputFileStatementLimit)
		stage1.SetHeader("-- DBSteward stage 1 structure additions and modifications - generated %s\n%s", timestamp, oldSetNewSet)
		defer stage1.Close()
		stage2 = output.NewOutputFileSegmenter(dbsteward, self.lookup.Operations, upgradePrefix+"_stage2_data", 1, dbsteward.OutputFileStatementLimit)
		stage2.SetHeader("-- DBSteward stage 2 data definitions removed - generated %s\n%s", timestamp, oldSetNewSet)
		defer stage2.Close()
		stage3 = output.NewOutputFileSegmenter(dbsteward, self.lookup.Operations, upgradePrefix+"_stage3_schema", 1, dbsteward.OutputFileStatementLimit)
		stage3.SetHeader("-- DBSteward stage 3 structure changes, constraints, and removals - generated %s\n%s", timestamp, oldSetNewSet)
		defer stage3.Close()
		stage4 = output.NewOutputFileSegmenter(dbsteward, self.lookup.Operations, upgradePrefix+"_stage4_data", 1, dbsteward.OutputFileStatementLimit)
		stage4.SetHeader("-- DBSteward stage 4 data definition changes and additions - generated %s\n%s", timestamp, oldSetNewSet)
		defer stage4.Close()
	}

	dbsteward.OldDatabase = oldDoc
	dbsteward.NewDatabase = newDoc

	self.DiffDocWork(stage1, stage2, stage3, stage4)
}

func (self *Diff) DropOldSchemas(ofs output.OutputFileSegmenter) {
	// TODO(feat) support oldname following?
	for _, oldSchema := range lib.GlobalDBSteward.OldDatabase.Schemas {
		if lib.GlobalDBSteward.NewDatabase.TryGetSchemaNamed(oldSchema.Name) == nil {
			lib.GlobalDBSteward.Info("Drop old schema: %s", oldSchema.Name)
			// TODO(go,slony) GlobalOperations.SetContextReplicaSetId(oldSchema.SlonySetId)
			ofs.WriteSql(self.lookup.Schema.GetDropSql(oldSchema)...)
		}
	}
}

func (self *Diff) CreateNewSchemas(ofs output.OutputFileSegmenter) {
	// TODO(feat) support oldname following?
	for _, newSchema := range lib.GlobalDBSteward.NewDatabase.Schemas {
		if lib.GlobalDBSteward.OldDatabase.TryGetSchemaNamed(newSchema.Name) == nil {
			lib.GlobalDBSteward.Info("Create new schema: %s", newSchema.Name)
			// TODO(go,slony) GlobalOperations.SetContextReplicaSetId(newSchema.SlonySetId)
			ofs.WriteSql(self.lookup.Schema.GetCreationSql(newSchema)...)
		}
	}
}
