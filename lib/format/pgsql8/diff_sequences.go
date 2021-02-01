package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

type DiffSequences struct {
}

func NewDiffSequences() *DiffSequences {
	return &DiffSequences{}
}

func (self *DiffSequences) DiffSequences(ofs output.OutputFileSegmenter, oldSchema *model.Schema, newSchema *model.Schema) {
	// drop old sequences
	if oldSchema != nil {
		for _, oldSeq := range oldSchema.Sequences {
			if newSchema.TryGetSequenceNamed(oldSeq.Name) == nil {
				ofs.WriteSql(GlobalSequence.GetDropSql(oldSchema, oldSeq)...)
			}
		}
	}

	// create new sequences, alter changed sequences
	for _, newSeq := range newSchema.Sequences {
		oldSeq := oldSchema.TryGetSequenceNamed(newSeq.Name)
		if oldSeq == nil {
			ofs.WriteSql(GlobalSequence.GetCreationSql(newSchema, newSeq)...)
			continue
		}

		parts := []sql.SequenceAlterPart{}

		if !util.IntpEq(oldSeq.Increment, newSeq.Increment) {
			parts = append(parts, &sql.SequenceAlterPartIncrement{newSeq.Increment})
		}
		if !util.IntpEq(oldSeq.Min, newSeq.Min) {
			parts = append(parts, &sql.SequenceAlterPartMinValue{newSeq.Min})
		}
		if !util.IntpEq(oldSeq.Max, newSeq.Max) {
			parts = append(parts, &sql.SequenceAlterPartMaxValue{newSeq.Max})
		}
		if !util.IntpEq(oldSeq.Cache, newSeq.Cache) {
			parts = append(parts, &sql.SequenceAlterPartCache{newSeq.Cache})
		}
		if oldSeq.Cycle != newSeq.Cycle {
			parts = append(parts, &sql.SequenceAlterPartCycle{newSeq.Cycle})
		}

		ofs.WriteSql(&sql.SequenceAlterParts{
			Sequence: sql.SequenceRef{newSchema.Name, newSeq.Name},
			Parts:    parts,
		})
	}
}
