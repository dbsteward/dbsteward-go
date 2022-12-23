package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
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

		if !oldSeq.Increment.Equals(newSeq.Increment) {
			parts = append(parts, &sql.SequenceAlterPartIncrement{newSeq.Increment})
		}
		if !oldSeq.Min.Equals(newSeq.Min) {
			parts = append(parts, &sql.SequenceAlterPartMinValue{newSeq.Min})
		}
		if !oldSeq.Max.Equals(newSeq.Max) {
			parts = append(parts, &sql.SequenceAlterPartMaxValue{newSeq.Max})
		}
		if !oldSeq.Cache.Equals(newSeq.Cache) {
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
