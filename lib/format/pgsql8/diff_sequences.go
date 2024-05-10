package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

func diffSequences(conf lib.Config, ofs output.OutputFileSegmenter, oldSchema *ir.Schema, newSchema *ir.Schema) error {
	// drop old sequences
	if oldSchema != nil {
		for _, oldSeq := range oldSchema.Sequences {
			if newSchema.TryGetSequenceNamed(oldSeq.Name) == nil {
				ofs.WriteSql(getDropSequenceSql(oldSchema, oldSeq)...)
			}
		}
	}
	// create new sequences, alter changed sequences
	for _, newSeq := range newSchema.Sequences {
		oldSeq := oldSchema.TryGetSequenceNamed(newSeq.Name)
		if oldSeq == nil {
			sql, err := getCreateSequenceSql(conf, newSchema, newSeq)
			if err != nil {
				return err
			}
			ofs.WriteSql(sql...)
		} else {
			ofs.WriteSql(getAlterSequenceSql(newSchema.Name, oldSeq, newSeq))
		}
	}
	return nil
}

func getAlterSequenceSql(newSchema string, oldSeq, newSeq *ir.Sequence) *sql.SequenceAlterParts {
	parts := []sql.SequenceAlterPart{}
	if !oldSeq.Increment.Equals(newSeq.Increment) {
		parts = append(parts, &sql.SequenceAlterPartIncrement{Value: newSeq.Increment})
	}
	if !oldSeq.Min.Equals(newSeq.Min) {
		parts = append(parts, &sql.SequenceAlterPartMinValue{Value: newSeq.Min})
	}
	if !oldSeq.Max.Equals(newSeq.Max) {
		parts = append(parts, &sql.SequenceAlterPartMaxValue{Value: newSeq.Max})
	}
	if !oldSeq.Cache.Equals(newSeq.Cache) {
		parts = append(parts, &sql.SequenceAlterPartCache{Value: newSeq.Cache})
	}
	if oldSeq.Cycle != newSeq.Cycle {
		parts = append(parts, &sql.SequenceAlterPartCycle{Value: newSeq.Cycle})
	}
	return &sql.SequenceAlterParts{
		Sequence: sql.SequenceRef{Schema: newSchema, Sequence: newSeq.Name},
		Parts:    parts,
	}
}
