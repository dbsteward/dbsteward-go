package sql

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

type SequenceAlterParts struct {
	Sequence SequenceRef
	Parts    []SequenceAlterPart
}

type SequenceAlterPart interface {
	GetSequenceAlterPartSql(q output.Quoter) string
}

func (self *SequenceAlterParts) ToSql(q output.Quoter) string {
	parts := ""
	for _, part := range self.Parts {
		partSql := part.GetSequenceAlterPartSql(q)
		if partSql == "" {
			continue
		}
		parts += "\n  " + partSql
	}
	if parts == "" {
		return ""
	}
	return fmt.Sprintf("ALTER SEQUENCE %s%s;", self.Sequence.Qualified(q), parts)
}

type SequenceAlterPartIncrement struct {
	Value util.Opt[int]
}

func (self *SequenceAlterPartIncrement) GetSequenceAlterPartSql(q output.Quoter) string {
	// 1 is default increment. if we're altering and omitting, that means to go back to default
	return fmt.Sprintf("INCREMENT BY %d", self.Value.GetOr(1))
}

type SequenceAlterPartMinValue struct {
	Value util.Opt[int]
}

func (self *SequenceAlterPartMinValue) GetSequenceAlterPartSql(q output.Quoter) string {
	if val, ok := self.Value.Maybe(); ok {
		return fmt.Sprintf("MINVALUE %d", val)
	}
	return "NO MINVALUE"
}

type SequenceAlterPartMaxValue struct {
	Value util.Opt[int]
}

func (self *SequenceAlterPartMaxValue) GetSequenceAlterPartSql(q output.Quoter) string {
	if val, ok := self.Value.Maybe(); ok {
		return fmt.Sprintf("MAXVALUE %d", val)
	}
	return "NO MAXVALUE"
}

type SequenceAlterPartCache struct {
	Value util.Opt[int]
}

func (self *SequenceAlterPartCache) GetSequenceAlterPartSql(q output.Quoter) string {
	// 1 is default cache. if we're altering and omitting, that means to go back to default
	return fmt.Sprintf("CACHE %d", self.Value.GetOr(1))
}

type SequenceAlterPartRestartWith struct {
	Value int
}

func (self *SequenceAlterPartRestartWith) GetSequenceAlterPartSql(q output.Quoter) string {
	return fmt.Sprintf("RESTART WITH %d", self.Value)
}

type SequenceAlterPartCycle struct {
	Value bool
}

func (self *SequenceAlterPartCycle) GetSequenceAlterPartSql(q output.Quoter) string {
	if self.Value {
		return "CYCLE"
	}
	return "NO CYCLE"
}
