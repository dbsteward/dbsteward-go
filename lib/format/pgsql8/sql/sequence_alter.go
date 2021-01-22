package sql

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/output"
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
		if parts != "" {
			parts += ","
		}
		parts += "\n  " + partSql
	}
	if parts == "" {
		return ""
	}
	return fmt.Sprintf("ALTER SEQUENCE %s%s;", self.Sequence.Qualified(q), parts)
}

type SequenceAlterPartIncrement struct {
	Value *int
}

func (self *SequenceAlterPartIncrement) GetSequenceAlterPartSql(q output.Quoter) string {
	inc := 1 // 1 is default increment. if we're altering and omitting, that means to go back to default
	if self.Value != nil {
		inc = *self.Value
	}
	return fmt.Sprintf("INCREMENT BY %d", inc)
}

type SequenceAlterPartMinValue struct {
	Value *int
}

func (self *SequenceAlterPartMinValue) GetSequenceAlterPartSql(q output.Quoter) string {
	if self.Value == nil {
		return "NO MINVALUE"
	}
	return fmt.Sprintf("MINVALUE %d", *self.Value)
}

type SequenceAlterPartMaxValue struct {
	Value *int
}

func (self *SequenceAlterPartMaxValue) GetSequenceAlterPartSql(q output.Quoter) string {
	if self.Value == nil {
		return "NO MAXVALUE"
	}
	return fmt.Sprintf("MAXVALUE %d", *self.Value)
}

type SequenceAlterPartCache struct {
	Value *int
}

func (self *SequenceAlterPartCache) GetSequenceAlterPartSql(q output.Quoter) string {
	cache := 1 // 1 is default cache. if we're altering and omitting, that means to go back to default
	if self.Value != nil {
		cache = *self.Value
	}
	return fmt.Sprintf("CACHE %d", cache)
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
