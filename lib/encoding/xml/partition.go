package xml

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/ir"
)

type TablePartition struct {
	Type      string                   `xml:"type,attr"`
	SqlFormat string                   `xml:"sqlFormat,attr,omitempty"`
	Options   []*TablePartitionOption  `xml:"tablePartitionOption"`
	Segments  []*TablePartitionSegment `xml:"tablePartitionSegment"`
}

func (tp *TablePartition) ToIR() (*ir.TablePartition, error) {
	if tp == nil {
		return nil, nil
	}
	rv := ir.TablePartition{}
	var err error
	rv.Type, err = ir.NewTablePartitionType(tp.Type)
	if err != nil {
		return nil, fmt.Errorf("inavalid table partition: %w", err)
	}
	rv.SqlFormat, err = ir.NewSqlFormat(tp.SqlFormat)
	if err != nil {
		return nil, fmt.Errorf("inavalid table partition: %w", err)
	}
	for _, opt := range tp.Options {
		rv.Options = append(rv.Options, opt.ToIR())
	}
	for _, seg := range tp.Segments {
		rv.Segments = append(rv.Segments, seg.ToIR())
	}
	return &rv, nil
}

type TablePartitionOption struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

func (tpo *TablePartitionOption) ToIR() *ir.TablePartitionOption {
	return &ir.TablePartitionOption{
		Name:  tpo.Name,
		Value: tpo.Value,
	}
}

type TablePartitionSegment struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

func (seg *TablePartitionSegment) ToIR() *ir.TablePartitionSegment {
	return &ir.TablePartitionSegment{
		Name:  seg.Name,
		Value: seg.Value,
	}
}
