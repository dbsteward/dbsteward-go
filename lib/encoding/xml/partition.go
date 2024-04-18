package xml

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/model"
)

type TablePartition struct {
	Type      string                   `xml:"type,attr"`
	SqlFormat string                   `xml:"sqlFormat,attr,omitempty"`
	Options   []*TablePartitionOption  `xml:"tablePartitionOption"`
	Segments  []*TablePartitionSegment `xml:"tablePartitionSegment"`
}

func (tp *TablePartition) ToModel() (*model.TablePartition, error) {
	if tp == nil {
		return nil, nil
	}
	rv := model.TablePartition{}
	var err error
	rv.Type, err = model.NewTablePartitionType(tp.Type)
	if err != nil {
		return nil, fmt.Errorf("inavalid table partition: %w", err)
	}
	rv.SqlFormat, err = model.NewSqlFormat(tp.SqlFormat)
	if err != nil {
		return nil, fmt.Errorf("inavalid table partition: %w", err)
	}
	for _, opt := range tp.Options {
		rv.Options = append(rv.Options, opt.ToModel())
	}
	for _, seg := range tp.Segments {
		rv.Segments = append(rv.Segments, seg.ToModel())
	}
	return &rv, nil
}

type TablePartitionOption struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

func (tpo *TablePartitionOption) ToModel() *model.TablePartitionOption {
	return &model.TablePartitionOption{
		Name:  tpo.Name,
		Value: tpo.Value,
	}
}

type TablePartitionSegment struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

func (seg *TablePartitionSegment) ToModel() *model.TablePartitionSegment {
	return &model.TablePartitionSegment{
		Name:  seg.Name,
		Value: seg.Value,
	}
}
