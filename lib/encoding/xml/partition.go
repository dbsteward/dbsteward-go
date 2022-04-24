package xml

type TablePartition struct {
	Type      string                   `xml:"type,attr"`
	SqlFormat string                   `xml:"sqlFormat,attr,omitempty"`
	Options   []*TablePartitionOption  `xml:"tablePartitionOption"`
	Segments  []*TablePartitionSegment `xml:"tablePartitionSegment"`
}

type TablePartitionOption struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

type TablePartitionSegment struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}
