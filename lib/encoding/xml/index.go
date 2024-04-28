package xml

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/util"
)

type Index struct {
	Name         string       `xml:"name,attr,omitempty"`
	Using        string       `xml:"using,attr,omitempty"`
	Unique       bool         `xml:"unique,attr,omitempty"`
	Concurrently bool         `xml:"concurrently,attr,omitempty"`
	Dimensions   []*IndexDim  `xml:"indexDimension"`
	Conditions   []*IndexCond `xml:"indexWhere"`
}

type IndexDim struct {
	Name  string `xml:"name,attr"` // TODO(go,4) why does a dimension have a name? just for compositing/differencing's sake?
	Sql   bool   `xml:"sql,attr,omitempty"`
	Value string `xml:",chardata"`
}

func (id *IndexDim) ToIR() (*ir.IndexDim, error) {
	return &ir.IndexDim{
		Name:  id.Name,
		Sql:   id.Sql,
		Value: id.Value,
	}, nil
}

type IndexCond struct {
	SqlFormat string `xml:"sqlFormat,attr,omitempty"`
	Condition string `xml:",chardata"`
}

func (id *IndexCond) ToIR() (*ir.IndexCond, error) {
	rv := ir.IndexCond{
		Condition: id.Condition,
	}
	var err error
	rv.SqlFormat, err = ir.NewSqlFormat(id.SqlFormat)
	if err != nil {
		return nil, err
	}
	return &rv, nil
}

func (idx *Index) ToIR() (*ir.Index, error) {
	rv := ir.Index{
		Name:         idx.Name,
		Unique:       idx.Unique,
		Concurrently: idx.Concurrently,
	}
	var err error
	rv.Using, err = newIndexType(idx.Using)
	if err != nil {
		return nil, fmt.Errorf("index '%s' invalid: %s", idx.Name, err)
	}
	for _, d := range idx.Dimensions {
		nd, err := d.ToIR()
		if err != nil {
			return nil, fmt.Errorf("index '%s' invalid: %s", idx.Name, err)
		}
		rv.Dimensions = append(rv.Dimensions, nd)
	}
	for _, c := range idx.Conditions {
		nc, err := c.ToIR()
		if err != nil {
			return nil, fmt.Errorf("index '%s' invalid: %s", idx.Name, err)
		}
		rv.Conditions = append(rv.Conditions, nc)
	}
	return &rv, nil
}

func newIndexType(s string) (ir.IndexType, error) {
	v := ir.IndexType(s)
	if v.Equals(ir.IndexTypeBtree) {
		return ir.IndexTypeBtree, nil
	}
	if v.Equals(ir.IndexTypeHash) {
		return ir.IndexTypeHash, nil
	}
	if v.Equals(ir.IndexTypeGin) {
		return ir.IndexTypeGin, nil
	}
	if v.Equals(ir.IndexTypeGist) {
		return ir.IndexTypeGist, nil
	}
	return "", fmt.Errorf("invalid index type '%s'", s)
}

func (self *Index) AddDimensionNamed(name, value string) {
	// TODO(feat) sanity check
	self.Dimensions = append(self.Dimensions, &IndexDim{
		Name:  name,
		Value: value,
	})
}

func (self *Index) AddDimension(value string) {
	self.AddDimensionNamed(
		fmt.Sprintf("%s_%d", self.Name, len(self.Dimensions)+1),
		value,
	)
}

func (self *Index) TryGetCondition(sqlFormat string) util.Opt[*IndexCond] {
	// TODO(go,core) fallback to returning empty sqlformat condition if it exists
	return util.Find(self.Conditions, func(c *IndexCond) bool {
		return strings.EqualFold(c.SqlFormat, sqlFormat)
	})
}

func (self *Index) IdentityMatches(other *Index) bool {
	if other == nil {
		return false
	}
	return strings.EqualFold(self.Name, other.Name)
}

func (self *Index) Equals(other *Index, sqlFormat string) bool {
	if self == nil || other == nil {
		// nil != nil in this case
		return false
	}
	if !strings.EqualFold(self.Name, other.Name) {
		return false
	}
	if self.Unique != other.Unique {
		return false
	}
	if self.Concurrently != other.Concurrently {
		return false
	}
	if !strings.EqualFold(self.Using, other.Using) {
		return false
	}
	if len(self.Dimensions) != len(other.Dimensions) {
		return false
	}

	// dimension order matters
	for i, dim := range self.Dimensions {
		if !dim.Equals(other.Dimensions[i]) {
			return false
		}
	}

	// if any conditions are defined, there must be a condition for the requested sqlFormat, and the two must be textually equal
	if len(self.Conditions) > 0 || len(other.Conditions) > 0 {
		if self.TryGetCondition(sqlFormat).Equals(other.TryGetCondition(sqlFormat)) {
			return false
		}
	}

	return true
}

func (self *Index) Merge(overlay *Index) {
	if overlay == nil {
		return
	}
	self.Using = overlay.Using
	self.Unique = overlay.Unique
	self.Dimensions = overlay.Dimensions
}

func (self *IndexDim) Equals(other *IndexDim) bool {
	if self == nil || other == nil {
		return false
	}

	// name does _not_ matter for equality - it's a dbsteward concept
	return self.Value == other.Value
}

func (self *IndexCond) Equals(other *IndexCond) bool {
	if self == nil || other == nil {
		return false
	}
	return strings.EqualFold(self.SqlFormat, other.SqlFormat) &&
		strings.TrimSpace(self.Condition) == strings.TrimSpace(other.Condition)
}
