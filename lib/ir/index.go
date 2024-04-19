package ir

import (
	"fmt"
	"strings"
)

type IndexType string

const (
	IndexTypeBtree IndexType = "btree"
	IndexTypeHash  IndexType = "hash"
	IndexTypeGin   IndexType = "gin"
	IndexTypeGist  IndexType = "gist"
)

func NewIndexType(s string) (IndexType, error) {
	v := IndexType(s)
	if v.Equals(IndexTypeBtree) {
		return IndexTypeBtree, nil
	}
	if v.Equals(IndexTypeHash) {
		return IndexTypeHash, nil
	}
	if v.Equals(IndexTypeGin) {
		return IndexTypeGin, nil
	}
	if v.Equals(IndexTypeGist) {
		return IndexTypeGist, nil
	}
	return "", fmt.Errorf("invalid index type '%s'", s)
}

func (it IndexType) Equals(other IndexType) bool {
	return strings.EqualFold(string(it), string(other))
}

type Index struct {
	Name         string
	Using        IndexType
	Unique       bool
	Concurrently bool
	Dimensions   []*IndexDim
	Conditions   []*IndexCond
}

type IndexDim struct {
	Name  string // TODO(go,4) why does a dimension have a name? just for compositing/differencing's sake?
	Sql   bool
	Value string
}
type IndexCond struct {
	SqlFormat SqlFormat
	Condition string
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

func (self *Index) TryGetCondition(sqlFormat SqlFormat) *IndexCond {
	// TODO(go,core) fallback to returning empty sqlformat condition if it exists
	for _, cond := range self.Conditions {
		if cond.SqlFormat.Equals(sqlFormat) {
			return cond
		}
	}
	return nil
}

func (self *Index) IdentityMatches(other *Index) bool {
	if other == nil {
		return false
	}
	return strings.EqualFold(self.Name, other.Name)
}

func (self *Index) Equals(other *Index, sqlFormat SqlFormat) bool {
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
	if !self.Using.Equals(other.Using) {
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

func (self *Index) Validate(*Definition, *Schema, *Table) []error {
	// TODO(go,3) validate values
	return nil
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
	return self.SqlFormat.Equals(other.SqlFormat) && strings.TrimSpace(self.Condition) == strings.TrimSpace(other.Condition)
}
