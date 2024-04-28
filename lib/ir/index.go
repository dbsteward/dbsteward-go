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

func (idx *Index) AddCondition(f SqlFormat, c string) {
	idx.Conditions = append(
		idx.Conditions,
		&IndexCond{
			SqlFormat: f,
			Condition: c,
		},
	)
}

func (idx *Index) AddDimensionNamed(name, value string) {
	// TODO(feat) sanity check
	idx.Dimensions = append(idx.Dimensions, &IndexDim{
		Name:  name,
		Value: value,
	})
}

func (idx *Index) AddDimension(value string) {
	idx.AddDimensionNamed(
		fmt.Sprintf("%s_%d", idx.Name, len(idx.Dimensions)+1),
		value,
	)
}

func (idx *Index) TryGetCondition(sqlFormat SqlFormat) *IndexCond {
	// TODO(go,core) fallback to returning empty sqlformat condition if it exists
	for _, cond := range idx.Conditions {
		if cond.SqlFormat.Equals(sqlFormat) {
			return cond
		}
	}
	return nil
}

func (idx *Index) IdentityMatches(other *Index) bool {
	if other == nil {
		return false
	}
	return strings.EqualFold(idx.Name, other.Name)
}

func (idx *Index) Equals(other *Index, sqlFormat SqlFormat) bool {
	if idx == nil || other == nil {
		// nil != nil in this case
		return false
	}
	if !strings.EqualFold(idx.Name, other.Name) {
		return false
	}
	if idx.Unique != other.Unique {
		return false
	}
	if idx.Concurrently != other.Concurrently {
		return false
	}
	if !idx.Using.Equals(other.Using) {
		return false
	}
	if len(idx.Dimensions) != len(other.Dimensions) {
		return false
	}

	// dimension order matters
	for i, dim := range idx.Dimensions {
		if !dim.Equals(other.Dimensions[i]) {
			return false
		}
	}

	// if any conditions are defined, there must be a condition for the requested sqlFormat, and the two must be textually equal
	if len(idx.Conditions) > 0 || len(other.Conditions) > 0 {
		if idx.TryGetCondition(sqlFormat).Equals(other.TryGetCondition(sqlFormat)) {
			return false
		}
	}

	return true
}

func (idx *Index) Merge(overlay *Index) {
	if overlay == nil {
		return
	}
	idx.Using = overlay.Using
	idx.Unique = overlay.Unique
	idx.Dimensions = overlay.Dimensions
}

func (idx *Index) Validate(*Definition, *Schema, *Table) []error {
	// TODO(go,3) validate values
	return nil
}

func (idx *IndexDim) Equals(other *IndexDim) bool {
	if idx == nil || other == nil {
		return false
	}

	// name does _not_ matter for equality - it's a dbsteward concept
	return idx.Value == other.Value
}

func (idx *IndexCond) Equals(other *IndexCond) bool {
	if idx == nil || other == nil {
		return false
	}
	return idx.SqlFormat.Equals(other.SqlFormat) && strings.TrimSpace(idx.Condition) == strings.TrimSpace(other.Condition)
}
