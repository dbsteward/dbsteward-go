package xml

import (
	"fmt"
	"log/slog"
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

func IndexesFromIR(l *slog.Logger, idxs []*ir.Index) ([]*Index, error) {
	if len(idxs) == 0 {
		return nil, nil
	}
	var rv []*Index
	for _, idx := range idxs {
		if idx != nil {
			rv = append(rv, IndexFromIR(l, idx))
		}
	}
	return rv, nil
}

func IndexFromIR(l *slog.Logger, idx *ir.Index) *Index {
	l = l.With(slog.String("index", idx.Name))
	l.Debug("converting index")
	defer l.Debug("done converting index")
	return &Index{
		Name:         idx.Name,
		Using:        string(idx.Using),
		Unique:       idx.Unique,
		Concurrently: idx.Concurrently,
		Dimensions:   IndexDimensionsFromIR(l, idx.Dimensions),
		Conditions:   IndexConditionsFromIR(l, idx.Conditions),
	}
}

func IndexDimensionsFromIR(l *slog.Logger, dims []*ir.IndexDim) []*IndexDim {
	if len(dims) == 0 {
		return nil
	}
	var rv []*IndexDim
	for _, dim := range dims {
		if dim != nil {
			rv = append(
				rv,
				&IndexDim{
					Name:  dim.Name,
					Sql:   dim.Sql,
					Value: dim.Value,
				},
			)
		}
	}
	return rv
}

func IndexConditionsFromIR(l *slog.Logger, conds []*ir.IndexCond) []*IndexCond {
	if len(conds) == 0 {
		return nil
	}
	var rv []*IndexCond
	for _, cond := range conds {
		if cond != nil {
			rv = append(
				rv,
				&IndexCond{
					SqlFormat: string(cond.SqlFormat),
					Condition: cond.Condition,
				},
			)
		}
	}
	return rv
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

func (idx *Index) TryGetCondition(sqlFormat string) util.Opt[*IndexCond] {
	// TODO(go,core) fallback to returning empty sqlformat condition if it exists
	return util.Find(idx.Conditions, func(c *IndexCond) bool {
		return strings.EqualFold(c.SqlFormat, sqlFormat)
	})
}

func (idx *Index) IdentityMatches(other *Index) bool {
	if other == nil {
		return false
	}
	return strings.EqualFold(idx.Name, other.Name)
}

func (idx *Index) Equals(other *Index, sqlFormat string) bool {
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
	if !strings.EqualFold(idx.Using, other.Using) {
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

func (idxd *IndexDim) Equals(other *IndexDim) bool {
	if idxd == nil || other == nil {
		return false
	}

	// name does _not_ matter for equality - it's a dbsteward concept
	return idxd.Value == other.Value
}

func (idxc *IndexCond) Equals(other *IndexCond) bool {
	if idxc == nil || other == nil {
		return false
	}
	return strings.EqualFold(idxc.SqlFormat, other.SqlFormat) &&
		strings.TrimSpace(idxc.Condition) == strings.TrimSpace(other.Condition)
}
