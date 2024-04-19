package xml

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib/ir"
)

type Table struct {
	Name           string          `xml:"name,attr"`
	Description    string          `xml:"description,attr,omitempty"`
	Owner          string          `xml:"owner,attr,omitempty"`
	PrimaryKey     DelimitedList   `xml:"primaryKey,attr,omitempty"`
	PrimaryKeyName string          `xml:"primaryKeyName,attr,omitempty"`
	ClusterIndex   string          `xml:"clusterIndex,attr,omitempty"`
	InheritsTable  string          `xml:"inheritsTable,attr,omitempty"`
	InheritsSchema string          `xml:"inheritsSchema,attr,omitempty"`
	OldTableName   string          `xml:"oldTableName,attr,omitempty"`
	OldSchemaName  string          `xml:"oldSchemaName,attr,omitempty"`
	SlonySetId     *int            `xml:"slonySetId,attr,omitempty"`
	SlonyId        *int            `xml:"slonyId,attr,omitempty"`
	TableOptions   []*TableOption  `xml:"tableOption"`
	Partitioning   *TablePartition `xml:"tablePartition"`
	Columns        []*Column       `xml:"column"`
	ForeignKeys    []*ForeignKey   `xml:"foreignKey"`
	Indexes        []*Index        `xml:"index"`
	Constraints    []*Constraint   `xml:"constraint"`
	Grants         []*Grant        `xml:"grant"`
	Rows           *DataRows       `xml:"rows"`
}

type TableOption struct {
	SqlFormat string `xml:"sqlFormat,attr"`
	Name      string `xml:"name"`
	Value     string `xml:"value"`
}

func (topt TableOption) ToIR() (*ir.TableOption, error) {
	sqlFormat, err := ir.NewSqlFormat(topt.SqlFormat)
	if err != nil {
		return nil, err
	}
	return &ir.TableOption{
		SqlFormat: sqlFormat,
		Name:      topt.Name,
		Value:     topt.Value,
	}, nil
}

func (table *Table) ToIR() (*ir.Table, error) {
	m := ir.Table{
		Name:           table.Name,
		Description:    table.Description,
		Owner:          table.Owner,
		PrimaryKey:     []string(table.PrimaryKey),
		PrimaryKeyName: table.PrimaryKeyName,
		ClusterIndex:   table.ClusterIndex,
		InheritsTable:  table.InheritsTable,
		InheritsSchema: table.InheritsSchema,
		OldTableName:   table.OldTableName,
		OldSchemaName:  table.OldSchemaName,
	}
	for _, to := range table.TableOptions {
		n, err := to.ToIR()
		if err != nil {
			return nil, fmt.Errorf("table '%s' invalid: %w", table.Name, err)
		}
		m.TableOptions = append(m.TableOptions, n)
	}
	tp, err := table.Partitioning.ToIR()
	if err != nil {
		return nil, fmt.Errorf("table '%s' invalid: %w", table.Name, err)
	}
	m.Partitioning = tp
	for _, col := range table.Columns {
		nCol, err := col.ToIR()
		if err != nil {
			return nil, fmt.Errorf("table '%s' invalid: %w", table.Name, err)
		}
		m.Columns = append(m.Columns, nCol)
	}
	for _, fk := range table.ForeignKeys {
		nFK, err := fk.ToIR()
		if err != nil {
			return nil, fmt.Errorf("table '%s' invalid: %w", table.Name, err)
		}
		m.ForeignKeys = append(m.ForeignKeys, nFK)
	}
	for _, idx := range table.Indexes {
		nIdx, err := idx.ToIR()
		if err != nil {
			return nil, fmt.Errorf("table '%s' invalid: %w", table.Name, err)
		}
		m.Indexes = append(m.Indexes, nIdx)
	}
	for _, c := range table.Constraints {
		nc, err := c.ToIR()
		if err != nil {
			return nil, fmt.Errorf("table '%s' invalid: %w", table.Name, err)
		}
		m.Constraints = append(m.Constraints, nc)
	}
	for _, g := range table.Grants {
		ng, err := g.ToIR()
		if err != nil {
			return nil, fmt.Errorf("table '%s' invalid: %w", table.Name, err)
		}
		m.Grants = append(m.Grants, ng)
	}
	m.Rows, err = table.Rows.ToIR()
	if err != nil {
		return nil, fmt.Errorf("table '%s' invalid: %w", table.Name, err)
	}
	return &m, nil
}
