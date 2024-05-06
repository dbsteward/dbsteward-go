package xml

import (
	"fmt"
	"log/slog"

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

func TablesFromIR(l *slog.Logger, irts []*ir.Table) ([]*Table, error) {
	if len(irts) == 0 {
		return nil, nil
	}
	l.Debug("start converting tables")
	defer l.Debug("done converting tables")
	if len(irts) == 0 {
		return nil, nil
	}
	var rv []*Table
	for _, irt := range irts {
		if irt != nil {
			t, err := TableFromIR(l, irt)
			if err != nil {
				return nil, err
			}
			rv = append(rv, t)
		}
	}
	return rv, nil
}

func TableFromIR(l *slog.Logger, irt *ir.Table) (*Table, error) {
	l = l.With(slog.String("table", irt.Name))
	l.Debug("start converting table")
	defer l.Debug("done converting table")
	t := Table{
		Name:           irt.Name,
		Description:    irt.Description,
		Owner:          irt.Owner,
		PrimaryKey:     irt.PrimaryKey,
		PrimaryKeyName: irt.PrimaryKeyName,
		ClusterIndex:   irt.ClusterIndex,
		InheritsTable:  irt.InheritsTable,
		InheritsSchema: irt.InheritsSchema,
		OldTableName:   irt.OldTableName,
		OldSchemaName:  irt.OldSchemaName,
		// SlonySetId: Does not appear in the IR
		// SlonyID: Does not appear in the IR
		TableOptions: TableOptionsFromIR(l, irt.TableOptions),
	}
	var err error
	t.Partitioning, err = TablePartitionFromIR(l, irt.Partitioning)
	if err != nil {
		return nil, err
	}
	t.Columns, err = ColumnsFromIR(l, irt.Columns)
	if err != nil {
		return nil, err
	}
	t.ForeignKeys, err = ForeignKeysFromIR(l, irt.ForeignKeys)
	if err != nil {
		return nil, err
	}
	t.Indexes, err = IndexesFromIR(l, irt.Indexes)
	if err != nil {
		return nil, err
	}
	t.Constraints, err = ConstraintsFromIR(l, irt.Constraints)
	if err != nil {
		return nil, err
	}
	t.Grants, err = GrantsFromIR(l, irt.Grants)
	if err != nil {
		return nil, err
	}
	t.Rows, err = DataRowsFromIR(l, irt.Rows)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func TableOptionsFromIR(l *slog.Logger, irops []*ir.TableOption) []*TableOption {
	if len(irops) == 0 {
		return nil
	}
	var rv []*TableOption
	for _, irop := range irops {
		if irop != nil {
			rv = append(
				rv,
				&TableOption{
					SqlFormat: string(irop.SqlFormat),
					Name:      irop.Name,
					Value:     irop.Value,
				},
			)
		}
	}
	return rv
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
