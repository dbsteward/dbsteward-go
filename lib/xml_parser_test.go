package lib_test

import (
	"testing"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/model"
)

func TestXmlParser_CompositeDoc_InheritedRows(t *testing.T) {
	parent := &model.Definition{
		Schemas: []*model.Schema{
			&model.Schema{
				Name: "public",
				Tables: []*model.Table{
					&model.Table{
						Name:       "parent_table",
						PrimaryKey: model.DelimitedList{"pk"},
						Columns: []*model.Column{
							{Name: "pk", Type: "int"},
							{Name: "col1", Type: "char(10)", Default: "yeahboy"},
						},
					},
				},
			},
		},
	}
	child := &model.Definition{
		Schemas: []*model.Schema{&model.Schema{
			Name: "public",
			Tables: []*model.Table{
				&model.Table{
					Name:           "child_table",
					PrimaryKey:     model.DelimitedList{"pkchild"},
					InheritsSchema: "public",
					InheritsTable:  "parent_table",
					Columns: []*model.Column{
						{Name: "pkchild", Type: "int"},
						{Name: "x", Type: "int"},
					},
					Rows: &model.DataRows{
						Columns: model.DelimitedList{"pkchild", "col1"},
						Rows: []*model.DataRow{
							&model.DataRow{
								Columns: []*model.DataCol{
									{Text: "99999999999999"},
									{Text: "techmology"},
								},
							},
						},
					},
				},
			},
		},
		},
	}
	parentAndChild := &model.Definition{
		Schemas: []*model.Schema{&model.Schema{
			Name: "public",
			Tables: []*model.Table{
				&model.Table{
					Name:       "parent_table",
					PrimaryKey: model.DelimitedList{"pk"},
					Columns: []*model.Column{
						{Name: "pk", Type: "int"},
						{Name: "col1", Type: "char(10)", Default: "yeahboy"},
					},
				},
				&model.Table{
					Name:           "child_table",
					PrimaryKey:     model.DelimitedList{"pkchild"},
					InheritsSchema: "public",
					InheritsTable:  "parent_table",
					Columns: []*model.Column{
						{Name: "pkchild", Type: "int"},
						{Name: "x", Type: "int"},
					},
					Rows: &model.DataRows{
						Columns: model.DelimitedList{"pkchild", "col1"},
						Rows: []*model.DataRow{
							&model.DataRow{
								Columns: []*model.DataCol{
									{Text: "99999999999999"},
									{Text: "techmology"},
								},
							},
						},
					},
				},
			},
		},
		},
	}
	grandchild := &model.Definition{
		Schemas: []*model.Schema{&model.Schema{
			Name: "notpublic",
			Tables: []*model.Table{
				&model.Table{
					Name:           "grandchild_table",
					PrimaryKey:     model.DelimitedList{"pkgrandchild"},
					InheritsSchema: "public",
					InheritsTable:  "child_table",
					Columns: []*model.Column{
						{Name: "pkgrandchild", Type: "int"},
						{Name: "y", Type: "int"},
					},
					Rows: &model.DataRows{
						Columns: model.DelimitedList{"pkgrandchild", "col1"},
						Rows: []*model.DataRow{
							&model.DataRow{
								Columns: []*model.DataCol{
									{Text: "99999999999999"},
									{Text: "techmology"},
								},
							},
						},
					},
				},
			},
		},
		},
	}

	xp := lib.NewXmlParser()

	// TODO(go,nth) v1 doesn't actually assert anything... should we?
	// AFAICT we just verify it doesn't blow up
	// TODO(go,3) really we're testing schema.Merge functionality... I think?
	_ = xp.CompositeDoc(parent, child, "", -1, nil)
	_ = xp.CompositeDoc(parentAndChild, grandchild, "", -1, nil)
}
