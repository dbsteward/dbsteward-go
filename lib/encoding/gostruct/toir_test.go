package gostruct

import (
	"log/slog"
	"reflect"
	"testing"

	"github.com/dbsteward/dbsteward/lib/ir"
)

func TestTableToIR(t *testing.T) {
	tests := []struct {
		name   string
		t      Table
		expect ir.Table
	}{
		{
			name: "simple",
			t: Table{
				Name:        "Table1",
				Description: "T1 Desc",
				Fields: struct {
					F1 int
					F2 *string
				}{},
			},
			expect: ir.Table{
				Name:        "Table1",
				Description: "T1 Desc",
				Columns: []*ir.Column{
					{
						Name:     "F1",
						Type:     "BIGINT",
						Nullable: false,
					},
					{
						Name:     "F2",
						Type:     "TEXT",
						Nullable: true,
					},
				},
			},
		},
		{
			name: "primary key",
			t: Table{
				Name:        "Table1",
				Description: "T1 Desc",
				Fields: struct {
					F1 int `dbsteward:",,PRIMARY KEY"`
					F2 *string
				}{},
			},
			expect: ir.Table{
				Name:        "Table1",
				Description: "T1 Desc",
				PrimaryKey:  []string{"F1"},
				Columns: []*ir.Column{
					{
						Name:     "F1",
						Type:     "BIGINT",
						Nullable: false,
					},
					{
						Name:     "F2",
						Type:     "TEXT",
						Nullable: true,
					},
				},
			},
		},
		{
			name: "skip unexported",
			t: Table{
				Name:        "Table1",
				Description: "T1 Desc",
				Fields: struct {
					F1 int
					f2 *string
				}{},
			},
			expect: ir.Table{
				Name:        "Table1",
				Description: "T1 Desc",
				Columns: []*ir.Column{
					{
						Name:     "F1",
						Type:     "BIGINT",
						Nullable: false,
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := test.t.toIR(slog.Default())
			if err != nil {
				t.Fatalf(err.Error())
			}
			if !reflect.DeepEqual(test.expect, *actual) {
				t.Fatalf("Expect %+v but %+v", test.expect, *actual)
			}
		})
	}
}

func TestFieldToIRInvalidParam(t *testing.T) {
	s := struct {
		F int `dbsteward:",,this is not valid"`
	}{}
	actual := ir.Table{}
	err := fieldToIR(nil, &actual, reflect.TypeOf(s).Field(0))
	if err == nil {
		t.Fatalf("should have resulted in an error")
	}
}

func TestFieldToIR(t *testing.T) {
	tests := []struct {
		name   string
		v      any
		expect ir.Column
	}{
		{
			name: "simple",
			v:    struct{ Field int }{},
			expect: ir.Column{
				Name:     "Field",
				Type:     "BIGINT",
				Nullable: false,
			},
		},
		{
			name: "unique",
			v: struct {
				Field int `dbsteward:",,unique"`
			}{},
			expect: ir.Column{
				Name:     "Field",
				Type:     "BIGINT",
				Nullable: false,
				Unique:   true,
			},
		},
		{
			name: "with default int value",
			v: struct {
				Field int `dbsteward:",,DEFAULT 5"`
			}{},
			expect: ir.Column{
				Name:     "Field",
				Type:     "BIGINT",
				Nullable: false,
				Default:  "5",
			},
		},
		{
			name: "with default string value",
			v: struct {
				Field string `dbsteward:",,DEFAULT '5'"`
			}{},
			expect: ir.Column{
				Name:     "Field",
				Type:     "TEXT",
				Nullable: false,
				Default:  "5",
			},
		},
		{
			name: "simple pointer",
			v:    struct{ Field *int }{},
			expect: ir.Column{
				Name:     "Field",
				Type:     "BIGINT",
				Nullable: true,
			},
		},
		{
			name: "change to nullable",
			v: struct {
				Field int `dbsteward:",,null"`
			}{},
			expect: ir.Column{
				Name:     "Field",
				Type:     "BIGINT",
				Nullable: true,
			},
		},
		{
			name: "change to not nullable",
			v: struct {
				Field *int `dbsteward:",,NOT NULL"`
			}{},
			expect: ir.Column{
				Name:     "Field",
				Type:     "BIGINT",
				Nullable: false,
			},
		},
		{
			name: "rename field",
			v: struct {
				Field int `dbsteward:"renamed"`
			}{},
			expect: ir.Column{
				Name:     "renamed",
				Type:     "BIGINT",
				Nullable: false,
			},
		},
		{
			name: "change type, name unchanged",
			v: struct {
				Field int `dbsteward:",INT"`
			}{},
			expect: ir.Column{
				Name:     "Field",
				Type:     "INT",
				Nullable: false,
			},
		},
		{
			name: "change type and rename",
			v: struct {
				Field int `dbsteward:"renamed,INT"`
			}{},
			expect: ir.Column{
				Name:     "renamed",
				Type:     "INT",
				Nullable: false,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := ir.Table{}
			err := fieldToIR(nil, &actual, reflect.TypeOf(test.v).Field(0))
			if err != nil {
				t.Fatalf(err.Error())
			}
			if *actual.Columns[0] != test.expect {
				t.Fatalf("Expect %+v but %+v", test.expect, *actual.Columns[0])
			}
		})
	}
}

func TestGoTypeToSQL(t *testing.T) {
	boolTrue := true
	tests := []struct {
		v      any
		expect string
	}{
		{
			v:      true,
			expect: "BOOLEAN",
		},
		{
			v:      &boolTrue,
			expect: "BOOLEAN",
		},
		{
			v:      int(5),
			expect: "BIGINT",
		},
		{
			v:      int8(8),
			expect: "SMALLINT",
		},
		{
			v:      int16(8),
			expect: "SMALLINT",
		},
		{
			v:      int32(42),
			expect: "INT",
		},
		{
			v:      int64(42),
			expect: "BIGINT",
		},
		{
			v:      uint(42),
			expect: "BIGINT",
		},
		{
			v:      uint8(42),
			expect: "SMALLINT",
		},
		{
			v:      uint16(42),
			expect: "INT",
		},
		{
			v:      uint32(42),
			expect: "BIGINT",
		},
		{
			v:      float32(42),
			expect: "REAL",
		},
		{
			v:      float64(42),
			expect: "DOUBLE PRECISION",
		},
		{
			v:      "float64(42)",
			expect: "TEXT",
		},
	}
	for _, test := range tests {
		t.Run(test.expect, func(t *testing.T) {
			actual, err := goTypeToSQL(reflect.TypeOf(test.v))
			if err != nil {
				t.Fatalf(err.Error())
			}
			if actual != test.expect {
				t.Fatalf("Expect '%s' but '%s'", test.expect, actual)
			}
		})
	}
}
