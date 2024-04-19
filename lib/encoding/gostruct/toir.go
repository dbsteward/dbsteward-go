package gostruct

import (
	"fmt"
	"log/slog"
	"reflect"
	"strings"

	"github.com/dbsteward/dbsteward/lib/ir"
)

func (t Table) toIR(l *slog.Logger) (*ir.Table, error) {
	l = l.With("table", t.Name)
	rv := ir.Table{
		Name:        t.Name,
		Description: t.Description,
	}
	ft := reflect.TypeOf(t.Fields)
	if ft == nil {
		return nil, fmt.Errorf("table %s: has no fields", t.Name)
	}
	if ft.Kind() != reflect.Struct {
		return nil, fmt.Errorf("table %s: fields must be a struct", t.Name)
	}
	for idx := range ft.NumField() {
		f := ft.Field(idx)
		err := fieldToIR(l, &rv, f)
		if err != nil {
			return nil, fmt.Errorf("table %s: %w", t.Name, err)
		}
	}
	return &rv, nil
}

func fieldToIR(_ *slog.Logger, table *ir.Table, rf reflect.StructField) error {
	if !rf.IsExported() {
		return nil
	}
	var (
		err          error
		name         string
		nullable     *bool
		unique       bool
		typ, Default string
	)
	boolTrue := true
	boolFalse := false
	tag := rf.Tag.Get("dbsteward")
	if tag != "" {
		param := strings.Split(tag, ",")
		if len(param) > 0 && len(param[0]) > 0 {
			name = param[0]
		} else {
			// Redundant to make errors more desriptive
			name = rf.Name
		}
		if len(param) > 1 {
			typ = strings.TrimSpace(param[1])
		}
		for i := 2; i < len(param); i++ {
			switch strings.ToLower(strings.TrimSpace(param[i])) {
			case "null":
				nullable = &boolTrue
			case "not null":
				nullable = &boolFalse
			case "unique":
				unique = true
			case "primary key":
				table.PrimaryKey = append(table.PrimaryKey, name)
			default:
				if len(param[i]) > 7 && strings.ToLower(param[i][:7]) == "default" {
					Default = strings.Trim(strings.TrimSpace(param[i][7:]), "'")
				} else {
					return fmt.Errorf("field %s unrecognized parameter: '%s'", name, param[i])
				}
			}
		}
	}
	if name == "" {
		name = rf.Name
	}
	if typ == "" {
		typ, err = goTypeToSQL(rf.Type)
		if err != nil {
			return fmt.Errorf("column %s: %w", name, err)
		}
	}
	if nullable == nil {
		if rf.Type.Kind() == reflect.Pointer {
			nullable = &boolTrue
		} else {
			nullable = &boolFalse
		}
	}
	f := ir.Column{
		Name:     name,
		Type:     typ,
		Nullable: *nullable,
		Default:  Default,
		Unique:   unique,
	}
	table.Columns = append(table.Columns, &f)
	return nil
}

func goTypeToSQL(t reflect.Type) (string, error) {
	switch t.Kind() {
	case reflect.Bool:
		return "BOOLEAN", nil
	case reflect.Int8, reflect.Int16, reflect.Uint8:
		return "SMALLINT", nil
	case reflect.Int32, reflect.Uint16:
		return "INT", nil
	case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint32, reflect.Uint64:
		return "BIGINT", nil
	case reflect.Float32:
		return "REAL", nil
	case reflect.Float64:
		return "DOUBLE PRECISION", nil
	case reflect.String:
		return "TEXT", nil
	case reflect.Pointer:
		return goTypeToSQL(t.Elem())
	default:
		return t.String(), fmt.Errorf("unknown data type '%s'", t.String())
	}
}
