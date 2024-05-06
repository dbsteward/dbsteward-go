package pgsql8

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

func diffTypes(l *slog.Logger, differ *diff, ofs output.OutputFileSegmenter, oldSchema *ir.Schema, newSchema *ir.Schema) error {
	dropTypes(ofs, oldSchema, newSchema)
	err := createTypes(ofs, oldSchema, newSchema)
	if err != nil {
		return err
	}

	// there is no alter for types
	// find types that still exist that are different
	// placehold type data in table columns, and recreate the type
	for _, newType := range newSchema.Types {
		oldType := oldSchema.TryGetTypeNamed(newType.Name)
		if oldType == nil {
			// CREATE is handled by createTypes()
			continue
		}

		if oldType.Equals(newType) {
			// type did not change, nothing to do
			continue
		}

		// TODO(feat) what about functions in other schemas?
		for _, oldFunc := range GlobalSchema.GetFunctionsDependingOnType(oldSchema, oldType) {
			ofs.WriteSql(sql.NewComment(
				"Type migration of %s.%s requires recreating dependent function %s.%s",
				newSchema.Name, newType.Name, oldSchema.Name, oldFunc.Name,
			))
			ofs.WriteSql(getFunctionDropSql(oldSchema, oldFunc)...)
		}

		columns, sql, err := alterColumnTypePlaceholder(l, differ, oldType)
		if err != nil {
			return err
		}
		ofs.WriteSql(sql...)

		if newType.Kind == ir.DataTypeKindDomain {
			err = diffDomain(ofs, oldSchema, oldType, newSchema, newType)
			if err != nil {
				return err
			}
		} else {
			ofs.WriteSql(getDropTypeSql(oldSchema, oldType)...)
			sql, err := getCreateTypeSql(newSchema, newType)
			if err != nil {
				return fmt.Errorf("could not get data type creation sql for type alter: %w", err)
			}
			ofs.WriteSql(sql...)
		}

		// functions are only recreated if they changed elsewise, so need to create them here
		for _, newFunc := range GlobalSchema.GetFunctionsDependingOnType(newSchema, newType) {
			s, err := getFunctionCreationSql(l, newSchema, newFunc)
			if err != nil {
				return err
			}
			ofs.WriteSql(s...)
		}

		ofs.WriteSql(alterColumnTypeRestore(columns, newSchema, newType)...)
	}
	return nil
}

func dropTypes(ofs output.OutputFileSegmenter, oldSchema *ir.Schema, newSchema *ir.Schema) {
	if oldSchema != nil {
		for _, oldType := range oldSchema.Types {
			if newSchema.TryGetTypeNamed(oldType.Name) == nil {
				// TODO(go,pgsql) old dbsteward does GetDropSql(*newSchema*, oldtype) but that's not consistent with anything else. Need to validate
				ofs.WriteSql(getDropTypeSql(oldSchema, oldType)...)
			}
		}
	}
}

func createTypes(ofs output.OutputFileSegmenter, oldSchema *ir.Schema, newSchema *ir.Schema) error {
	for _, newType := range newSchema.Types {
		if oldSchema.TryGetTypeNamed(newType.Name) == nil {
			sql, err := getCreateTypeSql(newSchema, newType)
			if err != nil {
				return fmt.Errorf("could not get data type creation sql for type diff: %w", err)
			}
			ofs.WriteSql(sql...)
		}
	}
	return nil
}

func diffDomain(ofs output.OutputFileSegmenter, oldSchema *ir.Schema, oldType *ir.TypeDef, newSchema *ir.Schema, newType *ir.TypeDef) error {
	oldInfo := oldType.DomainType
	newInfo := newType.DomainType

	// TODO(feat) what about minor typename changes like "character varying" => "varchar" or "mytype" => "public.mytype"
	if !strings.EqualFold(oldInfo.BaseType, newInfo.BaseType) {
		// TODO(feat) don't we need to convert columns as in DiffTypes?
		ofs.WriteSql(sql.NewComment("domain base type changed from %s to %s; recreating the type", oldInfo.BaseType, newInfo.BaseType))
		ofs.WriteSql(getDropTypeSql(oldSchema, oldType)...)
		sql, err := getCreateTypeSql(newSchema, newType)
		if err != nil {
			return fmt.Errorf("could not get data type creation sql for domain diff: %w", err)
		}
		ofs.WriteSql(sql...)
	}

	ref := sql.TypeRef{Schema: newSchema.Name, Type: newType.Name}

	if oldInfo.Default != "" && newInfo.Default == "" {
		ofs.WriteSql(&sql.Annotated{
			Annotation: "domain default dropped",
			Wrapped:    &sql.TypeDomainAlterDropDefault{Type: ref},
		})
	} else if oldInfo.Default != newInfo.Default {
		// TODO(feat) what about recursively resolving this in the case that the base type is another user defined type?
		ofs.WriteSql(&sql.Annotated{
			Annotation: "domain default changed from " + oldInfo.Default,
			Wrapped: &sql.TypeDomainAlterSetDefault{
				Type: ref,
				Value: &sql.TypedValue{
					Type:   newInfo.BaseType,
					Value:  newInfo.Default,
					IsNull: false, // TODO(feat) how do we distinguish default="NULL" meaning 'NULL' or NULL, and default="" meaning '' or NULL?
				},
			},
		})
	}

	if oldInfo.Nullable != newInfo.Nullable {
		ofs.WriteSql(&sql.Annotated{
			Annotation: "domain nullability changed",
			Wrapped:    &sql.TypeDomainAlterSetNullable{Type: ref, Nullable: newInfo.Nullable},
		})
	}

	for _, newConstraint := range newType.DomainConstraints {
		oldConstraint := oldType.TryGetDomainConstraintNamed(newConstraint.Name)
		if oldConstraint != nil {
			if !oldConstraint.Equals(newConstraint) {
				ofs.WriteSql(sql.NewComment("domain constraint %s changed from %s", oldConstraint.Name, oldConstraint.Check))
				ofs.WriteSql(&sql.TypeDomainAlterDropConstraint{Type: ref, Constraint: oldConstraint.Name})
				ofs.WriteSql(&sql.TypeDomainAlterAddConstraint{
					Type:       ref,
					Constraint: newConstraint.Name,
					Check:      sql.RawSql(newConstraint.GetNormalizedCheck())},
				)
			}
		} else {
			ofs.WriteSql(sql.NewComment("domain constraint %s added", newConstraint.Name))
			ofs.WriteSql(&sql.TypeDomainAlterAddConstraint{
				Type:       ref,
				Constraint: newConstraint.Name,
				Check:      sql.RawSql(newConstraint.GetNormalizedCheck())},
			)
		}
	}
	for _, oldConstraint := range oldType.DomainConstraints {
		if newType.TryGetDomainConstraintNamed(oldConstraint.Name) == nil {
			ofs.WriteSql(&sql.Annotated{
				Annotation: fmt.Sprintf("domain constraint %s removed", oldConstraint.Name),
				Wrapped:    &sql.TypeDomainAlterDropConstraint{Type: ref, Constraint: oldConstraint.Name},
			})
		}
	}
	return nil
}
