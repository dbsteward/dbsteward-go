package pgsql8

import (
	"fmt"
	"strings"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

type DiffTypes struct {
}

func NewDiffTypes() *DiffTypes {
	return &DiffTypes{}
}

func (self *DiffTypes) DiffTypes(ofs output.OutputFileSegmenter, oldSchema *model.Schema, newSchema *model.Schema) {
	self.dropTypes(ofs, oldSchema, newSchema)
	self.createTypes(ofs, oldSchema, newSchema)

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

		GlobalOperations.SetContextReplicaSetId(newType.SlonySetId)

		// TODO(feat) what about functions in other schemas?
		for _, oldFunc := range GlobalSchema.GetFunctionsDependingOnType(oldSchema, oldType) {
			ofs.WriteSql(sql.NewComment(
				"Type migration of %s.%s requires recreating dependent function %s.%s",
				newSchema.Name, newType.Name, oldSchema.Name, oldFunc.Name,
			))
			ofs.WriteSql(GlobalFunction.GetDropSql(oldSchema, oldFunc)...)
		}

		columns, sql := GlobalDataType.AlterColumnTypePlaceholder(oldSchema, oldType)
		ofs.WriteSql(sql...)

		if newType.Kind.Equals(model.DataTypeKindDomain) {
			self.diffDomain(ofs, oldSchema, oldType, newSchema, newType)
		} else {
			ofs.WriteSql(GlobalDataType.GetDropSql(oldSchema, oldType)...)
			sql, err := GlobalDataType.GetCreationSql(newSchema, newType)
			lib.GlobalDBSteward.FatalIfError(err, "Could not get data type creation sql for type alter")
			ofs.WriteSql(sql...)
		}

		// functions are only recreated if they changed elsewise, so need to create them here
		for _, newFunc := range GlobalSchema.GetFunctionsDependingOnType(newSchema, newType) {
			ofs.WriteSql(GlobalFunction.GetCreationSql(newSchema, newFunc)...)
		}

		ofs.WriteSql(GlobalDataType.AlterColumnTypeRestore(columns, newSchema, newType)...)
	}
}

func (self *DiffTypes) dropTypes(ofs output.OutputFileSegmenter, oldSchema *model.Schema, newSchema *model.Schema) {
	if oldSchema != nil {
		for _, oldType := range oldSchema.Types {
			if newSchema.TryGetTypeNamed(oldType.Name) == nil {
				GlobalOperations.SetContextReplicaSetId(oldType.SlonySetId)
				// TODO(go,pgsql) old dbsteward does GetDropSql(*newSchema*, oldtype) but that's not consistent with anything else. Need to validate
				ofs.WriteSql(GlobalDataType.GetDropSql(oldSchema, oldType)...)
			}
		}
	}
}

func (self *DiffTypes) createTypes(ofs output.OutputFileSegmenter, oldSchema *model.Schema, newSchema *model.Schema) {
	for _, newType := range newSchema.Types {
		if oldSchema.TryGetTypeNamed(newType.Name) == nil {
			GlobalOperations.SetContextReplicaSetId(newType.SlonySetId)
			sql, err := GlobalDataType.GetCreationSql(newSchema, newType)
			lib.GlobalDBSteward.FatalIfError(err, "Could not get data type creation sql for type diff")
			ofs.WriteSql(sql...)
		}
	}
}

func (self *DiffTypes) diffDomain(ofs output.OutputFileSegmenter, oldSchema *model.Schema, oldType *model.DataType, newSchema *model.Schema, newType *model.DataType) {
	oldInfo := oldType.DomainType
	newInfo := newType.DomainType

	// TODO(feat) what about minor typename changes like "character varying" => "varchar" or "mytype" => "public.mytype"
	if !strings.EqualFold(oldInfo.BaseType, newInfo.BaseType) {
		// TODO(feat) don't we need to convert columns as in DiffTypes?
		ofs.WriteSql(sql.NewComment("domain base type changed from %s to %s; recreating the type", oldInfo.BaseType, newInfo.BaseType))
		ofs.WriteSql(GlobalDataType.GetDropSql(oldSchema, oldType)...)
		sql, err := GlobalDataType.GetCreationSql(newSchema, newType)
		lib.GlobalDBSteward.FatalIfError(err, "Could not get data type creation sql for domain diff")
		ofs.WriteSql(sql...)
	}

	ref := sql.TypeRef{newSchema.Name, newType.Name}

	if oldInfo.Default != "" && newInfo.Default == "" {
		ofs.WriteSql(&sql.Annotated{
			Annotation: "domain default dropped",
			Wrapped:    &sql.TypeDomainAlterDropDefault{ref},
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
			Wrapped:    &sql.TypeDomainAlterSetNullable{ref, newInfo.Nullable},
		})
	}

	for _, newConstraint := range newType.DomainConstraints {
		oldConstraint := oldType.TryGetDomainConstraintNamed(newConstraint.Name)
		if oldConstraint != nil {
			if !oldConstraint.Equals(newConstraint) {
				ofs.WriteSql(sql.NewComment("domain constraint %s changed from %s", oldConstraint.Name, oldConstraint.Check))
				ofs.WriteSql(&sql.TypeDomainAlterDropConstraint{ref, oldConstraint.Name})
				ofs.WriteSql(&sql.TypeDomainAlterAddConstraint{ref, newConstraint.Name, sql.RawSql(newConstraint.GetNormalizedCheck())})
			}
		} else {
			ofs.WriteSql(sql.NewComment("domain constraint %s added", newConstraint.Name))
			ofs.WriteSql(&sql.TypeDomainAlterAddConstraint{ref, newConstraint.Name, sql.RawSql(newConstraint.GetNormalizedCheck())})
		}
	}
	for _, oldConstraint := range oldType.DomainConstraints {
		if newType.TryGetDomainConstraintNamed(oldConstraint.Name) == nil {
			ofs.WriteSql(&sql.Annotated{
				Annotation: fmt.Sprintf("domain constraint %s removed", oldConstraint.Name),
				Wrapped:    &sql.TypeDomainAlterDropConstraint{ref, oldConstraint.Name},
			})
		}
	}
}
