package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

var GlobalSequence *Sequence = NewSequence()

type Sequence struct {
}

func NewSequence() *Sequence {
	return &Sequence{}
}

func (self *Sequence) GetCreationSql(schema *model.Schema, sequence *model.Sequence) []output.ToSql {
	GlobalOperations.SetContextReplicaSetId(sequence.SlonySetId)

	// TODO(go,3) put validation elsewhere

	if sequence.Cache != nil && *sequence.Cache < 1 {
		lib.GlobalDBSteward.Fatal("Sequence %s.%s must have cache value >= 1, %d was given", schema.Name, sequence.Name, sequence.Cache)
	}

	ref := sql.SequenceRef{schema.Name, sequence.Name}
	ddl := []output.ToSql{
		&sql.SequenceCreate{
			Sequence:  ref,
			Cache:     sequence.Cache,
			Start:     sequence.Start,
			Min:       sequence.Min,
			Max:       sequence.Max,
			Increment: sequence.Increment,
			Cycle:     sequence.Cycle,
			OwnedBy:   sequence.OwnedByColumn,
		},
	}

	if sequence.Owner != "" {
		ddl = append(ddl, &sql.SequenceAlterOwner{
			Sequence: ref,
			Role:     lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, sequence.Owner),
		})
	}

	if sequence.Description != "" {
		ddl = append(ddl, &sql.SequenceSetComment{
			Sequence: ref,
			Comment:  sequence.Description,
		})
	}

	return ddl
}

func (self *Sequence) GetGrantSql(doc *model.Definition, schema *model.Schema, seq *model.Sequence, grant *model.Grant) []output.ToSql {
	GlobalOperations.SetContextReplicaSetId(seq.SlonySetId)

	roles := make([]string, len(grant.Roles))
	for i, role := range grant.Roles {
		roles[i] = lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, role)
	}

	perms := util.IIntersectStrs(grant.Permissions, model.PermissionListAllPgsql8)
	if len(perms) == 0 {
		lib.GlobalDBSteward.Fatal("No format-compatible permissions on sequence %s.%s grant: %v", schema.Name, seq.Name, grant.Permissions)
	}
	invalidPerms := util.IDifferenceStrs(perms, model.PermissionListValidSequence)
	if len(invalidPerms) > 0 {
		lib.GlobalDBSteward.Fatal("Invalid permissions on sequence grant: %v", invalidPerms)
	}

	seqRef := sql.SequenceRef{schema.Name, seq.Name}

	ddl := []output.ToSql{
		&sql.SequenceGrant{
			Sequence: seqRef,
			Perms:    []string(grant.Permissions),
			Roles:    roles,
			CanGrant: grant.CanGrant(),
		},
	}

	// SEQUENCE IMPLICIT GRANTS
	// READYONLY USER PROVISION: generate a SELECT on the sequence for the readonly user
	// TODO(go,3) move this out of here, let this create just a single grant
	roRole := lib.GlobalXmlParser.RoleEnum(lib.GlobalDBSteward.NewDatabase, model.RoleReadOnly)
	if roRole != "" {
		ddl = append(ddl, &sql.SequenceGrant{
			Sequence: seqRef,
			Perms:    []string{model.PermissionSelect}, // TODO(feat) doesn't this need to have usage too?
			Roles:    []string{roRole},
			CanGrant: false,
		})
	}

	return ddl
}
