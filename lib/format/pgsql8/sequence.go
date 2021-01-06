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
	// TODO(go,pgsql)
	return nil
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
