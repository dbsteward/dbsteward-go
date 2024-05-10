package pgsql8

import (
	"fmt"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/format/pgsql8/sql"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
	"github.com/dbsteward/dbsteward/lib/util"
)

func getCreateSequenceSql(conf lib.Config, schema *ir.Schema, sequence *ir.Sequence) ([]output.ToSql, error) {
	// TODO(go,3) put validation elsewhere
	cache, cacheValueSet := sequence.Cache.Maybe()
	if !cacheValueSet {
		cache = 1
	}
	if cacheValueSet && cache < 1 {
		// TODO better formatting for optional value?
		return nil, fmt.Errorf("sequence %s.%s must have cache value >= 1, %d was given", schema.Name, sequence.Name, cache)
	}

	ref := sql.SequenceRef{Schema: schema.Name, Sequence: sequence.Name}
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
		// NOTE: Old dbsteward uses ALTER TABLE for this, which is valid according to docs, however
		// ALTER SEQUENCE also works in pgsql 8, and that's more correct
		role, err := roleEnum(conf.Logger, conf.NewDatabase, sequence.Owner, conf.IgnoreCustomRoles)
		if err != nil {
			return nil, err
		}
		ddl = append(ddl, &sql.SequenceAlterOwner{
			Sequence: ref,
			Role:     role,
		})
	}

	if sequence.Description != "" {
		ddl = append(ddl, &sql.SequenceSetComment{
			Sequence: ref,
			Comment:  sequence.Description,
		})
	}

	return ddl, nil
}

func getDropSequenceSql(schema *ir.Schema, sequence *ir.Sequence) []output.ToSql {
	return []output.ToSql{
		&sql.SequenceDrop{
			Sequence: sql.SequenceRef{Schema: schema.Name, Sequence: sequence.Name},
		},
	}
}

func getSequenceGrantSql(conf lib.Config, schema *ir.Schema, seq *ir.Sequence, grant *ir.Grant) ([]output.ToSql, error) {
	roles := make([]string, len(grant.Roles))
	var err error
	for i, role := range grant.Roles {
		roles[i], err = roleEnum(conf.Logger, conf.NewDatabase, role, conf.IgnoreCustomRoles)
		if err != nil {
			return nil, err
		}
	}

	perms := util.IIntersectStrs(grant.Permissions, ir.PermissionListAllPgsql8)
	if len(perms) == 0 {
		return nil, fmt.Errorf("no format-compatible permissions on sequence %s.%s grant: %v", schema.Name, seq.Name, grant.Permissions)
	}
	invalidPerms := util.IDifferenceStrs(perms, ir.PermissionListValidSequence)
	if len(invalidPerms) > 0 {
		return nil, fmt.Errorf("invalid permissions on sequence grant: %v", invalidPerms)
	}

	seqRef := sql.SequenceRef{Schema: schema.Name, Sequence: seq.Name}

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
	roRole, err := roleEnum(conf.Logger, conf.NewDatabase, ir.RoleReadOnly, conf.IgnoreCustomRoles)
	if err != nil {
		return nil, err
	}
	if roRole != "" {
		ddl = append(ddl, &sql.SequenceGrant{
			Sequence: seqRef,
			Perms:    []string{ir.PermissionSelect}, // TODO(feat) doesn't this need to have usage too?
			Roles:    []string{roRole},
			CanGrant: false,
		})
	}

	return ddl, nil
}
