package pgsql8

import (
	"strings"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/util"
)

// roleContext informs registerRole's behavior by
// passing information on the context in which
// the role occurred
type roleContext string

const (
	roleContextOwner roleContext = "owner"
	roleContextGrant roleContext = "grant"
)

func newRoleIndex(dbOwner string) *roleIndex {
	return &roleIndex{
		dbOwner: dbOwner,
		owners:  util.NewCountHeap(strings.ToLower),
		apps:    util.NewCountHeap(strings.ToLower),
		ro:      util.NewCountHeap(strings.ToLower),
	}
}

// builtinRoles as defined here: https://www.postgresql.org/docs/current/predefined-roles.html
// This map of funtions facilitates translating when reasonable
var builtinRoles = map[string]func(*roleIndex) string{
	"pg_read_all_data":            func(r *roleIndex) string { return r.resolution.ReadOnly },
	"pg_write_all_data":           func(r *roleIndex) string { panic("not implemented") },
	"pg_read_all_settings":        func(r *roleIndex) string { panic("not implemented") },
	"pg_read_all_stats":           func(r *roleIndex) string { panic("not implemented") },
	"pg_stat_scan_tables":         func(r *roleIndex) string { panic("not implemented") },
	"pg_monitor":                  func(r *roleIndex) string { panic("not implemented") },
	"pg_database_owner":           func(r *roleIndex) string { return r.dbOwner },
	"pg_signal_backend":           func(r *roleIndex) string { panic("not implemented") },
	"pg_read_server_files":        func(r *roleIndex) string { panic("not implemented") },
	"pg_write_server_files":       func(r *roleIndex) string { panic("not implemented") },
	"pg_execute_server_program":   func(r *roleIndex) string { panic("not implemented") },
	"pg_checkpoint":               func(r *roleIndex) string { panic("not implemented") },
	"pg_use_reserved_connections": func(r *roleIndex) string { panic("not implemented") },
	"pg_create_subscription":      func(r *roleIndex) string { panic("not implemented") },
}

// roleIndex tracks the roles used in the database
// It tracks the frequency and contenxt in which their
// used and uses this information to predict the most
// likely purpose of each role.
type roleIndex struct {
	dbOwner    string
	owners     *util.CountHeap[string, string]
	apps       *util.CountHeap[string, string]
	ro         *util.CountHeap[string, string]
	resolution ir.RoleAssignment
}

// registerRole records the use of the specified role in the
// specified context for future evaluation
func (ri *roleIndex) registerRole(context roleContext, role string) {
	if _, isBuiltin := builtinRoles[role]; isBuiltin {
		return
	}
	if context == roleContextGrant && strings.HasSuffix(role, "_ro") || strings.HasSuffix(role, "_readonly") {
		ri.ro.Push(role)
	} else if context == roleContextGrant {
		ri.apps.Push(role)
	} else if context == roleContextOwner {
		ri.owners.Push(role)
	}
}

// resolveRoles sorts through all the roles and does voting to decide which
// role gets to be owner, readonly, etc
// returns the IR object with the resolved roles but also stores them
// internally for future use by get()
func (ri *roleIndex) resolveRoles() *ir.RoleAssignment {
	customRoles := util.NewSet(strings.ToLower)
	if ri.apps.Len() > 0 {
		ri.resolution.Application = ri.apps.Pop()
	}
	if ri.owners.Len() > 0 {
		ri.resolution.Owner = ri.owners.Pop()
	}
	if ri.ro.Len() > 0 {
		ri.resolution.ReadOnly = ri.ro.Pop()
	}
	customRoles.AddFrom(ri.apps.PopAll())
	customRoles.AddFrom(ri.ro.PopAll())
	customRoles.AddFrom(ri.owners.PopAll())
	customRoles.Remove(
		ri.resolution.Application,
		ri.resolution.Owner,
		ri.resolution.Replication,
		ri.resolution.ReadOnly,
	)
	ri.resolution.CustomRoles = append(ri.resolution.CustomRoles, customRoles.Items()...)
	return &ri.resolution
}

// get returns a resolved role. A resolved role may be
// different than what is passed in, such as the case with
// pg_database_owner. But in many cases it will be the same.
func (ri *roleIndex) get(r string) string {
	if resolve, isBuiltin := builtinRoles[r]; isBuiltin {
		return resolve(ri)
	}
	return r
}
