package pgsql8

func VersAtLeast(major, minor int) func(VersionNum) bool {
	return func(v VersionNum) bool {
		return v.IsAtLeast(major, minor)
	}
}

// In 9.1, `information_schema.triggers` deprecated use of the `action_timing`
// column in favor of `condition_timing`. As far as I can tell, the two columns
// are functionally equivalent.
var FEAT_TRIGGER_USE_ACTION_TIMING = VersAtLeast(9, 1)

// In 10.0 `pg_catalog.pg_sequence` became available for use, and the old ability
// to SELECT from the sequence got heavily changed
//
// https://www.postgresql.org/docs/10/catalog-pg-sequence.html
var FEAT_SEQUENCE_USE_CATALOG = VersAtLeast(10, 0)

// In 12.0 pg_catalog.pg_constraint.consrc was removed, in favor of using
// `pg_get_constraintdef(oid)`. Technically, this capability has existed since 8.0,
// however, `consrc` contained the constraint definition as originally written,
// which we preferred over the parsed+pretty-printed version.
//
// https://www.postgresql.org/docs/12/catalog-pg-constraint.html
var FEAT_CONSTRAINT_USE_GETTER = VersAtLeast(12, 0)

// In 11.0 pg_catalog.pg_proc removed use of `proisagg` and `proiswindow`
// in favor of `prokind` values of 'a' and w' respectively.
//
// https://www.postgresql.org/docs/11/catalog-pg-proc.html
var FEAT_FUNCTION_USE_KIND = VersAtLeast(11, 0)
