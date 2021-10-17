package live

func VersAtLeast(major, minor int) func(VersionNum) bool {
	return func(v VersionNum) bool {
		return v.IsAtLeast(major, minor)
	}
}

// In 9.1, information_schema.triggers deprecated use of the `action_timing`
// column in favor of `condition_timing`. As far as I can tell, the two columns
// are functionally equivalent.
var FEAT_TRIGGER_USE_ACTION_TIMING = VersAtLeast(9, 1)

// In 10.0 pg_catalog.pg_sequence became available for use, and the old ability
// to SELECT from the sequence got heavily changed
// https://www.postgresql.org/docs/10/catalog-pg-sequence.html
var FEAT_SEQUENCE_USE_CATALOG = VersAtLeast(10, 0)
