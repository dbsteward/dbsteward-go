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
