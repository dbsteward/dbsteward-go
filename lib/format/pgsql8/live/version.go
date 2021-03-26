package live

// https://www.postgresql.org/support/versioning/
// This is obtained from `SHOW server_version_num;`
// Prior to 10.0, Version X.Y.Z was represented as X*10000+Y*100+Z
//   8.4.22 -> 80422
//   9.1.0 -> 90100
// Starting with 10.0, Version X.Y is represented as X*10000 + Y
//   12.5 -> 120005
type VersionNum int

func NewVersionNum(major, minor int, patch ...int) VersionNum {
	if major >= 10 {
		// disregard patch after 10.0
		return VersionNum(major*10000 + minor)
	}
	// if patch not provided assume 0
	if len(patch) == 0 {
		patch = []int{0}
	}
	return VersionNum(major*10000 + minor*100 + patch[0])
}

func (self VersionNum) IsOlderThan(major, minor int, patch ...int) bool {
	return self < NewVersionNum(major, minor, patch...)
}
