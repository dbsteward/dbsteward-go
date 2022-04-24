package xml_test

import (
	"os"
	"testing"
)

// HACK: this exists solely to force code coverage metrics in the xml package
func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
