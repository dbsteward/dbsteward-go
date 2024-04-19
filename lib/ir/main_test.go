package ir_test

import (
	"os"
	"testing"
)

// HACK: this exists solely to force code coverage metrics in the model package

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
