package xml

import (
	"log/slog"
	"os"
	"testing"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/stretchr/testify/assert"
)

// one eighty test uses an IR to build an XML document
// then extracts it and ensures it results in the
// same IR

// TODO list: Things that don't work yet but are feature improvements

func TestOneEighty(t *testing.T) {
	const role = "postgres"
	irSchema := ir.FullFeatureSchema(role)
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(handler)
	xmlDoc, err := FromIR(logger, &irSchema)
	if err != nil {
		t.Fatal(err)
	}
	reflectedSchema, err := xmlDoc.ToIR()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, ir.FullFeatureSchema(role), *reflectedSchema, "reflection does not match original")
}
