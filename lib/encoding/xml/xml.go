// Package xml provides parsing for "dbxml" files,
// according to the DTD found at the project root
package xml

import (
	"encoding/xml"
	"io"

	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/pkg/errors"
)

// ReadDoc parses a `Definition` from an `io.Reader` that returns
// XML that conforms to the DTD at the project root
func ReadDoc(r io.Reader) (*model.Definition, error) {
	doc := &Document{}
	err := xml.NewDecoder(r).Decode(doc)
	if err != nil {
		return nil, errors.Wrap(err, "while unmarshaling xml")
	}

	def, err := doc.ToModel()
	return def, errors.Wrap(err, "while constructing schema definition")
}

// WriteDoc writes the given `Document` to the given `io.Writer`
// in XML that conforms to the DTD at the project root
func WriteDoc(w io.Writer, def *model.Definition) error {
	// TODO(go,nth) get rid of empty closing tags like <grant ...></grant> => <grant .../>
	// Go doesn't natively support this (yet?), and google is being google about it
	// https://github.com/golang/go/issues/21399

	doc := &Document{}
	err := doc.FromModel(def)
	if err != nil {
		return errors.Wrap(err, "while building document model")
	}

	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	return errors.Wrap(enc.Encode(doc), "while marshaling xml")
}
