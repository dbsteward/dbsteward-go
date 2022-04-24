// Package xml provides parsing for "dbxml" files,
// according to the DTD found at the project root
//
// Objects in this package are focused solely on
// xml document-model level operations, and generally
// are clueless about databases as a whole. For
// domain aware objects, use the `model` package.
//
// `util.Opt` cannot be used here because we cannot sanely
// generically unmarshal arbitrary types (TODO - can we solve this?)
package xml

import (
	"encoding/xml"
	"io"

	"github.com/pkg/errors"
)

// ReadDoc parses a `Definition` from an `io.Reader` that returns
// XML that conforms to the DTD at the project root
func ReadDoc(r io.Reader) (*Document, error) {
	doc := &Document{}
	err := xml.NewDecoder(r).Decode(doc)
	if err != nil {
		return nil, err
	}
	return doc, nil
}

// WriteDoc writes the given `Document` to the given `io.Writer`
// in XML that conforms to the DTD at the project root
func WriteDoc(w io.Writer, doc *Document) error {
	// TODO(go,nth) get rid of empty closing tags like <grant ...></grant> => <grant .../>
	// Go doesn't natively support this (yet?), and google is being google about it
	// https://github.com/golang/go/issues/21399
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	return errors.Wrap(enc.Encode(doc), "while marshaling xml")
}