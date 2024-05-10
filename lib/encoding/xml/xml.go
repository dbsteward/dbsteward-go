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
	"log/slog"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/pkg/errors"
)

const EncodingXML = lib.EncodingFormat("xml")

func init() {
	lib.RegisterEncoding(EncodingXML, NewXMLEncoding)
}

type XMLEncoding struct{}

func NewXMLEncoding() lib.Encoding {
	return XMLEncoding{}
}

func (e XMLEncoding) Import(l *slog.Logger, r io.Reader) (*ir.Definition, error) {
	doc, err := ReadDoc(r)
	if err != nil {
		return nil, err
	}
	return doc.ToIR()
}

func (e XMLEncoding) Export(l *slog.Logger, def *ir.Definition, w io.Writer) error {
	doc, err := FromIR(l, def)
	if err != nil {
		return err
	}
	return WriteDoc(l, w, doc)
}

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
func WriteDoc(l *slog.Logger, w io.Writer, doc *Document) error {
	// TODO(go,nth) get rid of empty closing tags like <grant ...></grant> => <grant .../>
	// Go doesn't natively support this (yet?), and google is being google about it
	// https://github.com/golang/go/issues/21399
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	return errors.Wrap(enc.Encode(doc), "could not marshal xml")
}

// TODO lift these up

func ReadDef(r io.Reader) (*ir.Definition, error) {
	doc, err := ReadDoc(r)
	if err != nil {
		return nil, err
	}
	return doc.ToIR()
}

func WriteDef(l *slog.Logger, w io.Writer, def *ir.Definition) error {
	doc, err := FromIR(l, def)
	if err != nil {
		return err
	}
	return WriteDoc(l, w, doc)
}
