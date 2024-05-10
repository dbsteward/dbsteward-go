package lib

import (
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

var extensionMutex sync.Mutex

type EncodingFormat string

type Encoding interface {
	Import(*slog.Logger, io.Reader) (*ir.Definition, error)
	Export(*slog.Logger, *ir.Definition, io.Writer) error
}

var encodings = make(map[EncodingFormat]func() Encoding)

func RegisterEncoding(id EncodingFormat, constructor func() Encoding) {
	extensionMutex.Lock()
	defer extensionMutex.Unlock()
	encodings[id] = constructor
}

func GetEncoding(id EncodingFormat) (func() Encoding, error) {
	extensionMutex.Lock()
	defer extensionMutex.Unlock()
	constructor, exists := encodings[id]
	if !exists {
		return nil, fmt.Errorf("no such encoding as %s", id)
	}
	return constructor, nil
}

type Operations interface {
	Build(outputPrefix string, dbDoc *ir.Definition) error
	BuildUpgrade(
		oldOutputPrefix, oldCompositeFile string, oldDbDoc *ir.Definition, oldFiles []string,
		newOutputPrefix, newCompositeFile string, newDbDoc *ir.Definition, newFiles []string,
	) error
	ExtractSchema(host string, port uint, name, user, pass string) (*ir.Definition, error)
	CompareDbData(dbDoc *ir.Definition, host string, port uint, name, user, pass string) (*ir.Definition, error)
	SqlDiff(old, new []string, outputFile string)

	GetQuoter() output.Quoter
}

var formats = make(map[ir.SqlFormat]func(Config) Operations)

func RegisterFormat(id ir.SqlFormat, constructor func(Config) Operations) {
	extensionMutex.Lock()
	defer extensionMutex.Unlock()
	formats[id] = constructor
}

func Format(id ir.SqlFormat) (func(Config) Operations, error) {
	extensionMutex.Lock()
	defer extensionMutex.Unlock()
	constructor, exists := formats[id]
	if !exists {
		return nil, fmt.Errorf("no such format as %s", id)
	}
	return constructor, nil
}
