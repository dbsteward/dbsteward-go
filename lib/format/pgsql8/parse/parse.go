package parse

import (
	"io"
	"os"

	"github.com/pkg/errors"

	"github.com/dbsteward/dbsteward/lib/model"
)

func FromFiles(files []string) (*model.Definition, error) {
	doc := &model.Definition{}
	for _, file := range files {
		err := MergeFromFile(doc, file)
		if err != nil {
			return nil, err
		}
	}
	return doc, nil
}

func MergeFromFile(doc *model.Definition, file string) error {
	f, err := os.Open(file)
	if err != nil {
		return errors.Wrap(err, "Could not read sql file")
	}
	defer f.Close()
	return ParseInto(doc, f)
}

func ParseInto(doc *model.Definition, r io.Reader) error {
	return nil
}
