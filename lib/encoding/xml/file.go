package xml

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/util"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

func LoadDefintion(file string) (*ir.Definition, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, errors.Wrapf(err, "could not read dbxml file %s", file)
	}
	defer f.Close()

	return ReadDef(f)
}

func SaveDefinition(filename string, def *ir.Definition) error {
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("could not open file %s for writing: %w", filename, err)
	}
	defer f.Close()

	err = WriteDef(f, def)
	if err != nil {
		return fmt.Errorf("could not write XML document to '%s': %w", filename, err)
	}
	return nil
}

func FormatXml(def *ir.Definition) (string, error) {
	buf := &bytes.Buffer{}
	err := WriteDef(buf, def)
	if err != nil {
		return "", fmt.Errorf("could not marshal definition: %w", err)
	}
	return buf.String(), nil
}

func XmlComposite(files []string) (*ir.Definition, error) {
	doc, _, err := XmlCompositeAddendums(slog.Default(), files, 0)
	if err != nil {
		return nil, err
	}
	return doc, nil
}

func XmlCompositeAddendums(l *slog.Logger, files []string, addendums uint) (*ir.Definition, *ir.Definition, error) {
	var composite, addendumsDoc *ir.Definition
	startAddendumsIdx := -1

	if addendums > 0 {
		addendumsDoc = &ir.Definition{}
		startAddendumsIdx = len(files) - int(addendums)
	}

	for _, file := range files {
		l.Info(fmt.Sprintf("Loading XML %s...", file))

		doc, err := LoadDefintion(file)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to load and parse xml file %s: %w", file, err)
		}
		l.Info(fmt.Sprintf("Compositing XML %s", file))
		composite, err = CompositeDoc(composite, doc, file, startAddendumsIdx, addendumsDoc)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to composite xml file %s: %w", file, err)
		}
	}
	formatted, err := FormatXml(composite)
	if err != nil {
		return nil, nil, err
	}
	ValidateXml(formatted)

	return composite, addendumsDoc, nil
}

func CompositeDoc(base, overlay *ir.Definition, file string, startAddendumsIdx int, addendumsDoc *ir.Definition) (*ir.Definition, error) {
	util.Assert(overlay != nil, "CompositeDoc overlay must not be nil, you probably want CompositeDoc(nil, doc, ...) instead")

	if base == nil {
		base = &ir.Definition{}
	}

	overlay, err := expandIncludes(overlay, file)
	if err != nil {
		return base, err
	}
	overlay = expandTabrowData(overlay)
	overlay, err = SqlFormatConvert(overlay)
	if err != nil {
		return base, err
	}

	// TODO(go,core) data addendums
	// TODO(go,slony) slony composite aspects

	base.Merge(overlay)

	// NOTE: v1 had schema validation occur _during_ the merge, which arguably is more efficient,
	// but also is a very different operation. We're going to try a separate validation step in v2+
	errs := base.Validate()
	if len(errs) > 0 {
		// TODO(go,nth) can we find a better way to represent validation errors? should we actually validate _outside_ this function?
		return base, &multierror.Error{
			Errors: errs,
		}
	}

	return base, nil
}

func expandTabrowData(doc *ir.Definition) *ir.Definition {
	for _, schema := range doc.Schemas {
		for _, table := range schema.Tables {
			if table.Rows != nil {
				table.Rows.ConvertTabRows()
			}
		}
	}
	return doc
}

func expandIncludes(doc *ir.Definition, file string) (*ir.Definition, error) {
	for _, includeFile := range doc.IncludeFiles {
		include := includeFile.Name
		// if the include is relative, make it relative to the parent file
		if !filepath.IsAbs(include) {
			inc, err := filepath.Abs(filepath.Join(filepath.Dir(file), include))
			if err != nil {
				return doc, fmt.Errorf("could not establish absolute path to file %s included from %s", include, file)
			}
			include = inc
		}
		includeDoc, err := LoadDefintion(include)
		if err != nil {
			return doc, fmt.Errorf("failed to load and parse xml file %s included from %s", include, file)
		}

		doc, err = CompositeDoc(doc, includeDoc, include, -1, nil)
		if err != nil {
			return doc, errors.Wrapf(err, "while compositing included file %s from %s", include, file)
		}
	}
	doc.IncludeFiles = nil

	return doc, nil
}

func XmlCompositePgData(doc *ir.Definition, dataFiles []string) *ir.Definition {
	// TODO(go,pgsql) pgdata compositing
	return nil
}

// @TODO: Why is this here? Probably needs to go, should be handled by
// the individual drivers after the IR has been produced.
func SqlFormatConvert(doc *ir.Definition) (*ir.Definition, error) {
	// legacy 1.0 column add directive attribute conversion
	for _, schema := range doc.Schemas {
		for _, table := range schema.Tables {
			for _, column := range table.Columns {
				column.ConvertStageDirectives()
			}
		}
	}
	return doc, nil
}

func SlonyIdNumber(doc *ir.Definition) *ir.Definition {
	// TODO(go,slony)
	return nil
}

func FileSort(file, sortedFile string) {
	// TODO(go,xmlutil)
}

func ValidateXml(xmlstr string) {
	// TODO(go,core) validate the given xml against DTD. and/or, do we even need this now that we're serializing straight from structs?
}
