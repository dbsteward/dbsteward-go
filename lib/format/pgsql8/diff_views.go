package pgsql8

import (
	"log/slog"

	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

// TODO(go,core) lift some of these to sql99

func createViewsOrdered(l *slog.Logger, ofs output.OutputFileSegmenter, oldDoc *ir.Definition, newDoc *ir.Definition) error {
	return forEachViewInDepOrder(newDoc, func(newRef ir.ViewRef) error {
		ll := l.With(slog.String("view", newRef.String()))
		ll.Debug("consider creating")
		oldSchema := oldDoc.TryGetSchemaNamed(newRef.Schema.Name)
		var oldView *ir.View
		if oldSchema != nil {
			// TODO(go,nth) allow nil receivers in TryGet methods to alleviate branching
			oldView = oldSchema.TryGetViewNamed(newRef.View.Name)
		}
		if oldView != nil {
			ll = ll.With(slog.String("old view", oldView.Name))
		}
		if shouldCreateView(oldView, newRef.View) {
			ll.Debug("shouldCreateView returned true")
			s, err := getCreateViewSql(l, newRef.Schema, newRef.View)
			for _, s1 := range s {
				ll.Debug(s1.ToSql(defaultQuoter(ll)))
			}
			if err != nil {
				return err
			}
			ofs.WriteSql(s...)
		} else {
			ll.Debug("shouldCreateView returned false")
		}
		return nil
	})
}

func shouldCreateView(oldView, newView *ir.View) bool {
	return oldView == nil || lib.GlobalDBSteward.AlwaysRecreateViews || !oldView.Equals(newView, ir.SqlFormatPgsql8)
}

func dropViewsOrdered(ofs output.OutputFileSegmenter, oldDoc *ir.Definition, newDoc *ir.Definition) error {
	return forEachViewInDepOrder(oldDoc, func(oldViewRef ir.ViewRef) error {
		newSchema := newDoc.TryGetSchemaNamed(oldViewRef.Schema.Name)
		newView := newSchema.TryGetViewNamed(oldViewRef.View.Name)
		if shouldDropView(oldViewRef.View, newSchema, newView) {
			ofs.WriteSql(getDropViewSql(oldViewRef.Schema, oldViewRef.View)...)
		}
		return nil
	})
}

func shouldDropView(oldView *ir.View, newSchema *ir.Schema, newView *ir.View) bool {
	// don't drop the view if new_schema is null - we've already dropped the view by this point
	// otherwise, drop if it changed or no longer exists
	return newSchema != nil && !oldView.Equals(newView, ir.SqlFormatPgsql8)
}

func forEachViewInDepOrder(doc *ir.Definition, callback func(ir.ViewRef) error) error {
	// TODO(go,3) unify this with XmlParser.TableDepOrder?
	if doc == nil {
		return nil
	}

	visited := map[ir.ViewRef]bool{}

	for _, rootSchema := range doc.Schemas {
		for _, rootView := range rootSchema.Views {
			ref := ir.ViewRef{Schema: rootSchema, View: rootView}
			if _, ok := visited[ref]; ok {
				continue
			}
			if err := dfsViewDeps(doc, ref, visited, callback); err != nil {
				return err
			}
		}
	}
	return nil
}

func dfsViewDeps(doc *ir.Definition, ref ir.ViewRef, visited map[ir.ViewRef]bool, callback func(ir.ViewRef) error) error {
	if _, ok := visited[ref]; ok {
		return nil
	}
	visited[ref] = true

	deps, err := getViewDependencies(doc, ref.Schema, ref.View)
	if err != nil {
		return err
	}
	for _, dep := range deps {
		if err := dfsViewDeps(doc, dep, visited, callback); err != nil {
			return err
		}
	}
	return callback(ref)
}
