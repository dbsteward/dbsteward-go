package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

// TODO(go,core) lift some of these to sql99

func createViewsOrdered(ofs output.OutputFileSegmenter, oldDoc *ir.Definition, newDoc *ir.Definition) {
	forEachViewInDepOrder(newDoc, func(newRef ir.ViewRef) {
		oldSchema := oldDoc.TryGetSchemaNamed(newRef.Schema.Name)
		var oldView *ir.View
		if oldSchema != nil {
			// TODO(go,nth) allow nil receivers in TryGet methods to alleviate branching
			oldView = oldSchema.TryGetViewNamed(newRef.View.Name)
		}
		if shouldCreateView(oldView, newRef.View) {
			ofs.WriteSql(getCreateViewSql(newRef.Schema, newRef.View)...)
		}
	})
}

func shouldCreateView(oldView, newView *ir.View) bool {
	return oldView == nil || lib.GlobalDBSteward.AlwaysRecreateViews || !oldView.Equals(newView, ir.SqlFormatPgsql8)
}

func dropViewsOrdered(ofs output.OutputFileSegmenter, oldDoc *ir.Definition, newDoc *ir.Definition) {
	forEachViewInDepOrder(oldDoc, func(oldViewRef ir.ViewRef) {
		newSchema := newDoc.TryGetSchemaNamed(oldViewRef.Schema.Name)
		newView := newSchema.TryGetViewNamed(oldViewRef.View.Name)
		if shouldDropView(oldViewRef.View, newSchema, newView) {
			ofs.WriteSql(getDropViewSql(oldViewRef.Schema, oldViewRef.View)...)
		}
	})
}

func shouldDropView(oldView *ir.View, newSchema *ir.Schema, newView *ir.View) bool {
	// don't drop the view if new_schema is null - we've already dropped the view by this point
	// otherwise, drop if it changed or no longer exists
	return newSchema != nil && !oldView.Equals(newView, ir.SqlFormatPgsql8)
}

func forEachViewInDepOrder(doc *ir.Definition, callback func(ir.ViewRef)) {
	// TODO(go,3) unify this with XmlParser.TableDepOrder?
	if doc == nil {
		return
	}

	visited := map[ir.ViewRef]bool{}

	for _, rootSchema := range doc.Schemas {
		for _, rootView := range rootSchema.Views {
			ref := ir.ViewRef{Schema: rootSchema, View: rootView}
			if _, ok := visited[ref]; ok {
				continue
			}
			dfsViewDeps(doc, ref, visited, callback)
		}
	}
}

func dfsViewDeps(doc *ir.Definition, ref ir.ViewRef, visited map[ir.ViewRef]bool, callback func(ir.ViewRef)) {
	if _, ok := visited[ref]; ok {
		return
	}
	visited[ref] = true

	for _, dep := range getViewDependencies(doc, ref.Schema, ref.View) {
		dfsViewDeps(doc, dep, visited, callback)
	}
	callback(ref)
}
