package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/ir"
	"github.com/dbsteward/dbsteward/lib/output"
)

type DiffViews struct {
}

func NewDiffViews() *DiffViews {
	return &DiffViews{}
}

// TODO(go,core) lift some of these to sql99

func (self *DiffViews) CreateViewsOrdered(ofs output.OutputFileSegmenter, oldDoc *ir.Definition, newDoc *ir.Definition) {
	self.forEachViewInDepOrder(newDoc, func(newRef ir.ViewRef) {
		oldSchema := oldDoc.TryGetSchemaNamed(newRef.Schema.Name)
		var oldView *ir.View
		if oldSchema != nil {
			// TODO(go,nth) allow nil receivers in TryGet methods to alleviate branching
			oldView = oldSchema.TryGetViewNamed(newRef.View.Name)
		}
		if self.shouldCreateView(oldView, newRef.View) {
			ofs.WriteSql(GlobalView.GetCreationSql(newRef.Schema, newRef.View)...)
		}
	})
}

func (self *DiffViews) shouldCreateView(oldView, newView *ir.View) bool {
	return oldView == nil || lib.GlobalDBSteward.AlwaysRecreateViews || !oldView.Equals(newView, ir.SqlFormatPgsql8)
}

func (self *DiffViews) DropViewsOrdered(ofs output.OutputFileSegmenter, oldDoc *ir.Definition, newDoc *ir.Definition) {
	self.forEachViewInDepOrder(oldDoc, func(oldViewRef ir.ViewRef) {
		newSchema := newDoc.TryGetSchemaNamed(oldViewRef.Schema.Name)
		newView := newSchema.TryGetViewNamed(oldViewRef.View.Name)
		if self.shouldDropView(oldViewRef.View, newSchema, newView) {
			ofs.WriteSql(GlobalView.GetDropSql(oldViewRef.Schema, oldViewRef.View)...)
		}
	})
}

func (self *DiffViews) shouldDropView(oldView *ir.View, newSchema *ir.Schema, newView *ir.View) bool {
	// don't drop the view if new_schema is null - we've already dropped the view by this point
	// otherwise, drop if it changed or no longer exists
	return newSchema != nil && !oldView.Equals(newView, ir.SqlFormatPgsql8)
}

func (self *DiffViews) forEachViewInDepOrder(doc *ir.Definition, callback func(ir.ViewRef)) {
	// TODO(go,3) unify this with XmlParser.TableDepOrder?
	if doc == nil {
		return
	}

	visited := map[ir.ViewRef]bool{}

	for _, rootSchema := range doc.Schemas {
		for _, rootView := range rootSchema.Views {
			ref := ir.ViewRef{rootSchema, rootView}
			if _, ok := visited[ref]; ok {
				continue
			}
			self.dfsViewDeps(doc, ref, visited, callback)
		}
	}
}

func (self *DiffViews) dfsViewDeps(doc *ir.Definition, ref ir.ViewRef, visited map[ir.ViewRef]bool, callback func(ir.ViewRef)) {
	if _, ok := visited[ref]; ok {
		return
	}
	visited[ref] = true

	for _, dep := range GlobalView.GetDependencies(doc, ref.Schema, ref.View) {
		self.dfsViewDeps(doc, dep, visited, callback)
	}
	callback(ref)
}
