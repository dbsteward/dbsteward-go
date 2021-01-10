package pgsql8

import (
	"github.com/dbsteward/dbsteward/lib"
	"github.com/dbsteward/dbsteward/lib/model"
	"github.com/dbsteward/dbsteward/lib/output"
)

var GlobalDiffViews *DiffViews = NewDiffViews()

type DiffViews struct {
}

func NewDiffViews() *DiffViews {
	return &DiffViews{}
}

// TODO(go,core) lift some of these to sql99

func (self *DiffViews) CreateViewsOrdered(ofs output.OutputFileSegmenter, oldDoc *model.Definition, newDoc *model.Definition) {
	self.forEachViewInDepOrder(newDoc, func(newRef model.ViewRef) {
		oldSchema := oldDoc.TryGetSchemaNamed(newRef.Schema.Name)
		var oldView *model.View
		if oldSchema != nil {
			// TODO(go,nth) allow nil receivers in TryGet methods to alleviate branching
			oldView = oldSchema.TryGetViewNamed(newRef.View.Name)
		}
		if self.shouldCreateView(oldView, newRef.View) {
			ofs.WriteSql(GlobalView.GetCreationSql(newRef.Schema, newRef.View)...)
		}
	})
}

func (self *DiffViews) shouldCreateView(oldView, newView *model.View) bool {
	return oldView == nil || lib.GlobalDBSteward.AlwaysRecreateViews || !oldView.Equals(newView, model.SqlFormatPgsql8)
}

func (self *DiffViews) DropViewsOrdered(ofs output.OutputFileSegmenter, oldDoc *model.Definition, newDoc *model.Definition) {
	// TODO(go,pgsql)
}

func (self *DiffViews) forEachViewInDepOrder(doc *model.Definition, callback func(model.ViewRef)) {
	// TODO(go,3) unify this with XmlParser.TableDepOrder?
	if doc == nil {
		return
	}

	visited := map[model.ViewRef]bool{}

	for _, rootSchema := range doc.Schemas {
		for _, rootView := range rootSchema.Views {
			ref := model.ViewRef{rootSchema, rootView}
			if _, ok := visited[ref]; ok {
				continue
			}
			self.dfsViewDeps(doc, ref, visited, callback)
		}
	}
}

func (self *DiffViews) dfsViewDeps(doc *model.Definition, ref model.ViewRef, visited map[model.ViewRef]bool, callback func(model.ViewRef)) {
	if _, ok := visited[ref]; ok {
		return
	}
	visited[ref] = true

	for _, dep := range GlobalView.GetDependencies(doc, ref.Schema, ref.View) {
		self.dfsViewDeps(doc, dep, visited, callback)
	}
	callback(ref)
}
