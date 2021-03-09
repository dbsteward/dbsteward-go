# So you want to help develop...

## What needs to be done?

This first pass is going to be a very very straightforward port from PHP to Go. I've been copying code almost verbatim, just upconverting to Go idioms where it makes sense.

I've been stubbing out methods as I go, just slowly working down the code paths, and marking things with `TODO` comments:

In descending order of priority:

- `TODO(go,core)` - core features and code paths for the rewrite
- `TODO(go,pgsql)`, `TODO(go,mysql)`, `TODO(go,mssql)` - specific to those formats
- `TODO(go,slony)` - slony manipulation utilities and features
- `TODO(go,xmlutil)` - xml-manipulation utilities and features
- `TODO(go,nth)` - nice-to-haves. things that we should come back and fix or clean up as part of this rewrite, but not critical
- `TODO(feat)` - feature work, bug fixes, improvements. Things that don't have to do with the go port. these are either carry-overs from the old codebase, or things I notice as I go.
- `TODO(go,3)` - marking areas I know for sure I want to fix as part of the 3.0 refactor
- `TODO(go,4)` - marking thoughts I have about what I'd like to change as part of the 4.0 refactor

In general, for each of `pgsql`, `mysql`, and `mssql`, we need to implement `build`, `diff`, and `extract` codepaths. `pgsql` additionally/conditionally supports `slony` as a replication appliance. Finally, there's a handful of miscellaneous utility functions.

Once most things have solidified (I'm still changing idioms and whatnot frequently), we'll need to start writing unit tests, starting with those defined in old DBSteward, then onto the rest of the new code.

I've been trying to keep this README up-to-date as I change things and think about where I want things to go, but at the moment it's not guaranteed to be an accurate depiction of the current state of affairs. At the time of writing, I mostly have pgsql builds at parity with the legacy code, and am working my way through pgsql diffing.

At the moment, I (@austinhyde) am the only one actively working on this, and as such, am just pushing to master, working my way through pgsql codepaths, comparing outputs to original DBSteward outputs. If you would like to contribute, please open an issue and let me know, and I can start breaking work up into tickets so we don't step on each other's toes and we can communicate any changes.

<details id="status">
  <summary>Here's the current status of the various aspects for v2: (click to open big list)</summary>

- [ ] core, utilities
  - [ ] data addendum aggregation
    - [ ] implement
    - [ ] test
  - [ ] DTD validation
    - thought: can we do this without involving xmllint? I'd prefer to avoid any runtime deps if we can help it
    - thought: can we autogenerate the DTD from model structs?
    - [ ] implement
    - [ ] test
  - [ ] XML Data Insert
    - [x] implement
    - [ ] test
  - [ ] XML Sort
    - [ ] implement
    - [ ] test
  - [ ] XML Convert
    - [x] implement
    - [ ] test
- [ ] pgsql
  - [ ] fresh builds (@austinhyde)
    - [x] implement
    - [ ] clean up, refinement
    - [ ] testing
  - [ ] xml diffing (@austinhyde)
    - [x] implement
    - [ ] clean up, refinement
    - [ ] testing
  - [ ] extraction (@austinhyde)
    - [x] implement
    - [ ] clean up, refinement
    - [ ] testing
  - [ ] db data diffing
    - [x] implement
    - [ ] test
  - [ ] sql diffing
    - [ ] implement
    - [ ] testing
  - [ ] pgdataxml compositing
    - [ ] implement
    - [ ] test
- [ ] slony support
  - [ ] slonik generation (build, diff)
    - [ ] implement
    - [ ] test
  - [ ] slonik convert
    - [ ] implement
    - [ ] test
  - [ ] slony compare
    - [ ] implement
    - [ ] test
  - [ ] slony diff
    - [ ] implement
    - [ ] test
- [ ] mysql
  - [ ] fresh builds
    - [ ] implement
    - [ ] clean up, refinement
    - [ ] testing
  - [ ] diffing
    - [ ] implement
    - [ ] clean up, refinement
    - [ ] testing
  - [ ] extraction
    - [ ] implement
    - [ ] clean up, refinement
    - [ ] testing
- [ ] mssql
  - [ ] fresh builds
    - [ ] implement
    - [ ] clean up, refinement
    - [ ] testing
  - [ ] diffing
    - [ ] implement
    - [ ] clean up, refinement
    - [ ] testing
  - [ ] extraction
    - [ ] implement
    - [ ] clean up, refinement
    - [ ] testing
- [ ] Polish for Release
  - [ ] triage and/or fix remaining `TODO(go,nth|core|xmlutil|pgsql|mssql|mysql)` items
  - [ ] triage and/or fix remaining lint warnings
  - [ ] code/api documentation
  - [ ] docker image
  - [ ] user documentation
  - [ ] flip github repos?
  - [ ] update websites?
  - [ ] CI builds: testing, coverage. Look into GH actions
  - [ ] github issue templates, labels, etc
  - [ ] license, contribution considerations (should we have contributors assign IP?)
  - [ ] create github release 2.0

</details>

There's a few big cross cutting things that need to be sorted out yet:

- [ ] Lift general code up to `sql99` - The general framework is there, but most code so far is only implemented inside `pgsql8`
- [ ] Refactor the various `GlobalDBSteward.Fatal` calls, because we can't unit test those code paths. Will probably need to return actual `error` values
- [ ] Standardize and normalize various names and algorithms (e.g. unify `TryGetXNamed` and `TryGetXMatching`, implement `IdentityMatches` on everything)
- [ ] Change `sql` generation to rely more on specific types instead of just strings, see if we can't pull some of the common stuff to a common package between different dialects
  
## Differences from PHP codebase

### Static classes & Globals

The PHP code relies heavily on static functions and global variables.

I'm not attempting to fix this in 2.0, but just papering over it with global singleton instances.

See `lib/dbsteward.go` for an example of this:

```go
var GlobalDBSteward *DBSteward = NewDBSteward()

type DBSteward struct {
  ...
}

func NewDBSteward() *DBSteward {
  ...
}
```

Anywhere that PHP uses e.g. `xml_parser::something()` or `dbsteward::$FOO`, we want to replace that with the global singleton call/lookup: `GlobalXmlParser.Something()` or `GlobalDBStewawrd.Foo`

### Magic format classes & circular dependencies

There's simply no way to replicate the old magic classname autoloader that would dynamically replace e.g. `format_column::foo()` with `mysql5_column::foo()` according to `dbsteward::$sql_format`. This was due to our use of inheriting common functionality from e.g. `sql99_table`, which needs to invoke e.g. `pgsql8_column`, but without that extra step of indirection, because it's all static class methods, there's no way to know, from `sql99_table`, that you need to call `pgsql8_column` and not `mysql5_column`.

Instead, as described above, each format will export a global singleton object, like `format/pgsql8.GlobalPgsql8` or `format/mysql5.GlobalMysql5Column`.

_tbd: this is still up in the air, will evolve as we fill more out_

The catch is, that as written in the legacy codebase, `dbsteward` depends on e.g. `pgsql8` and vice versa, creating a circular dependency. Because these live in different packages in the Go rewrite, we can't do this - Go doesn't allow circular dependencies.

So, we have a somewhat janky setup until we can rectify this:

- `main.go` depends on both `lib` and `lib/format/*`
- `main.go` creates the global dbsteward instance, with pointers to the format objects
- `lib` depends on `lib/format` but not `lib/format/*`
- `lib/format/*` depends on `lib` to call global dbsteward/xmlparser objects

### Sql99 and Abstract Classes

Go does not have a concept of "abstract classes" like PHP or Java.

What it does have is struct and interface embedding. Embedding a struct (either by value or by pointer) or an interface causes the containing struct to automatically proxy field and method calls to the embedded struct. This embedded object can be referred to explicitly by its type name.

We can utilize the following idiom to mimic PHP/Java style abstract classes in Go:

```go
type ThingInterface interface {
  DoSomething()
  DoSomethingElse()
}

type AbstractThing struct {
  ThingInterface // embedding the interface
}

func (self *AbstractThing) DoSomething() {
  self.DoSomethingElse()
}

type ConcreteThing struct {
  *AbstractThing // embedding the struct
}

func (self *ConcreteThing) DoSomethingElse() {
  fmt.Println("hi")
}

func main() {
  abstract := &AbstractThing{}
  concrete := &ConcreteThing {
    AbstractThing: abstract,
  }
  concrete.AbstractThing.ThingInterface = concrete

  concrete.DoSomething() // "hi"
}
```

This is quite roundabout, but, from main:

1. make a new `AbstractThing`
2. make a new `ConcreteThing`, explicitly setting the embedded `AbstractThing`. (there's an implicit fieldname based on the name of the embedded type)
3. once we have a `concrete` instance, we can set the `AbstractThing`'s embedded instance to it.
   - `AbstractThing` embeds a `ThingInterface`, so `concrete` must implement `ThingInterface` to be assigned
   - `ConcreteThing`s implement `ThingInterface` because they embed `AbstractThing`, which in turn embeds `ThingInterface`, meaning that `concrete` has all the methods of `ThingInterface`
   - therefore anything that embeds an interface, even transitively, automatically implements that interface
4. `concrete.DoSomething()` invokes `ConcreteThing.DoSomething(concrete)`, but there is no such method, so it's proxied to the embedded `abstract` (from the explicit field from step 2)
5. `abstract.DoSomething()` invokes `AbstractThing.DoSomething(abstract)`, whose implementation calls `self.DoSomethingElse()`
6. `self.DoSomethingElse()` invokes `AbstractThing.DoSomethingElse(abstract)`, but there is no such method, so it's proxied to the embedded `concrete` (from the assignment of `concrete.AbstractThing.ThingInterface` in step 3)
7. `concrete.DoSomethingElse()` invokes `ConcreteThing.DoSomethingElse(concrete)`, whose implementation calls `Println`
8. hi

So, TL;DR, we set it up so that missed method calls from the child go to the parent, and missed method calls from the parent go to the child.

And if you're wondering what happens if you call a method that's defined on the interface, but neither parent or child? Well, it goes around and around forever looking for that method.

All in all, this is adequate for the first pass, but embedding is such a huge footgun, I'm hoping to get rid of this in 3.0.

### [Major Change] XML documents

The PHP codebase directly operated on SimpleXML documents and nodes. Aside from coupling issues, this also made it super annoying to test and led to lots of duplicated code (I'm looking at you, tables-named-x xpath).

Attempting to replicate this in Go will lead to all kinds of heartache, because Go _really_ wants you to decode your document to a custom data structure and operate on that. Accessing the document directly is possible, but probably more pain than its worth.

So, given that almost all XML reads and writes go through `XmlParser` anyways, I've chosen to use that as an abstraction boundary from raw XML to an actual object model.

This object model lives in the `model` package. Operations that traverse and manipulate this model can live either on methods on the model (like the very frequently used `x.GetYNamed(z)`/xpath lookups), or in external operations (like `DBSteward.doXmlDataInsert()`).

After modifications are made, the `XmlParser` can marshal that data structure back out to XML, and no one is the wiser.

This is maybe the most significant departure from the PHP codebase so far

### [Major Change] SQL building

The PHP codebase directly manipulates and builds SQL strings. Not only are these places a PITA to read and write, they're finnicky to test as well, and are prohibitive in terms of potential post-processing optimizations, like compacting and reordering `ALTER TABLE` statements in mysql.

Instead of directly manipulating SQL, we should build custom objects for each DML/DDL clause and emit these, and marshal them to SQL at the last minute.

```go
package output
type ToSql interface {
  ToSql(q Quoter) string
}

package sql
type AlterTableSetOwner struct {
	Table TableRef
	Role  string
}

func (self *AlterTableSetOwner) ToSql(q output.Quoter) string {
	return fmt.Sprintf(
		"ALTER TABLE %s OWNER TO %s;",
		self.Table.Qualified(q),
		q.QuoteRole(self.Role),
	)
}

// package pgsql8
func DoSomethingWithTable() []output.ToSql {
  ...
  output = append(output, &sql.AlterTableOwner{
    Table: sql.TableRef{schema, table},
    Role: someRole,
  })
  ...
}
```

### New Features & Bugfixes

v2 and v3's goals are to be as minimally different behavior-wise from v1 as possible, even including bugs.

However, as I've gone, I've found that a few features/fixes needed to be added so I can validate that the code is as correct as possible. These changes are designed to be as minimally impactful as possible and not break practical compatibility.

The general theme here is that lowering the amount of human intervention required to generate a correct and functional migration is not considered to be a forwards- or backwards-incompatible change, nor are changes that do not affect the ability for v1 to consume v2-generated XML, nor are changes that avoid crashes that would prevent normal operation of the code.

- CHANGED: Function definition bodies, view queries, and similar fields are extracted/composited as `<![CDATA[...]]>` directives
  - Rationale: Besides being more correct, it's necessary due to the way Go XML un/marshalling works
  - Before: These were extracted as plain text, with xml characters escaped
  - After: These are now extracted as CDATA
  - Compatibility: v1 should still be able to consume this without issue - TODO(go,core) verify this claim
- NEW: Heuristic-based `<database><role>` assignment in pgsql8 extraction
  - Rationale: Prevented automated testing of diff of schema to extracted schema
  - Before: All roles were set to the database user doing the extraction, other roles encountered treated as `<customRole>`
  - After: Roles are now set according to their usage in the schema, falling back to the current user. Other roles still treated as custom roles.
  - Compatibility: Previous behavior required human intervention 100% of the time, and this cuts it down to near zero.
- NEW: Extracting table inheritance in pgsql8
  - Rationale: Prevented automated testing of diff of schema to extracted schema (due to partitioning synthesis)
  - Before: Tables that inherited from another were extracted as full-blown tables
  - After: Tables inheriting from another are marked with (the already supported) `inheritsSchema` and `inheritsTable` attributes. Columns that are inherited from the parent are omitted from the child.
  - Compatibility: Previous behavior was incomplete and required human intervention 100% of the time, this almost never does
- NEW: Extracting CHECK constraints in pgsql8
  - Rationale: Prevented automated testing of diff of schema to extracted schema (due to partitioning synthesis)
  - Before: CHECK constraints were ignored
  - After: CHECK constraints are extracted as `<constraint type="CHECK">` elements
  - Compatibility: Previous behavior was incomplete and required human intervention 100% of the time, this almost never does
- NEW: Extracting trigger functions in pgsql8
  - Rationale: Prevented automated testing of diff of schema to extracted schema (due to partitioning synthesis)
  - Before: Functions with `RETURNS trigger` were not extracted
  - After: Functions with `RETURNS trigger` are now extracted as such
  - Compatibility: Previous behavior was incomplete and required human intervention 100% of the time, this almost never does
- FIXED: No longer extracts aggregate/window functions in pgsql8
  - Rationale: I was already mucking about in the function and noticed the potential bug. There was no compelling reason to leave the bad behavior.
  - Before: User-defined aggregate and window functions would have been attempted to be extracted, and likely would have resulted in invalid XML if not crashing outright.
  - After: Aggregate/window functions are retrieved, but ignored with a warning.
  - Compatibility: Previous behavior either resulted in incoherent XML requiring human intervention or would not work at all if an aggregate/window function was present. New behavior prevents this case entirely and warns the user.
- CHANGED: No longer ignores "C" language functions in pgsql8
  - Rationale: I was already mucking about in the function and noticed the potential for incomplete extraction. Changing to a warning does not change behavior at all, but becomes more informative to the user.
  - Before: Extraction query flat out ignored `LANGUAGE C` functions
  - After: `LANGUAGE C` functions are retrieved, but ignored with a warning
  - Compatibility: No behavior change
- FIXED: _Always_ extracts `oids=false` for tables in Postgres 12+ (issue https://github.com/dbsteward/dbsteward/issues/139)
  - Rationale: Postgres 12 removes the `pg_catalog.pg_class.relhasoids` column, which causes DBSteward to crash with a fatal error. `oids=true` is no longer supported, and `oids=false` is not only the default, but the only possible value. I am also periodically testing against recent Postgres versions and noting/fixing issues related to that.
  - Before: Extraction would attempt to query for `relhasoids` for table storage options and crash in Postgres 12+
  - After: Extraction checks Postgres server version and changes queries accordingly. In Postgres 12+, we assume `oids=false`.
  - Compatibility: No behavior change for Postgres <12, prevents a crash in Postgres >=12
- CHANGED: Does not extract `oids=false` for tables in pgsql8 (issue https://github.com/dbsteward/dbsteward/issues/97)
  - Rationale: `oids=false` is the default in postgres, there is no need to specify it. Cuts down on clutter in extracted xml. The opposite, `oids=true` is left unchanged
  - Before: a `<tableOption><name>with</name><value>(oids=false)</value></tableOption>` would be extracted
  - After: this no-op value is not extracted.
  - Compatibility: Although it results in different DDL, it is functionally the same on a clean build - specifying this option has no effect, as it's the default behavior.

And here's a list of changes that I do not intend to keep forever, at least without serious thought and rework:

- CHANGED: Foreign key constraint creation time deferred until after data changes (issue https://github.com/dbsteward/dbsteward/issues/142)
  - Rationale: This is flat out broken in dbsteward, and there's no good way around it, as it prevents applying the upgrade from `someapp_v1.xml` to `someapp_v2.xml`. Changed to keep moving on things, pending further discussion.
  - Before: Foreign key constraints were created in stage 1
  - After: Foreign key constraints were created in stage 4
  - Compatibility: DANGEROUS. This is a drastic change, and violates some of the guarantees and safety of the multi-stage system.


## Idioms and Key Concepts in this codebase

### Identity Equality vs Object Equality

This is a concept that was left largely implicit in the v1 codebase, but is much more explicit in v2.

"Identity Equality" is when two objects have the same identity (but not necessarily the same contents/definition). "Object Equality" is when two objects have the same contents or definition.

We see this crop up when computing diffs. For example, when determining if we need to create or update a new e.g. table, we look at each table in the "new" schema, then search the "old" schema for a table with the same _identity_. If we find a match, then and only then do we check the _contents_ of the tables for equality. If two tables have the same identity but different contents, we (probably) issue an `ALTER TABLE`. If we do not find a match, we (probably) issue a `CREATE TABLE` instead. In the case of tables, we consider case-insensitive table name to be the table's identity.

Wherever possible, we should prefer to explicitly define and use methods for these two purposes. At the moment, those methods use the names `IdentityMatches` and `Equals`.

So, `table1.IdentityMatches(table2)` will return true if `table1`'s identity is equal to `table2`'s identity, and `table1.Equals(table2)` will return true of the contents of the two tables are equal. If either or both of the two objects is `nil`, in either case, we consider the two to be not equal.

There are a few interesting questions that arise here that we should answer and codify soon:
- Should identity be considered to be part of object equality? i.e. if two tables have the same columns, indexes, etc, but different names, are they the same table? 
- Should "namespace" be considered to be part of identity equality? i.e. if two tables have the same name but are in different tables, do they have the same identity?
  - I think the obvious answer here is "yes" - two tables in different schemas with the same name are literally different tables. This implies we need to check parent object lineage in most cases, which we currently _do not_ do, making this a giant `TODO(go,3)`
- How do we handle case-sensitivity when checking names?
  - Some engines have different rules for case sensitive identifiers
  - [Quotedness has a big impact here](#quoting-and-identifiers)