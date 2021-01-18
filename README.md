# DBSteward (Go Edition)

2.0.0 Goal: Almost straight port of PHP DBSteward to Go. Keep API/CLI compat with PHP DBSteward

3.0.0 Goal: Refactor to be more object-oriented, cleaner, more go-like. Keep API/CLI compat with PHP DBSteward

4.0.0 Goal: Break API compat, start working on next-gen features

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

## Thoughts for the future

### General Thoughts

Everything is just a diff; build is just diff of empty -> new

1. Build an in-memory definition of old & new
  - Do includes and compositing here
  - Read XML (or SQL, HCL, ...?)
  - Read DB structure
2. Transform those defs based on source
  - Add inferred schema objects like read-only grants, foreign key indexes, etc
  - Convert/normalize data types
  - Expand macros, tabrows, etc
3. Validate those defs
4. Diff old and new, generating a set of changes, NOT SQL. (but close to sql)
5. Transform the changes based on target
  - Generate polyfills (e.g. mysql sequences)
  - Translate to dialect-specific types
6. Validate changes
7. Serialize changes to DML/DDL
8. Output to desired location
  - live db
  - 4-stage migration
  - 1-stage migration

### Strategy Architecture

Currently we utilize "inheritance" to implement strategies for different things - the baseline/default strategies that work most of the time are in `sql99`, and then specific dialects can override either high-level or specific aspects of the strategies.

Except, this is a hard-to-follow and un-extensible pattern.

For example, currently, the `pgsql8` dialect package implements a set of things that are compatible with Postgres 8.0, but since then so many great features have come out that we can take advantage of: materialized views, DO blocks, concurrent indexes, native partitioning, etc. In the current architecture, there's no way for us to say, "oh, if we're targeting Postgres 8, use _this_ and if we're targeting Postgres 13, use _that_", without implementing an entirely new format/dialect.

It's also hard to follow, because of the limitations described in the "Static classes & Globals", "Magic format classes & circular dependencies", and "Sql99 and Abstract Classes" sections above. "Inheritance" is simply not a good match for the Go way of doing it, and, I believe, rarely a good pattern in general compared to different composition architectures.

So one pattern I'm thinking about implementing is a more explicit "strategy" architecture:

1. Every _type_ of algorithm in DBSteward's differencing and sql generation engine would conform to an interface, e.g. `type TableCreateStrategy interface { CreateNewTables(oldSchema, newSchema) }`.
2. Instead of "sql99", most algorithms would just have a default implementation that does things a bog-standard way, e.g. for every table, if the table doesn't exist in oldSchema, create it
3. If a given strategy implementation has sub-strategies, then those would be declared interface dependencies of the strategy implementation, e.g. `type DefaultTableCreateStrategy struct { repStrategy CreateTableReplicationStrategy }`
4. Every dialect and dialect version can have different implementations of each strategy as needed. Auxilliary concerns like replication tools can have separate strategies as well (e.g. Slony vs some other replication provider).
5. The concrete implementations of strategies are decided up front as a result of parsing inputs and CLI parameters, and then the "root strategy" would be invoked and trickle down the strategy-tree

This approach would give us a number of interesting properties:
- Extensible, opens the doors for dialect plugins, as dialects are now just a set of concrete implementations of strategies + a hook into the "strategy resolver"
- Easily allows for different versions of the same dialect. e.g. If we're targeting Postgres 13 we can generate a more concise/efficient diff than if we're targeting Postgres 8.
- Composable. Anything that conforms to a strategy interface is capable of fulfilling that role, not just predefined, hardcoded global instances.
- Makes it trivial to implement alternate/experimental algorithms even within a specific dialect/version.
- Opens the doors for more easily implemented polyfills. e.g. Polyfilling sequences in MySQL is now just a different strategy, rather than a pervasive set of feature flags through the code.


### Quoting and Identifiers

Currently DBSteward mostly considers quoting of identifiers to be a generation-layer concern; internally, it doesn't really care about quoted identifiers.

Except, this is incorrect in many cases. Postgres, for example, handles unquoted identifiers case insensitively, but quoted identifiers case sensitively.

This implies that "quotedness" is a property of the identifier itself, rather than a preference of the application.

This also begs the question of, why wouldn't DBSteward simply quote everything? If not-quoting can lead to invalid identifier errors, then why bother with conditional quoting? It doesn't cost us anything to always generate quotes.

In light of the Postgres quoting behavior though, the real answer is somewhat more complicated:

> Quoting an identifier also makes it case-sensitive, whereas unquoted names are always folded to lower case. For example, the identifiers FOO, foo, and "foo" are considered the same by PostgreSQL, but "Foo" and "FOO" are different from these three and each other. (The folding of unquoted names to lower case in PostgreSQL is incompatible with the SQL standard, which says that unquoted names should be folded to upper case. Thus, foo should be equivalent to "FOO" not "foo" according to the standard. If you want to write portable applications you are advised to always quote a particular name or never quote it.)

So, there's a few things to think about here:
- Postgres treats `foo` and `"foo"` as identical, but `FOO` and `"foo"` are not, because they fold to lowercase, not upper case
- Other engines likely treat `foo`, `FOO` and `"FOO"` as identical because they fold to upper case, but _not_ `FOO` and `"foo"`.
- We need to have a smarter quoting system, and we almost certainly need to rely on the user to tell us per-identifier whether it should be quoted or not.
- Is there some way to reliably, in a dialect-dependent way, automatically determine identifier equality in the face of lacking quoting information? e.g. if we see a name `Foo`, we might infer from the capital that case is important, and therefore we treat it as quoted and case-sensitive in Postgres. How would this work in a very fluid environment as in proposed by the "Strategy Architecture" section above?