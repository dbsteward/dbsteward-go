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

I haven't figured out exactly what this looks like yet, but I'm aiming for something like:

```go
type SqlStatement interface {
  ToSql(q Quoter) string
}

type AlterTableSetOwner struct {
	Table TableRef
	Role  string
}

func (self *AlterTableSetOwner) ToSql(q Quoter) string {
	return fmt.Sprintf(
		"ALTER TABLE %s OWNER TO %s;\n",
		self.Table.Qualified(q),
		q.QuoteRole(self.Role),
	)
}

func DoSomethingWithTable() []SqlStatement {
  ...
  output = append(output, &AlterTableOwner{
    Table: TableRef{schema, table},
    Role: someRole,
  })
  ...
}
```

Maybe could have intermediate objects for e.g. `ALTER TABLE` statements like
```go
type AlterTable struct {
  Table TableRef
  Clauses []AlterTablePart
}

type AlterTablePart interface {
  GetAlterTableSqlClause(q Quoter) string
}
type AlterTableSetOwnerClause struct {
  Role string
}
```

This is all still up in the air, though. I think for the first pass we should do what's straightforward and observe where things are rough and mark those for refactor in 3.0.