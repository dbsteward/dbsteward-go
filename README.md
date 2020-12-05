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

### Magic format classes

There's simply no way to replicate the old magic classname autoloader that would dynamically replace e.g. `format_column::foo()` with `mysql5_column::foo()` according to `dbsteward::$sql_format`.

Instead, as described above, each format will export a global singleton object, like `format/pgsql8.GlobalPgsql8` or `format/mysql5.GlobalMysql5Column`. Then the `format` package will export interfaces for all the methods that callers need, as well as a global map from `format.SqlFormat` to that interface.

The `DBSteward` object will expose helper methods for each of the format objects, like `FormatGeneral()` or `FormatColumn()`, which will return an instance of that exported interface.

So, anywhere PHP uses e.g. `format::build()` or `format_column::get_definition()`, we'd replace that with `GlobalDBSteward.FormatGeneral().Build()` or `GlobalDBSteward.FormatColumn().GetDefinition()`.

Yes, this is extremely verbose, but I don't have a better/cleaner solution right now, and I'd want this to all go away in 3.0 anyways.

### XML documents

The PHP codebase directly operated on SimpleXML documents and nodes. Aside from coupling issues, this also made it super annoying to test and led to lots of duplicated code (I'm looking at you, tables-named-x xpath).

Attempting to replicate this in Go will lead to all kinds of heartache, because Go _really_ wants you to decode your document to a custom data structure and operate on that. Accessing the document directly is possible, but probably more pain than its worth.

So, given that almost all XML reads and writes go through `xml_parser` anyways, I've chosen to use that as an abstraction boundary from raw XML to an actual object model.

This object model, for now, lives in the `xml` package. Operations that traverse and manipulate this model can live either on methods on the model (like the very frequently used xpath lookups), or in external operations (like `DBSteward.doXmlDataInsert()`).

After modifications are made, the `XmlParser` can marshal that data structure back out to XML, and no one is the wiser.

This is maybe the most significant departure from the PHP codebase so far