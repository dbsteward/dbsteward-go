# Thoughts for the future

## General Architecture

Everything is just a diff; build is just diff of empty -> new

1. Build an in-memory definition of old & new
     - Do includes and compositing here
     - Read XML (or SQL, HCL, ...?)
     - Read DB structure
1. Transform those defs based on source
     - Add inferred schema objects like read-only grants, foreign key indexes, etc
     - Convert/normalize data types
     - Expand macros, tabrows, etc
2. Validate those defs
3. Diff old and new, generating a set of changes, NOT SQL. (but close to sql)
4. Transform the changes based on target
     - Generate polyfills (e.g. mysql sequences)
     - Translate to dialect-specific types
5. Validate changes
6. Serialize changes to DML/DDL
7. Output to desired location
     - live db
     - 4-stage migration
     - 1-stage migration

## Specific Features

Keeping a running list of user-facing new features, QoL improvements, etc, I'd like to implement. The sections below detail how we might be able to accomplish some of these things, this is just a high level list, plainly stated.

As noted at the top, any API changes (that is, a change to the DTD, interpretation of the DTD, or CLI invocations) will be in at least v4. (v2 should be a nearly straight port and v3 should be no-op refactors, both of which should consititute no more than a patch version)

- Passing db connection strings
  - `--db postgres://localhost/somedb` (URI style) or `--db 'host=localhost name=somdb'` (DSN style) instead of `--dbhost localhost --dbname somedb`
- Optionally issuing CREATE DATABASE on fresh builds
  - Will need to name databases: `<database name="widgets">`
- Multiple database management
  - move non-`<database>` elements under `<database name="name">`
- Role management
  - Create/drop/alter users/roles, groups
  - Pluggable secret stores for automated credential management?
  - Pluggable strategies for different auth schemes - e.g. AWS IAM auth?
- Extension management:
  - `<extension name="some_ext" version="1.2.3" cascade="true" withSchema="_some_ext_data"/>`
- Compact foreign key references:
  - `<column name="foo_id" references="foo.id">` instead of `<column name="foo_id" foreignTable="foo" foreignColumn="id">`
  - `<foreignKey columns="a,b" references="otherschema.widget(c, d)"/>` instead of `<foreignKey columns="a,b" foreignSchema="otherschema" foreignTable="widget" foreignColumns="c,d"/>`
- Better dialect support
  - Pluggable dialects
  - More recent versions. Postgres 8 and MySQL 5 were released SIXTEEN years ago, MSSQL 10 was thirteen years ago!
  - Specific versions as first-class citizens, take advantage of new features when possible
- More schema definition formats
  - Pluggable definitions
  - SQL, HCL, Frameworks (e.g. SQLAlchemy)
  - Live database diffing
  - Lazy schema definitions - diffing currently happens entirely in memory, but large enough schemas could make that a problem.
- Better strategy for point-in-time changes, like renames and custom transforms
- Uncommon database features
  - Collations, events, rules, operators, opclasses
  - user-defined aggregate/window functions
  - Materialized views, deferred constraints
  - foreign data wrappers
- References to externally-managed objects
  - e.g. foreign key reference to a table not managed by dbsteward
- Externally-defined datasets
  - Currently we sort of support this via pgdataxml, but a) that's the only format and b) we composite into the xml, which is stored entirely in memory
  - CSV, json, other formats would be very cool
  - Streaming straight from the source so we don't hold it in memory would be cooler

## Specific v3 Refactors

- Drop static global variables in favor of direct dependencies
- First pass on [strategy architecture](#strategy-architecture)
- Move all validation to a dedicated step
- Separate XML un/marshalling from native in-memory model
- Have all model objects contain backref to parent objects
- Move all XmlParser/DBX functionality into strategy + model objects
- Create dedicated "expansion" step to resolve references, etc
- Promote data types to a first-class thing instead of string/regex matching everywhere

## Off the wall features

- Act as a language server to provide language features to embedded SQL, DBXML, etc in supporting editors
- Database packages
  - Imagine simply running `dbsteward package install user-auth` which downloads a set of pre-baked tables and other objects
- Codegen
  - Imagine being able to generate either specific ORM code (like SQLAlchemy) or bespoke code in your chosen language to access the db
  - Because dbsteward knows how it all fits together, and can theoretically have access to much more information than the raw db schema, we could generate connection code, data serialization code, common queries, ORM-like behavior, etc.
- Runtime variables
- ETL and smart cloning
  - Cloning databases is super expensive, and doing it efficiently is very difficult. If only there was a tool that understood schemas and excelled in only making the necessary changes!

## Strategy Architecture

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


## Quoting and Identifiers

Currently DBSteward mostly considers quoting of identifiers to be a generation-layer concern; internally, it doesn't really care about quoted identifiers.

Except, this is incorrect in many cases. Postgres, for example, handles unquoted identifiers case insensitively, but quoted identifiers case sensitively.

This implies that "quotedness" is a property of the identifier itself, rather than a preference of the application.

This also begs the question of, why wouldn't DBSteward simply quote everything? If not-quoting can lead to invalid identifier errors, then why bother with conditional quoting? It doesn't cost us anything to always generate quotes.

In light of the Postgres quoting behavior though, the real answer is somewhat more complicated:

https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS:

> Quoting an identifier also makes it case-sensitive, whereas unquoted names are always folded to lower case. For example, the identifiers FOO, foo, and "foo" are considered the same by PostgreSQL, but "Foo" and "FOO" are different from these three and each other. (The folding of unquoted names to lower case in PostgreSQL is incompatible with the SQL standard, which says that unquoted names should be folded to upper case. Thus, foo should be equivalent to "FOO" not "foo" according to the standard. If you want to write portable applications you are advised to always quote a particular name or never quote it.)

So, there's a few things to think about here:
- Postgres treats `foo` and `"foo"` as identical, but `FOO` and `"foo"` are not, because they fold to lowercase, not upper case
- Other engines likely treat `foo`, `FOO` and `"FOO"` as identical because they fold to upper case, but _not_ `FOO` and `"foo"`.
- We need to have a smarter quoting system, and we almost certainly need to rely on the user to tell us per-identifier whether it should be quoted or not.
- Is there some way to reliably, in a dialect-dependent way, automatically determine identifier equality in the face of lacking quoting information? e.g. if we see a name `Foo`, we might infer from the capital that case is important, and therefore we treat it as quoted and case-sensitive in Postgres. How would this work in a very fluid environment as in proposed by the "Strategy Architecture" section above?

## New Schema Sources & Formats

* [DBML](https://dbml.dbdiagram.io/home/)
* [CUE](https://cuelang.org/)
* [HCL](https://github.com/hashicorp/hcl)

At the moment, DBSteward only recognizes DBXML files as a valid source of a database schema.

For many reasons, I don't think this is sufficient. The most obvious being, that lots of people have an intense dislike of XML. It can be verbose and hard to read, hard to validate, and can sometimes have poor editor support.

But more importantly, XML has a number of limitations for relevant to this usecase. Most chiefly that there is no support for any kind of scalar data types. Lists and objects are implicit through the structure of the document, but, look no further than the data column element:

```
<!ELEMENT col (#PCDATA)>
<!ATTLIST col sql (true|false) #IMPLIED>
<!ATTLIST col null (true|false) #IMPLIED>
<!ATTLIST col empty (true|false) #IMPLIED>
```

- The element can contain arbitrary character data (read: byte array at minimum, but probably UTF-8 string). DBSteward currently interprets everything as a unicode string, but what if it was an automatically generated file containing, say, raw image data? Most databases do have a `blob` type after all.
- The attributes contain the literal string values "true" or "false", not real values. Several other places allow `t` or `f`. But what if someone typed `ture`? The attribute is clearly intended to be a boolean type, but is actually a string.
- The attributes exist solely to inform DBSteward how to handle ambiguities in the arbitrary character data: does `<col />` represent a null value or empty string? does `<col>now()</col>` represent the string `"now()"` or the function call `now()`?

Similarly, table primary keys (and many other cases) are defined through `<!ATTLIST table primaryKey CDATA #REQUIRED>`, which is a delimited list of identifiers. Delimited by what? it's not specified, but is commonly `[\s,]+`. What happens if one of those identifiers legitimately contains a comma and needs to be quoted, and how do we express a quote? `primaryKey="'foo,bar', baz"` would be parsed as `["'foo", "bar'", "baz"]`, and even if it was quote aware, single quotes identify literal strings, not a quoted identifier. With the revelations in the "Quoting and Identifiers" section, it's clear we'll need _some_ way of marking specific identifiers as quoted.

### Live Database Schema

Another good reason to support more than XML is that schemas _already_ exist in formats other than XML. The most obvious being, a live database! DBSteward more or less treats it this way with its extract functionality, but in that scenario the only thing you can do is dump it to XML and then separately do a diff/build. Imagine if instead of extract, then diff, we just treated the live database as the "old" and the XML as "new". Now we'd have no more awkwardness around imperfect extracts or data lost in XML serialization, or in checking out old VCS copies of the schema to compare to.

### SQL Schema

If we want to stick to file formats, what could be better for expressing a database schema than actual SQL? That is, after all, what it's made for. Instead of asking users to manually write out e.g. an `ADD COLUMN` statement, we could just parse two sets of SQL files like

```sql
-- old.sql
CREATE TABLE foo (
  id uuid PRIMARY KEY,
  bar text
);

-- new.sql
CREATE TABLE foo (
  id uuid PRIMARY KEY,
  bar text REFERENCES baz(bar),
  created_at timestamptz not null default now(),
);
```

and generate the `ALTER TABLE foo ADD COLUMN`'s on behalf of the user. The hardest part of this will be the need to understand most of every dialect of SQL we want to support, but comes without so many of the limitations of XML.

### HCL Schema

HCL also seems like a potentially promising format, as it has a less verbose syntax, built-in expression language, first-class scalar types besides string, first-class list and map types, and offers alternative JSON and YAML representations for those that really want it. A sample might look like this:

```hcl
schema "public" {
  table "foo" {
    primaryKey = [id]
    column "id" { type = "uuid" }
    column "bar" {
      type = "text"
      references = public.baz.baz
    }
    column "created_at" {
      type = "timestamptz"
      nullable = false
      default = sql("now()")
    }
  }
}
```

Here, we see a few very nice features as compared to the XML:
- `primaryKey` is a proper list type
- `primaryKey` is populated by actual variables referencing later columns
- the foreign key reference on column `bar` uses a variable expression to a different table/column in the schema
- `nullable` on column `created_at` is an actual boolean value
- `default` on column `created_at` uses a function call expression to clearly demarcate a value that should be interpreted as SQL and not a string value

### MVC/ORM Framework Models

And thinking more creatively, observing that almost no one uses DBSteward (yet!) and instead mostly uses either plain old SQL migrations or auto-generated migrations via e.g. Rails or SQLAlchemy or Laravel or GORM or what have you, we could instead treat those ORM models as the schema definition themselves.

However, there's a real opportunity in consuming raw framework models: being able to, say, read all the SQLAlchemy models in an existing application and diff between on-disk and live-database would allow DBSteward to effectively replace Alembic as a migration tool, and while I'm definitely biased here, I think that's a no-brainer switch for most any developer or team if we can make it happen reliably.

The biggest challenge here, obviously, is in actually interfacing with these definitions. In most cases this will probably involve, somehow, programmatically executing the files containing the models. Attempting to parse, say, a SQLAlchemy model definition, will almost definitely end in fire and tears. So, that means that somehow, we'd need to load up a Python interpreter and actually parse (if not execute) the definitions. This represents a huge amount of work.

On the other hand, I would observe that this would be a prime candidate for something like a paid closed-source plugin. To teams trying to optimize their workflow and get over the scalability problems of the traditional migration pattern (partially optimized by a framework or not), having a way to both reuse their existing code and drop its problematic by-products in favor of something that can reliably work without human intervention would be worth a lot of money.

### And Beyond and In General

Why stop with these ideas, though? I think that by the time we're considering any of these options, it would be quite doable to adopt a plugin-type architecture allowing almost any schema source imaginable.

The two hard parts will be:
- Figuring out what the plugin interface will be
- Figuring out how this go binary will load the plugin

One interesting technique I saw discussed somewhere ages ago was to skip past the standard shared library/dynamic linking plugin implementation, and instead treat plugins as a subprocess daemon that talks over RPC instead of ABI.

I really like the idea of this approach here. If it's implemented well, it would massively increase the potential ecosystem of DBSteward, and open up ideas like the MVC/ORM model schema source discussed above, as instead of trying to find e.g. a Golang Python parser, we can just lean on the language itself and communicate over a well-defined interface. This also means that plugins would be able to be shipped separately from the main binary, opening the doors for paid and/or closed-source plugins.

This would even apply beyond schema sources, and could even support generation strategies for new dialects and versions, as discussed above in "Strategy Architecture"