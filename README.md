# DBSteward (Go Edition)

[![Coverage Status](https://coveralls.io/repos/github/dbsteward/dbsteward-go/badge.svg?branch=master)](https://coveralls.io/github/dbsteward/dbsteward-go?branch=master) [![Tests and Coverage](https://github.com/dbsteward/dbsteward-go/actions/workflows/main.yml/badge.svg)](https://github.com/dbsteward/dbsteward-go/actions/workflows/main.yml)

**NOTE** This codebase is **VERY** work-in-progress, and **is not functional yet**

This repo contains a rewrite of https://github.com/dbsteward/dbsteward. If you're looking for working software, go there.

2.0.0 Goal: Almost straight port of PHP DBSteward to Go. Keep API/CLI compat (version 1.4) with PHP DBSteward

3.0.0 Goal: Refactor to be more object-oriented, cleaner, more go-like. Keep API/CLI compat with PHP DBSteward

4.0.0 Goal: Break API compat, start working on next-gen features

## What is DBSteward?

DBSteward is a database schema change management tool, which works in a fundamentally different way than you're probably used to, but in doing so, solves a great number of common (and uncommon) problems that teams have with most DB change management tools.

### The problem it solves

Almost all of the tools currently out there are based on the "migration pattern", where application developers manage changes to their database by applying a sequence of "migrations", each of which contains (or is translated to) SQL that makes some alteration to the databases. For example, you might have a folder in your codebase that looks like this:

```
my-great-app/
  migrations/
    001_db_init.sql
    002_add_users_table.sql
    003_more_user_fields.sql
    004_add_widgets_table.sql
```

And, `003_more_user_fields.sql` might look something like this:

```sql
ALTER TABLE users
  ADD COLUMN created_at timestamptz;

ALTER TABLE users
  ADD COLUMN bio text NOT NULL
  ADD COLUMN best_friend_id text REFERENCES users (id),
  ALTER COLUMN created_at SET NOT NULL,
  ALTER COLUMN created_at SET DEFAULT NOW();
```

But there's problems here.

**First:** there's a syntax error! Do you see it? There's no comma after the second `ADD COLUMN` and a comma after the third. You might discover this if you have an IDE that syntax-checks your SQL files, but those are pretty rare.

If your IDE doesn't inform you, you'll have to discover the hard way by testing your migration against your dev database. But, surprise! Many many teams out there don't have a dev database, or it's a shared database, so manually testing a migration can be a chore or even dangerous. Or is it? Does your database engine support transactional DDL ("Data Definition Language" - the subset of SQL like `ALTER TABLE` that changes your schema instead of your data), so if this fails, do you know what state your database is left in? Does `users` have the `created_at` column or not if you re-run this?

No matter how you find out about the error, you still need to figure out if you need to add a comma after the second `ADD COLUMN` or remove it from after the third. Do you remember off the top of your head which one it is? Even if you do, do all your teammates?

**Second:** just looking at this single migration, (assuming you fix the syntax error), do you know if this migration will work? There's a few different ways this can (and will!) fail:

- `created_at`, `bio`, `best_friend_id` can already exist in the database, maybe as part of a failed migration earlier, or maybe your coworker beat you to it.
- `id` might not be `text` or a `text`-like data type, causing the foreign key in `best_friend_id` to fail to be created
- we create `bio` as `NOT NULL`, but provide no default for it. If there are any rows at all in the table, this will fail.
- similarly, we create `created_at` as implicitly nullable with no default (because we didn't explicitly say `NOT NULL DEFAULT something`), but then we change it to be `NOT NULL` before setting a default. If there were any rows in the table with `created_at == NULL`, this will fail.

**Third:** do you know what actually happens when you run this (assuming you fix the above errors)? Did you know that in MySQL, this migration will cause the `users` table on disk to be completely rewritten not once, but twice? (once for each `ALTER TABLE`) If this is early on in your database's lifetime, this might not be a big deal, or even noticeable. But if this happens when you have a million rows in the table in your production database.... your on-call engineers are going to be very, very sad.

Similarly, if you wanted to remove the foreign key you created on `best_friend_id` at some point in the future, do you know what name it was given? It's not encoded in the migration, you literally have no choice but to look in the live database.

**Fourth:** suppose you had _just_ written `004_add_widgets_table.sql`. You put your PR up for review, it all works, you go to land, and.... now you find that your folder contains both `004_add_widgets_table.sql` and `004_fix_gadget_column.sql`. You go to run your migration tool and it is unhappy because it doesn't know what "version 4" is. So now you need to add some CI machinery to prevent this case from happening.

**Fifth:** if I asked you what columns were in the `users` table right now, you'd have to read through _every single migration_ in the folder. Each migration expresses a delta from the previous state of the database, not the current state. This makes it very hard to reason about what your database looks like without actually logging in to your dev or prod databases and poking around.

Now, different frameworks have tried to solve various parts of this, where instead of the above SQL, you might instead have something similar to:

```python
# file 123dca0_more_user_fields.py
previous_version = 'abcd012'
def up():
  alter_table('users',
    add_column('created_at', 'timestamptz'),
  )
  alter_table('users',
    add_column('bio', 'text', nullable=False),
    add_column('best_friend_id', 'text', ForeignKey('users', ['id'])),
    alter_column('created_at', nullable=False),
    alter_column('created_at', default="NOW()"),
  )
```

This solves some of the issues - syntax errors in the migration will probably go away, a sufficiently smart tool could warn you you're going to have data integrity problems and error out before actually applying the migration and/or apply optimizations for different database engines, the hash-based names and previous-version pointers eliminate most of the merge conflict issues, etc.

But, this only serves to complicate the matter: what are the arguments for `alter_column`? what if you forget something? After all, these migrations, even auto-generated ones, almost always need hand-tuning, and force you to understand not just what SQL is going to result from this but also learn a completely custom language/API for your migrations. For example, what are the runtime characteristics of this migration? Does it generate one `ALTER TABLE` or two, or (gasp!) five? When you say `default="NOW()"`, does that translate to `DEFAULT NOW()` or `DEFAULT 'NOW()'`?

#### How DBSteward solves these problems

In DBSteward, migrations are completely transient - they're automatically generated and applied as a result of diffing two database states. Usually, those states are "the current schema" and "the desired schema".

For example (omitting some of the required boilerplate for brevity), we might start with a schema that looks like this:

```xml
<!-- file schema.xml -->
<schema name="public">
  <table name="users" primaryKey="id">
    <column name="id" type="uuid" null="false" default="uuid_generate_v4()"/>
    <column name="name" type="text" null="false"/>
  </table>
</schema>
```

Using DBSteward to generate a migration with `dbsteward --xml schema.xml --outputfile build.sql` would produce output similarly to:
```sql
-- file build.sql
CREATE TABLE users (
  id uuid NOT NULL PRIMARY KEY DEFAULT uuid_generate_v4(),
  name text NOT NULL
);
```

Then, at some point later, after that migration was applied to the database, we might decide we want to make the changes from above, so we simply edit `schema.xml` _in place_:

```xml
<schema name="public">
  <table name="users" primaryKey="id">
    <column name="id" type="uuid" null="false" default="uuid_generate_v4()"/>
    <column name="name" type="text" null="false"/>
    <column name="created_at" type="timestamptz" null="false" default="now()"/>
    <column name="bio" type="text" null="false"/>
    <column name="best_friend_id" foreignTable="users" foreignColumn="id"/>
  </table>
</schema>
```

If we wanted to see what SQL would be generated as a result of these changes, we would need to diff `schema.xml` against the "old" version, which we could obtain a few ways:
- Extract the current schema from the live database: `dbsteward --dbschemadump --sqlformat pgsql8 --outputfile old_schema.xml --dbhost my-database-host --dbuser postgres --dbname mydatabase`
- Check out the previous version of the file from version control: `git show HEAD^:schema.xml >old_schema.xml`

Then, we can use DBSteward to diff the two with `dbsteward --oldxml old_schema.xml --newxml schema.xml --singlestageupgrade --outputfile upgrade.sql`, producing something like:
```sql
-- file upgrade.sql
ALTER TABLE "users" ADD COLUMN "created_at" timestamptz DEFAULT NOW();
ALTER TABLE "users" ADD COLUMN "bio" text NULL;
ALTER TABLE "users" ADD COLUMN "best_friend_id" uuid NULL;

UPDATE "users" SET "bio" = "";

ALTER TABLE "users" ALTER COLUMN "bio" SET NOT NULL;
ALTER TABLE "users" ADD FOREIGN KEY CONSTRAINT "users_best_friend_id_fkey" ("best_friend_id") REFERENCES "users" ("id");
```

This solves almost all of the problems we had with the migration workflow:
- We can trust DBSteward to generate syntactically correct SQL. It is well unit tested, and has been in use in production for many years.
- We can trust DBSteward to generate an (almost!) safe migration. Because it's aware of the whole schema, it knows what columns are present, what constraints it needs to abide by (both literal database constraints and technical constraints), and what steps it needs to take to make almost all changes to the schema. It's not perfect yet, and humans familiar with their database engine should still inspect the generated migrations, but for the most part DBSteward takes great care to inform the user of any gotchas it thinks might be there, and will err on the side of caution when possible. You'll note that in this example, the `bio` column was initially created as nullable, then an `UPDATE` was issued to set it to the (implicit) default value, and only then was the column changed to be `NOT NULL` once again.
- DBSteward knows how your database engine works. In the above example, we defaulted to Postgres (because it's great!), but if we had specified `--sqlformat mysql5`, those five `ALTER TABLES` would have been compacted to only two to account for MySQL's behavior. Similarly, it knows how Postgres prefers to name its auto-generated identifiers, and makes those explicit.
- There's no more weird not-quite-merge conflicts that can break master. You can, of course, save the generated migrations for posterity (especially helpful when things inevitably go wrong!), but the only merge conflicts in the critical path you're actually going to see will be bog standard git-generated merge conflicts.
- We know _exactly_ what the database structure is at any given time - it's whatever is in `schema.xml`! This has a huge effect on developer productivity, because they no longer need to dig around in the live database, trying to remember whether psql uses `\dT` or `\dt` to describe a table, and trying very hard not to accidentally paste a `DELETE FROM users` statement.

To see what this looks like in practice, check out the `example/` folder, which we use both to demonstrate DBSteward features, and to help verify and illustrate changes we make to the tool.

For more info, check out https://github.com/dbsteward/dbsteward - the original PHP codebase that this repo is replacing.

## Why are we doing this?

There's a number of reasons why we're rewriting DBSteward.

Firstly, it was originally written for PHP 5.2 or 5.3 (can't really remember). At that time, they had introduced classes, but not much else. Over time, the codebase evolved a little bit to take advantage of newer PHP features, but for the most part did not. Now, there's nothing wrong with PHP, and the latest versions of PHP are are very nice, but, PHP carries a lot of stigma, especially among teams that otherwise really want to use or contribute to DBSteward, but don't know or want to mess with managing a PHP environment for it.

Golang, on the other hand, is a very popular, very productive, and "modern" language (again, not that modern PHP isn't a modern langugage, but that's certainly the perception), meaning that it's a very low barrier of entry for anyone that wants to contribute. Furthermore, because Go generates self-contained statically linked binaries, users of DBSteward don't need to know anything about Go unless they want to build from source - they can just install a single pre-built binary and be off to the races.

Secondly, the existing DBSteward architecture is not very flexible and has a number of odd, undocumented features of dubious usefulness. The architecture as it stands is very hard to maintain, and while robust, fixing bugs or major issues (see "Quoting and Identifiers" below for an example) is extremely challenging. If we wanted to support e.g. Postgres 13 without breaking compatibility with Postgres 8 users, that would be nearly impossible without a monumental amount of work (which is what this undertaking is).

Simply rewriting and doing a straight port naturally doesn't fix many things (although distributing a single binary will sure be nice), but it does give us the opportunity to go through the whole codebase with a fine toothed comb, start making small changes, and recording thoughts that will allow us to make the big architectural shifts that I believe will make DBSteward a successful long-term, maintainable, and popular project.

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

## Thoughts for the future

### General Architecture

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

### Specific Features

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

### Specific v3 Refactors

- Drop static global variables in favor of direct dependencies
- First pass on [strategy architecture](#strategy-architecture)
- Move all validation to a dedicated step
- Separate XML un/marshalling from native in-memory model
- Have all model objects contain backref to parent objects
- Move all XmlParser/DBX functionality into strategy + model objects
- Create dedicated "expansion" step to resolve references, etc

### Off the wall features

- Act as a language server to provide language features to embedded SQL, DBXML, etc in supporting editors
- Database packages
  - Imagine simply running `dbsteward package install user-auth` which downloads a set of pre-baked tables and other objects
- Codegen
  - Imagine being able to generate either specific ORM code (like SQLAlchemy) or bespoke code in your chosen language to access the db
  - Because dbsteward knows how it all fits together, and can theoretically have access to much more information than the raw db schema, we could generate connection code, data serialization code, common queries, ORM-like behavior, etc.
- Runtime variables
- ETL and smart cloning
  - Cloning databases is super expensive, and doing it efficiently is very difficult. If only there was a tool that understood schemas and excelled in only making the necessary changes!

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

https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS:

> Quoting an identifier also makes it case-sensitive, whereas unquoted names are always folded to lower case. For example, the identifiers FOO, foo, and "foo" are considered the same by PostgreSQL, but "Foo" and "FOO" are different from these three and each other. (The folding of unquoted names to lower case in PostgreSQL is incompatible with the SQL standard, which says that unquoted names should be folded to upper case. Thus, foo should be equivalent to "FOO" not "foo" according to the standard. If you want to write portable applications you are advised to always quote a particular name or never quote it.)

So, there's a few things to think about here:
- Postgres treats `foo` and `"foo"` as identical, but `FOO` and `"foo"` are not, because they fold to lowercase, not upper case
- Other engines likely treat `foo`, `FOO` and `"FOO"` as identical because they fold to upper case, but _not_ `FOO` and `"foo"`.
- We need to have a smarter quoting system, and we almost certainly need to rely on the user to tell us per-identifier whether it should be quoted or not.
- Is there some way to reliably, in a dialect-dependent way, automatically determine identifier equality in the face of lacking quoting information? e.g. if we see a name `Foo`, we might infer from the capital that case is important, and therefore we treat it as quoted and case-sensitive in Postgres. How would this work in a very fluid environment as in proposed by the "Strategy Architecture" section above?

### New Schema Sources & Formats

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

#### Live Database Schema

Another good reason to support more than XML is that schemas _already_ exist in formats other than XML. The most obvious being, a live database! DBSteward more or less treats it this way with its extract functionality, but in that scenario the only thing you can do is dump it to XML and then separately do a diff/build. Imagine if instead of extract, then diff, we just treated the live database as the "old" and the XML as "new". Now we'd have no more awkwardness around imperfect extracts or data lost in XML serialization, or in checking out old VCS copies of the schema to compare to.

#### SQL Schema

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

#### HCL Schema

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

#### MVC/ORM Framework Models

And thinking more creatively, observing that almost no one uses DBSteward (yet!) and instead mostly uses either plain old SQL migrations or auto-generated migrations via e.g. Rails or SQLAlchemy or Laravel or GORM or what have you, we could instead treat those ORM models as the schema definition themselves.

However, there's a real opportunity in consuming raw framework models: being able to, say, read all the SQLAlchemy models in an existing application and diff between on-disk and live-database would allow DBSteward to effectively replace Alembic as a migration tool, and while I'm definitely biased here, I think that's a no-brainer switch for most any developer or team if we can make it happen reliably.

The biggest challenge here, obviously, is in actually interfacing with these definitions. In most cases this will probably involve, somehow, programmatically executing the files containing the models. Attempting to parse, say, a SQLAlchemy model definition, will almost definitely end in fire and tears. So, that means that somehow, we'd need to load up a Python interpreter and actually parse (if not execute) the definitions. This represents a huge amount of work.

On the other hand, I would observe that this would be a prime candidate for something like a paid closed-source plugin. To teams trying to optimize their workflow and get over the scalability problems of the traditional migration pattern (partially optimized by a framework or not), having a way to both reuse their existing code and drop its problematic by-products in favor of something that can reliably work without human intervention would be worth a lot of money.

#### And Beyond and In General

Why stop with these ideas, though? I think that by the time we're considering any of these options, it would be quite doable to adopt a plugin-type architecture allowing almost any schema source imaginable.

The two hard parts will be:
- Figuring out what the plugin interface will be
- Figuring out how this go binary will load the plugin

One interesting technique I saw discussed somewhere ages ago was to skip past the standard shared library/dynamic linking plugin implementation, and instead treat plugins as a subprocess daemon that talks over RPC instead of ABI.

I really like the idea of this approach here. If it's implemented well, it would massively increase the potential ecosystem of DBSteward, and open up ideas like the MVC/ORM model schema source discussed above, as instead of trying to find e.g. a Golang Python parser, we can just lean on the language itself and communicate over a well-defined interface. This also means that plugins would be able to be shipped separately from the main binary, opening the doors for paid and/or closed-source plugins.

This would even apply beyond schema sources, and could even support generation strategies for new dialects and versions, as discussed above in "Strategy Architecture"