# DBSteward (Go Edition)

[![Coverage Status](https://coveralls.io/repos/github/dbsteward/dbsteward-go/badge.svg?branch=master)](https://coveralls.io/github/dbsteward/dbsteward-go?branch=master) [![Tests and Coverage](https://github.com/dbsteward/dbsteward-go/actions/workflows/main.yml/badge.svg)](https://github.com/dbsteward/dbsteward-go/actions/workflows/main.yml)

**NOTE** This codebase is **VERY** work-in-progress, and **is not functional yet**

This repo contains a rewrite of https://github.com/dbsteward/dbsteward. If you're looking for working software, go there.

2.0.0 Goal: Almost straight port of PHP DBSteward to Go. Keep API/CLI compat (version 1.4) with PHP DBSteward

3.0.0 Goal: Refactor to be more object-oriented, cleaner, more go-like. Keep API/CLI compat with PHP DBSteward

4.0.0 Goal: Break API compat, start working on next-gen features

## What is DBSteward?

DBSteward is a database schema change management tool, which works in a fundamentally different way than you're probably used to, but in doing so, solves a great number of common (and uncommon) problems that teams have with most DB change management tools.

The TL;DR is that, the industry-standard "migration" workflow has a huge number of problems. DBSteward solves these by generating upgrades from one version of a database schema to another, by comparing XML files (for now) and seeing what has changed.

So, given two files `old_schema.xml` and `new_schema.xml`:

```xml
<!-- file old_schema.xml -->
<schema name="public">
  <table name="users" primaryKey="id">
    <column name="id" type="uuid" null="false" default="uuid_generate_v4()"/>
    <column name="name" type="text" null="false"/>
  </table>
</schema>
```

```xml
<!-- file new_schema.xml -->
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

Then running  `dbsteward --oldxml old_schema.xml --newxml new_schema.xml` will generate:
```sql
ALTER TABLE "users" ADD COLUMN "created_at" timestamptz DEFAULT NOW();
ALTER TABLE "users" ADD COLUMN "bio" text NULL;
ALTER TABLE "users" ADD COLUMN "best_friend_id" uuid NULL;

UPDATE "users" SET "bio" = "";

ALTER TABLE "users" ALTER COLUMN "bio" SET NOT NULL;
ALTER TABLE "users" ADD FOREIGN KEY CONSTRAINT "users_best_friend_id_fkey" ("best_friend_id") REFERENCES "users" ("id");
```

For a full explanation of what problems it solves and how it solves them, see [docs/WHAT_IS_IT.md](docs/WHAT_IS_IT.md).

## Why are we doing this?

There's a number of reasons why we're rewriting DBSteward.

Firstly, it was originally written for PHP 5.2 or 5.3 (can't really remember). At that time, they had introduced classes, but not much else. Over time, the codebase evolved a little bit to take advantage of newer PHP features, but for the most part did not. Now, there's nothing wrong with PHP, and the latest versions of PHP are are very nice, but, PHP carries a lot of stigma, especially among teams that otherwise really want to use or contribute to DBSteward, but don't know or want to mess with managing a PHP environment for it.

Golang, on the other hand, is a very popular, very productive, and "modern" language (again, not that modern PHP isn't a modern langugage, but that's certainly the perception), meaning that it's a very low barrier of entry for anyone that wants to contribute. Furthermore, because Go generates self-contained statically linked binaries, users of DBSteward don't need to know anything about Go unless they want to build from source - they can just install a single pre-built binary and be off to the races.

Secondly, the existing DBSteward architecture is not very flexible and has a number of odd, undocumented features of dubious usefulness. The architecture as it stands is very hard to maintain, and while robust, fixing bugs or major issues (see "Quoting and Identifiers" below for an example) is extremely challenging. If we wanted to support e.g. Postgres 13 without breaking compatibility with Postgres 8 users, that would be nearly impossible without a monumental amount of work (which is what this undertaking is).

Simply rewriting and doing a straight port naturally doesn't fix many things (although distributing a single binary will sure be nice), but it does give us the opportunity to go through the whole codebase with a fine toothed comb, start making small changes, and recording thoughts that will allow us to make the big architectural shifts that I believe will make DBSteward a successful long-term, maintainable, and popular project.

## How can I help develop?

See [docs/DEVELOPING.md](docs/DEVELOPING.md)

## Thoughts for the future

See [docs/FUTURE.md](docs/FUTURE.md)