# DBSteward (Go Edition)

## Architecture

1. `lib/reader` (TODO) converts xml/db schema definition into `lib/types` native data types
2. `lib/dialect/<dialect>`, produces a list of `lib.SQLStatement` for a single schema or by diffing two schemas, implemented by `lib/dialect/<dialect>/sql`
3. `lib/writer` (TODO) takes that list of `SQLStatement` and writes to a file or database
4. `cmd` implements the CLI tool

The major departures from PHP dbsteward are:

- sql producers (like `pgsql8_table`) consume native data types instead of xml nodes
  - don't need to do xml traversal to generate sql
  - easier testing
  - alternate config sources
  - direct db reading instead of needing to dump to file first
- sql producers produce a list of native data types instead of raw sql
  - don't need to do string manipulation to say "create a table"
  - easier testing
  - alternate execution strategies
  - ability to compact/reorder/etc sql before execution (e.g. several `ALTER TABLE` into a single one)
- schema differs use interfaces to fetch associated objects instead of traversing xml
  - allows different, more efficient implementations (e.g. diff live schema vs file)