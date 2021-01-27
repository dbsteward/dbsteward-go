# Examples

This folder contains example applications of DBSteward.

At the moment, this will be evolving as we implement more functionality. The diffs between two commits will show what's changed about sql generation in that commit.

Can run everything with `example/run all`, which:

1. Wipes out all generated files in the example directory
2. Builds a schema definition from v1 of our database: `someapp_v1.xml` -> `someapp_v1_build.sql`
3. Builds an upgrade ("diff") from v1 to v2 `someapp_v1.xml` + `someapp_v2.xml` -> `someapp_v2_upgrade*.sql`
4. Loads v1 into a real database using docker
5. Loads the v2 upgrade into the database
6. Extracts the v2 schema into `someapp_v2_extract.xml`
7. Diffs `someapp_v2.xml` and `someapp_v2_extract.xml`. If everything is working well, this will contain no changes since they both represent the same state of the database.

## Build

This generates the whole schema:

```
$ go run . --xml example/someapp_v1.xml 
10:37AM INF DBSteward Version 2.0.0
10:37AM INF XML file(s) are targetd for sqlformat=pgsql8
10:37AM INF Using sqlformat=pgsql8
10:37AM INF Compositing XML files...
10:37AM INF Loading XML example/someapp_v1.xml...
10:37AM INF Compositing XML example/someapp_v1.xml
10:37AM INF XML files example/someapp_v1.xml composited
10:37AM INF Saving composite as example/someapp_v1_composite.xml
10:37AM INF Building complete file example/someapp_v1_build.sql
10:37AM INF [File Segment] Fixed output file: example/someapp_v1_build.sql
10:37AM INF Calculating table foreign dependency order...
10:37AM INF Detected LANGUAGE SQL function public.destroy_session referring to table public.session_information in the database definition
10:37AM INF Defining structure
10:37AM INF Defining data inserts
10:37AM INF Done
```

Creates:
- `someapp_v1_composite.xml`: Contains expanded and converted XML that DBSteward logically operates on
- `someapp_v1_build.sql`: The SQL to generate the database

## Diff

This creates an upgrade from v1 to v2:

```
$ go run . --oldxml example/someapp_v1.xml --newxml example/someapp_v2.xml
10:37AM INF DBSteward Version 2.0.0
10:37AM INF XML file(s) are targetd for sqlformat=pgsql8
10:37AM INF Using sqlformat=pgsql8
10:37AM INF Compositing old XML files...
10:37AM INF Loading XML example/someapp_v1.xml...
10:37AM INF Compositing XML example/someapp_v1.xml
10:37AM INF Old XML files example/someapp_v1.xml composited
10:37AM INF Compositing new XML files...
10:37AM INF Loading XML example/someapp_v2.xml...
10:37AM INF Compositing XML example/someapp_v2.xml
10:37AM INF New XML files example/someapp_v2.xml composited
10:37AM INF Saving composite as example/someapp_v1_composite.xml
10:37AM INF Saving composite as example/someapp_v2_composite.xml
10:37AM INF Calculating old table foreign key dependency order...
10:37AM INF Calculating new table foreign key dependency order...
10:37AM INF [File Segment] Opening output file segment example/someapp_v2_upgrade_stage1_schema1.sql
10:37AM INF [File Segment] Opening output file segment example/someapp_v2_upgrade_stage2_data1.sql
10:37AM INF Drop Old Schemas
10:37AM INF Create New Schemas
10:37AM INF Update Structure
10:37AM INF Update Permissions
10:37AM INF [File Segment] Opening output file segment example/someapp_v2_upgrade_stage3_schema1.sql
10:37AM INF Update data
10:37AM INF [File Segment] Opening output file segment example/someapp_v2_upgrade_stage4_schema1.sql
10:37AM INF Done
```

Creates
- `someapp_v1_composite.xml` and `someapp_v2_composite.xml`, as in Clean Build
- `someapp_v2_upgrade_stage1_schema1.sql`: Stage 1 creates and updates new schema objects
- `someapp_v2_upgrade_stage2_data1.sql`: Stage 2 drops old data
- `someapp_v2_upgrade_stage3_schema1.sql`: Stage 3 drops old schema objects, alters others
- `someapp_v2_upgrade_stage4_data1.sql`: Stage 4 creates and updates new data