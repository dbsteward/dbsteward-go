# What is DBSteward?

DBSteward is a database schema change management tool, which works in a fundamentally different way than you're probably used to, but in doing so, solves a great number of common (and uncommon) problems that teams have with most DB change management tools.

## The problem it solves

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

### How DBSteward solves these problems

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
<!-- file schema.xml -->
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