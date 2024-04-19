# Embedded DBSteward

Everything in this document is preliminary, experimental, and probably insane.
Proceed with courage.

## Defining tables

Tables are defined by first creating a struct that matches the desired
table layout. For example:

```go
type People struct {
    ID   int
    Name string
}
```
This will result in the following table:
```sql
CREATE TABLE "People" (
    "ID"   INT,
    "Name" TEXT
);
```
Obviously, there's a lot of desirable stuff missing here. This can be
remedied using struct tags:
```go
type People struct {
    ID   int    `dbsteward:"id,BIGSERIAL,PRIMARY KEY"`
    Name string `dbsteward:"name"`
}
```
Which will result in:
```sql
CREATE TABLE "People" (
    id   BIGSERIAL PRIMARY KEY,
    name TEXT
);
```
