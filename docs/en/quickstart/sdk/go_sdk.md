# Go SDK

## Requirement

- OpenMLDB version: >= v0.6.2

- Deploy and run APIServer (refer to [APIServer deployment](https://openmldb.ai/docs/zh/main/deploy/install_deploy.html#apiserver) document)

## Go SDK package installment

```bash
go get github.com/4paradigm/OpenMLDB/go
```

## Go SDK usage

This section describes the basic use of Go SDK.

### Connect to OpenMLDB

The Go SDK needs to be connected to the API server.

```Go
db, err := sql.Open("openmldb", "openmldb://127.0.0.1:8080/test_db")
```

The format of data source (DSN) is:

```plain
openmldb://API_SERVER_HOST[:API_SERVER_PORT]/DB_NAME
```

You must connect to an existing database.

### Create Table

Create a table `demo`:

```Go
db.ExecContext(ctx, "CREATE TABLE demo(c1 int, c2 string);")
```

### Insert data

Insert date into table:

```go
db.ExecContext(ctx, `INSERT INTO demo VALUES (1, "bb"), (2, "bb");`)
```

### Query

```go
rows, err := db.QueryContext(ctx, `SELECT c1, c2 FROM demo;`)
if err != nil{
  panic(err)
}

var col1 int
var col2 string

for rows.Next() {
  if err := rows.Scan(&col1, &col2); err != nil {
    panic(err)
  }
  // process row ...
}
```

## Example

```Go
package main

import (
  "context"
  "database/sql"

  // Load OpenMLDB SDK
  _ "github.com/4paradigm/OpenMLDB/go"
)

func main() {
  db, err := sql.Open("openmldb", "openmldb://127.0.0.1:8080/test_db")
  if err != nil {
    panic(err)
  }

  defer db.Close()

  ctx := context.Background()

  if _, err := db.ExecContext(ctx, `CREATE TABLE demo (c1 int, c2 string);`); err != nil {
    panic(err)
  }

  if _, err := db.ExecContext(ctx, `INSERT INTO demo VALUES (1, "bb"), (2, "bb");`); err != nil {
    panic(err)
  }

  rows, err := db.QueryContext(ctx, `SELECT c1, c2 FROM demo;`)
  if err != nil{
    panic(err)
  }

  var col1 int
  var col2 string

  for rows.Next() {
    if err := rows.Scan(&col1, &col2); err != nil {
      panic(err)
    }
    println(col1, col2)
  }
}
```

