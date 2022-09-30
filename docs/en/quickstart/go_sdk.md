# Go SDK Quickstart

**Requirements**:
- OpenMLDB Version >= 0.7.0
- API server component is running

## 1. Install the Go SDK Package

```bash
go get github.com/4paradigm/OpenMLDB/go
```

## 2. API

### 2.1 Connect

Go SDK connects to API server.

```go
db, err := sql.Open("openmldb", "openmldb://127.0.0.1:8080/test_db")
```

The DSN schema is

```
openmldb://API_SERVER_HOST[:API_SERVER_PORT]/DB_NAME
```

Note that an existed database is required.

### 2.2 Create Table

```go
db.ExecContext(ctx, "CREATE TABLE demo(c1 int, c2 string);")
```

### 2.3 Insert Value

```go
db.ExecContext(ctx, `INSERT INTO demo VALUES (1, "bb"), (2, "bb");`)
```

### 2.4 Query

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

### 3. An Example

```go
package main

import (
	"context"
	"database/sql"

	// register openmldb driver
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
