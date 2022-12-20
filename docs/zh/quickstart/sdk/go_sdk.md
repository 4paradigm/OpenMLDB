# Go SDK 

**需求**:
- OpenMLDB 版本 >= 0.7.0
- 需要运行 API server 组件

## 1. 安装 Go SDK 包

```bash
go get github.com/4paradigm/OpenMLDB/go
```

## 2. 使用 Go SDK

### 2.1 连接 OpenMLDB

Go SDK 需要连接到 API server.

```go
db, err := sql.Open("openmldb", "openmldb://127.0.0.1:8080/test_db")
```

数据源 (DSN) 的格式为:

```
openmldb://API_SERVER_HOST[:API_SERVER_PORT]/DB_NAME
```

必须要连接到一个已经存在的数据库.

### 2.2 创建表

```go
db.ExecContext(ctx, "CREATE TABLE demo(c1 int, c2 string);")
```

### 2.3 插入数据

```go
db.ExecContext(ctx, `INSERT INTO demo VALUES (1, "bb"), (2, "bb");`)
```

### 2.4 查询

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

### 3. 完整示例

```go
package main

import (
	"context"
	"database/sql"

	// 加载 OpenMLDB SDK
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
