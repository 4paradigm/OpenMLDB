# [Alpha] Go SDK

```{warning}
Go SDK 目前功能支持上并不完善，目前仅用于开发测试或者特殊用途，不推荐在实际生产环境中使用。生产环境推荐使用 Java SDK，功能覆盖最完善，并且在功能、性能上都经过了充分测试。
```

## 需求

- OpenMLDB 版本：>= v0.6.2
- 部署并且运行 APIServer (参考 [APIServer 部署文档](https://openmldb.ai/docs/zh/main/deploy/install_deploy.html#apiserver)）

## Go SDK 包安装

```Bash
go get github.com/4paradigm/openmldb-go-sdk
```

## 使用 Go SDK

本节介绍 Go SDK 的基本使用。

### 连接 OpenMLDB

Go SDK 需要连接到 API server。

```Go
db, err := sql.Open("openmldb", "openmldb://127.0.0.1:8080/test_db")
```

数据源 (DSN) 的格式为：

```Plain
openmldb://API_SERVER_HOST[:API_SERVER_PORT]/DB_NAME
```

必须要连接到一个已经存在的数据库。

### 创建表

创建表 `demo`：

```Go
db.ExecContext(ctx, "CREATE TABLE demo(c1 int, c2 string);")
```

### 插入数据

向表中插入一条数据：

```Go
db.ExecContext(ctx, `INSERT INTO demo VALUES (1, "bb"), (2, "bb");`)
```

### 查询

```Go
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

## 示例

```Go
package main

import (
  "context"
  "database/sql"

  // 加载 OpenMLDB SDK
  _ "github.com/4paradigm/openmldb-go-sdk"
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
