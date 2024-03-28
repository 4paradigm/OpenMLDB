# 快速入门

本文将介绍如何快速入门 OpenM(ysq)LDB。

安装部署可参考 [OpenMLDB 部署文档](../../../deploy/index.rst) 和 [OpenM(ysq)LDB 部署文档](./install.md)。

## 使用兼容的 MySQL 命令行

部署 OpenMLDB 分布式集群以后，开发者不需要额外安装 OpenMLDB 命令行工具，使用预安装好的 MySQL 命令行工具，可直接连接 OpenMLDB 集群进行测试（注意，下面的 SQL 连接以及执行结果，均在 OpenMLDB 集群中返回，并非远端 MySQL 服务）。

![mysql-cli-1.png](./images/mysql-cli-1.png)

通过执行定制化的 OpenMLDB SQL，我们不仅可以查看 OpenMLDB 集群状态，还可以切换离线模式和在线模式，实现 MLOps 的离线和在线特征抽取功能。

![mysql-cli-2.png](./images/mysql-cli-2.png)

## 使用兼容的 JDBC 驱动

Java 用户一般使用 MySQL JDBC 驱动来连接 MySQL，同样的代码不需要任何修改就可以直连 OpenMLDB 集群。

编写 Java 应用代码如下，注意根据实际集群情况修改IP、端口以及用户名密码信息。

```java
public class Main {
    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3307/db1";
        String user = "root";
        String password = "root";

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            connection = DriverManager.getConnection(url, user, password);
            statement = connection.createStatement();

            resultSet = statement.executeQuery("SELECT * FROM db1.t1");

            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                System.out.println("ID: " + id + ", Name: " + name);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // Close the result set, statement, and connection
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
```

然后编译执行即可，可以在命令行输出中看到查询的 OpenMLDB 数据库数据。

![jdbc.png](./images/jdbc.png)

## 使用兼容的 SQLAlchemy 驱动

Python 用户常使用 SQLAlchemy 和 MySQL 驱动，同样的代码也可以直接用于查询 OpenMLDB 的在线数据。

编写 Python 应用代码如下：

```python
from sqlalchemy import create_engine, text

def main():
    engine = create_engine("mysql+pymysql://root:root@127.0.0.1:3307/db1", echo=True)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM db1.t1"))
        for row in result:
            print(row)

if __name__ == "__main__":
  main()
```

然后直接执行，在命令行也可以看到对应的 OpenMLDB 数据库输出。

![sqlalchemy.png](./images/sqlalchemy.png)

## 使用兼容的 Go MySQL 驱动

Golang 用户一般官方推荐的 github.com/go-sql-driver/mysql 驱动来访问 MySQL，同样不需要修改应用代码就可以直接访问 OpenMLDB 集群。

编写 Golang 应用代码如下：

```go
package main

import (
        "database/sql"
        "fmt"
        "log"

        _ "github.com/go-sql-driver/mysql"
)

func main() {
        // MySQL database connection parameters
        dbUser := "root"         // Replace with your MySQL username
        dbPass := "root"         // Replace with your MySQL password
        dbName := "db1"    // Replace with your MySQL database name
        dbHost := "localhost:3307"        // Replace with your MySQL host address
        dbCharset := "utf8mb4"            // Replace with your MySQL charset

        // Create a database connection
        db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=%s", dbUser, dbPass, dbHost, dbName, dbCharset))
        if err != nil {
                log.Fatalf("Error connecting to the database: %v", err)
        }
        defer db.Close()

        // Perform a simple query
        rows, err := db.Query("SELECT id, name FROM db1.t1")
        if err != nil {
                log.Fatalf("Error executing query: %v", err)
        }
        defer rows.Close()

        // Iterate over the result set
        for rows.Next() {
                var id int
                var name string
                if err := rows.Scan(&id, &name); err != nil {
                        log.Fatalf("Error scanning row: %v", err)
                }
                fmt.Printf("ID: %d, Name: %s\n", id, name)
        }
        if err := rows.Err(); err != nil {
                log.Fatalf("Error iterating over result set: %v", err)
        }
}
```

直接编译并运行，即可在命令行查看数据库输出的结果。

![go.png](./images/go.png)

## 使用兼容的 Sequel Ace 客户端

MySQL 用户常用 GUI 应用来简化数据库的管理，用户如果想要连接 OpenMLDB 集群，也可以使用这类开源的 GUI 工具。

以 Sequel Ace 为例，用户不需要修改任何项目代码，只需要在连接数据库时，地址和端口填写 OpenM(ysq)LDB服务的地址和端口，用户名和密码填写OpenMLDB服务的用户名和密码，即可按照 MySQL 的操作方法来访问 OpenMLDB 服务。

![sequel_ace.png](./images/sequel_ace.png)

## 使用兼容的 Navicat 客户端

除了 Sequel Ace ，Navicat 也是比较受欢迎的MySQL客户端，用户不需要修改任何项目代码，只需要新建连接（MySQL)时，地址和端口填写 OpenM(ysq)LDB服务的地址和端口，用户名和密码填写OpenMLDB服务的用户名和密码，即可按照 MySQL 的操作方法来访问 OpenMLDB 服务。

![navicat.png](./images/navicat.png)