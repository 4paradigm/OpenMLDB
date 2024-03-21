# Quick Start

This article will introduce how to quickly get started with OpenM(ysq)LDB.

For installation and deployment, please refer to [OpenMLDB Deployment Document](../../../deploy/index.rst)
and [OpenM(ysq)LDB Deployment Document](./install.md).

## Use a compatible MySQL command line

After deploying the OpenMLDB distributed cluster, developers do not need to install additional OpenMLDB command line
tools. Using the pre-installed MySQL command line tool, they can directly connect to the OpenMLDB cluster for testing (
note that the following SQL connections and execution results are all returned by OpenMLDB cluster, not remote MySQL
service).

![mysql-cli-1.png](./images/mysql-cli-1.png)

By executing customized OpenMLDB SQL, we can not only view the OpenMLDB cluster status, but also switch between offline
mode and online mode to realize the offline and online feature extraction functions of MLOps.

![mysql-cli-2.png](./images/mysql-cli-2.png)

## 使用兼容的 JDBC 驱动

Java developer generally use the MySQL JDBC driver to connect to MySQL. The same code can directly connect to the
OpenMLDB cluster without any modification.

Write the Java application code as follows. Pay attention to modify the IP, port and username and password information
according to the actual cluster situation.

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

Then compile and execute, and you can see the queried OpenMLDB database data in the command line output.

![jdbc.png](./images/jdbc.png)

## Use compatible SQLAlchemy driver

Python developer often use SQLAlchemy and MySQL drivers, and the same code can also be directly used to query OpenMLDB's
online data.

Write the Python application code as follows:

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

Then execute it directly, and you can also see the corresponding OpenMLDB database output on the command line.

![sqlalchemy.png](./images/sqlalchemy.png)

## Use compatible Go MySQL driver

Golang developer generally use the officially recommended github.com/go-sql-driver/mysql driver to access MySQL. They
can also directly access the OpenMLDB cluster without modifying the application code.

Write the Golang application code as follows:

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

Compile and run directly, and you can view the database output results on the command line.

![go.png](./images/go.png)

## Use a compatible Sequel Ace client

MySQL users commonly use GUI applications to simplify database management. If users want to connect to an OpenMLDB
cluster, they can also use such open source GUI tools.

Taking Sequel Ace as an example, users do not need to modify any project code. They only need to fill in the address and
port of the OpenM(ysq)LDB service when connecting to the database, and fill in the username and password of the OpenMLDB
service as the username and password. Then you can follow MySQL operation method to access the OpenMLDB service.

![sequel_ace.png](./images/sequel_ace.png)

## Use a compatible Navicat client

In addition to Sequel Ace, Navicat is also a popular MySQL client. Users do not need to modify any project code. They
only need to fill in the address and port of the OpenM (ysq) LDB service when creating a new connection (MySQL), and
fill in the user name and password. The username and password of the OpenMLDB service can be used to access the OpenMLDB
service according to the MySQL operation method.

![navicat.png](./images/navicat.png)