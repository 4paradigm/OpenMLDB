# [Alpha] C++ SDK
```plain
The current functionality support of the C++ SDK is not yet complete. It is currently only recommended for development, testing, or specific use cases. It is not recommended for use in a production environment. For production use, we recommend using the Java SDK, which has the most comprehensive feature coverage and has undergone extensive testing for both functionality and performance.
```
## C++ SDK Compilation and Installation
```plain
The C++ SDK static library is only supported on Linux systems and is not included in the standard release. If you need to use the C++ SDK library, you should compile the source code and enable the compilation option `INSTALL_CXXSDK=ON`.
```
To compile, you need to meet the [hardware requirements](../../deploy/compile.md#hardware-requirements) and install the necessary [dependencies](../../deploy/compile.md#dependencies).
```plain
git clone git@github.com:4paradigm/OpenMLDB.git
cd OpenMLDB
make INSTALL_CXXSDK=ON && make install
```

## User Code 

The following code demonstrates the basic use of C++ SDK. `openmldb_api.h` and `sdk/result_set.h` is the header file that must be included.

```c++
#include <ctime>
#include <iostream>
#include <string>

#include "openmldb_api.h"
#include "sdk/result_set.h"

int main()
{
      //Create and initialize the OpenmldbHandler object
      //Stand-alone version: parameter (ip, port), such as: OpenmldbHandler handler ("127.0.0.1", 6527);
      //Cluster version: parameters (ip: port, path), such as: OpenmldbHandler handler ("127.0.0.1:6527", "/openmldb");
      //Take the stand-alone version as an example.
      OpenmldbHandler handler("127.0.0.1", 6527);

      // Define database name
      std::time_t t = std::time(0);
      std::string db = "test_db" + std::to_string(t);

      // Create SQL statement and database
      std::string sql = "create database " + db + ";";
      // Execute the SQL statement. The execute() function returns bool. true indicates correct execution
      std::cout << execute(handler, sql);

      // Create SQL statement to use database
      sql = "use " + db + ";";
      std::cout << execute(handler, sql);

      // Create SQL statement to create table
      sql = "create table test_table ("
            "col1 string, col2 bigint,"
            "index(key=col1, ts=col2));";
      std::cout << execute(handler, sql);

      // Create SQL statements to insert rows into the table
      sql = "insert test_table values(\"hello\", 1)";
      std::cout << execute(handler, sql);
      sql = "insert test_table values(\"Hi~\", 2)";
      std::cout << execute(handler, sql);

      // Basic mode
      sql = "select * from test_table;";
      std::cout << execute(handler, sql);
      
      // Get the latest SQL execution result
      auto res = get_resultset();
      // Output SQL execution results
      print_resultset(res);
      // The output in this example should be：
      //      +-------+--------+
      //      | col1  | col2 |
      //      +-------+--------+
      //      | hello | 1     |
      //      | Hi~   | 2     |
      //     +-------+---------+



      // Parameter mode
      //The parameters to be filled in the SQL statement is marked as "?"
      sql = "select * from test_table where col1 = ? ;";
      // Create a ParameterRow object for filling parameters
      ParameterRow para(&handler);
      // Fill in parameters
      para << "Hi~";
      // Execute SQL statement, execute_parameterized() function returns bool. true indicates correct execution
      execute_parameterized(handler, db, sql, para);
      res = get_resultset();
      print_resultset(res);
      // The output in this example should be：
      //      +------+--------+
      //      | col1 | col2 |
      //      +------+-------+
      //      | Hi~  | 2      |
      //      +------+--------+


      // Request mode
      sql = "select col1, sum(col2) over w as w_col2_sum from test_table "
            "window w as (partition by test_table.col1 order by test_table.col2 "
            "rows between 2 preceding and current row);";
      RequestRow req(&handler, db, sql);
      req << "Hi~" << 3l;
      execute_request(req);
      res = get_resultset();
      print_resultset(res);
      // The output in this example should be：
      //      +------+--------------------+
      //      | col1 | w_col2_sum |
      //      +------+--------------------+
      //      | Hi~  | 5                 |
      //      +------+--------------------+
}
```
## Multi-Thread
The `OpenMLDBHandler` object is not thread-safe, but the internal connection to the `SQLClusterRouter` can be used multi-threaded. You can achieve multi-threading by sharing the Router within the Handler object, which is more efficient than creating multiple independent Handler instances (each with its independent Router). However, in a multi-threaded mode, you should be cautious because interfaces without db depend on the Router's internal cache of used db, which might be modified by other threads. It's advisable to use the db interface in such cases. The following code demonstrates a method for multi-threaded usage:

```c++
OpenmldbHandler h1("127.0.0.1:2181", "/openmldb");
OpenmldbHandler h2(h1.get_router());

std::thread t1([&](){ h1.execute("show components;"); print_resultset(h1.get_resultset());});

std::thread t2([&](){ h2.execute("show table status;"); print_resultset(h2.get_resultset());});

t1.join();
t2.join();
```

## Compile and run
You can refer to [Makefile](https://github.com/4paradigm/OpenMLDB/blob/main/demo/cxx_quickstart/Makefile) or use the command below to compile and run the sample code.

```bash
gcc <user_code>.cxx -o <bin_name> -lstdc++ -std=c++17 -I<install_path>/include  -L<install_path>/lib -lopenmldbsdk -lpthread -lm -ldl -lstdc++fs

./<bin_name>
```

