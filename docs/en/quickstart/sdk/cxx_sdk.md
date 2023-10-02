# C++ SDK

## C++SDK package compilation and installation

```plain
git clone git@github.com:4paradigm/OpenMLDB.git
cd OpenMLDB
make && make install
```

## Write user code

The following code demonstrates the basic use of C++ SDK. openmldb_api.h and sdk/result_set.h is the header file that must be included.

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
      // Execute the SQL statement. The execute() function returns the bool value. A value of true indicates correct execution
      std::cout << execute(handler, sql);

      // Create SQL statement and use database
      sql = "use " + db + ";";
      std::cout << execute(handler, sql);

      // Create SQL statement and create table
      sql = "create table test_table ("
            "col1 string, col2 bigint,"
            "index(key=col1, ts=col2));";
      std::cout << execute(handler, sql);

      // Create SQL statements and insert rows into the table
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



      // Band-parameter mode
    //The position of the parameters to be filled in the SQL statement is set to "?" to express
      sql = "select * from test_table where col1 = ? ;";
      // Create a ParameterRow object for filling parameters
      ParameterRow para(&handler);
      // Fill in parameters
      para << "Hi~";
      // Execute SQL statement execute_parameterized() function returns the bool value. A value of true indicates correct execution
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

## Compile and run

```plain
gcc <user_code>.cxx -o <bin_name> -lstdc++ -std=c++17 -I<install_path>/include  -L<install_path>/lib -lopenmldbsdk -lpthread
./<bin_name>
```

