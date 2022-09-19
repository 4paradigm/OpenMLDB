#include <iostream>
#include <openmldb_api.h>
#include <sdk/result_set.h>

int main()
{
      // init handler
      OpenmldbHandler handler("192.168.145.143", 6527);

      std::string db = "db_test";

      std::string sql = "create database " + db + ";";
      execute(handler, sql);

      sql = "use " + db + ";";
      execute(handler, sql);

      sql = "create table test_table ("
            "col1 string, col2 bigint,"
            "index(key=col1, ts=col2));";
      execute(handler, sql);

      sql = "insert test_table values(\"hello\", 1)";
      execute(handler, sql);
      sql = "insert test_table values(\"Hi~\", 2)";
      execute(handler, sql);

      // simple mode
      sql = "select * from test_table;";
      execute(handler, sql);
      auto res = get_resultset();
      print_resultset(res);

      // parameter mode
      sql = "select * from test_table where col1 = ? ;";
      ParameterRow para(&handler);
      para << "Hi~";
      execute_parameterized(handler, db, sql, para);
      res = get_resultset();

      // request mode
      sql = "select col1, sum(col2) over w as w_col2_sum from test_table "
            "window w as (partition by test_table.col1 order by test_table.col2 "
            "rows between 2 preceding and current row);";
      RequestRow req(&handler, db, sql);
      req << "Hi~" << 3l;
      execute_request(req);
      res = get_resultset();
      print_resultset(res);
}