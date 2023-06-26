#include <ctime>
#include <iostream>
#include <string>

#include "openmldb_api.h"
#include "sdk/result_set.h"

int main() {
    // 创建并初始化 OpenmldbHandler 对象
    // 单机版：参数（ip, port），如：OpenmldbHandler handler("127.0.0.1", 6527)；
    // 集群版：参数（ip:port, path），如：OpenmldbHandler handler("127.0.0.1:2181", "/openmldb")；
    // 在此以单机版为示例。
    OpenmldbHandler handler("127.0.0.1:2181", "/openmldb");

    // 定义数据库名
    std::time_t t = std::time(0);
    std::string db = "test_db" + std::to_string(t);

    // 创建 SQL 语句，创建数据库
    std::string sql = "create database " + db + ";";
    // 执行 SQL 语句，execute() 函数返回 bool 值，值为 true 表示正确执行
    std::cout << handler.execute(sql);

    // 创建 SQL 语句，使用数据库
    sql = "use " + db + ";";
    std::cout << handler.execute(sql);

    // 创建 SQL 语句，创建表
    sql =
        "create table test_table ("
        "col1 string, col2 bigint,"
        "index(key=col1, ts=col2));";
    std::cout << handler.execute(sql);

    // 创建 SQL 语句，向表中插入行
    sql = "insert test_table values(\"hello\", 1)";
    std::cout << handler.execute(sql);
    sql = "insert test_table values(\"Hi~\", 2)";
    std::cout << handler.execute(sql);

    // 普通模式
    sql = "select * from test_table;";
    std::cout << handler.execute(sql);

    // 获得最近一次 SQL 的执行结果
    auto res = handler.get_resultset();
    // 输出 SQL 的执行结果
    print_resultset(res);
    // 本示例中输出应该为：
    //      +-------+--------+
    //      | col1  | col2 |
    //      +-------+--------+
    //      | hello | 1     |
    //      | Hi~   | 2     |
    //     +-------+---------+

    // 带参模式
    // SQL 语句中待填参数的位置用 ？ 来表示
    sql = "select * from test_table where col1 = ? ;";
    // 创建 ParameterRow 对象，用于填充参数
    ParameterRow para(&handler);
    // 填入参数
    para << "Hi~";
    // 执行 SQL 语句，execute_parameterized() 函数返回 bool 值，值为 true 表示正确执行
    handler.execute_parameterized(db, sql, para);
    res = handler.get_resultset();
    print_resultset(res);
    // 本示例中输出应该为：
    //      +------+--------+
    //      | col1 | col2 |
    //      +------+-------+
    //      | Hi~  | 2      |
    //      +------+--------+

    // 请求模式
    sql =
        "select col1, sum(col2) over w as w_col2_sum from test_table "
        "window w as (partition by test_table.col1 order by test_table.col2 "
        "rows between 2 preceding and current row);";
    RequestRow req(&handler, db, sql);
    req << "Hi~" << 3l;
    handler.execute_request(req);
    res = handler.get_resultset();
    print_resultset(res);
    // 本示例中输出应该为：
    //      +------+--------------------+
    //      | col1 | w_col2_sum |
    //      +------+--------------------+
    //      | Hi~  | 5                 |
    //      +------+--------------------+
}