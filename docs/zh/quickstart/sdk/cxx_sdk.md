# [Alpha] C++ SDK

```{warning}
C++ SDK 目前功能支持上并不完善，不支持多线程，目前仅用于开发测试或者特殊用途，不推荐在实际生产环境中使用。生产环境推荐使用 Java SDK，功能覆盖最完善，并且在功能、性能上都经过了充分测试。
```

## C++ SDK 包编译安装

```{warning}
C++ SDK 静态库仅支持Linux系统，且不在标准发布中。如果需要使用 C++ SDK 库，请源码编译，并打开编译选项`INSTALL_CXXSDK=ON`。
```
编译需要满足 [硬件要求](../../deploy/compile.md#硬件要求)，安装 [依赖工具](../../deploy/compile.md#依赖工具)。

```bash
git clone git@github.com:4paradigm/OpenMLDB.git
cd OpenMLDB
make INSTALL_CXXSDK=ON && make install
```
编译完成后，会在 install 目录下生成`include`头文件目录和`lib`静态库目录。

## 编写用户代码

以下代码演示 C++ SDK 的基本使用。openmldb_api.h 和 sdk/result_set.h 是必须 include 的头文件。

```c++
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
    if (!handler.is_connected()) {
        std::cout << "connect failed" << std::endl;
        return -1;
    }
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
```


## 多线程使用

`OpenMLDBHandler` 对象是线程不安全的，但内部连接`SQLClusterRouter`可以在多线程中使用，所以可以通过共享Handler中的Router来实现多线程，比独立创建多个Handler（多个独立Router）更合适。但需要注意多线程模式下，无db的接口依赖Router内部缓存的used db，可能被其他线程修改，请使用db接口。以下代码演示了多线程使用的方法。

```c++
OpenmldbHandler h1("127.0.0.1:2181", "/openmldb");
OpenmldbHandler h2(h1.get_router());

std::thread t1([&](){ h1.execute("show components;"); print_resultset(h1.get_resultset());});

std::thread t2([&](){ h2.execute("show table status;"); print_resultset(h2.get_resultset());});

t1.join();
t2.join();
```

## 编译与运行

可参考[编译Makefile](https://github.com/4paradigm/OpenMLDB/blob/main/demo/cxx_quickstart/Makefile)或直接使用以下命令，编译并运行示例代码。

```bash
gcc <user_code>.cxx -o <bin_name> -lstdc++ -std=c++17 -I<install_path>/include  -L<install_path>/lib -lopenmldbsdk -lpthread -lm -ldl -lstdc++fs

./<bin_name>
```
