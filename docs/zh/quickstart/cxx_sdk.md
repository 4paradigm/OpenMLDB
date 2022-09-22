# OpenMLDB C++ SDK 快速上手

# 请先安装 C++ SDK 包

# 部署 OpenMLDB 
# 详细文件配置及步骤请参考：https://openmldb.ai/docs/zh/main/deploy/install_deploy.html
# 1. 配置环境
# 1.1 关闭操作系统 swap :     swapoff --all
# 1.2 关闭THP(Transparent Huge Pages) :
#       $ echo 'never' > /sys/kernel/mm/transparent_hugepage/enabled
#       $ echo 'never' > /sys/kernel/mm/transparent_hugepage/defrag

# 2. 部署 server

# 2.1 部署单机版
# （1）启动 tablet
#     sh bin/start.sh start standalone_tablet
# （2）启动nameserver
#     sh bin/start.sh start  standalone_nameserver
# （3）检查服务是否启动
#     $ ./bin/openmldb --host=172.27.128.33 --port=6527
# （4）启动 apiserver  （APIServer负责接收http请求，转发给OpenMLDB并返回结果。它是无状态的，而且并不是OpenMLDB必须部署的组件。 运行前需确保OpenMLDB cluster已经启动，否则APIServer将初始化失败并退出进程）
#     sh bin/start.sh start standalone_apiserver

# 2.2 部署集群版
# （1）部署zookeeper
#     sh bin/zkServer.sh start
#     （部署zookeeper集群参考：https://zookeeper.apache.org/doc/r3.4.14/zookeeperStarted.html#sc_RunningReplicatedZooKeeper）
# （2）启动 tablet
#     sh bin/start.sh start tablet
# （3）启动 nameserver
#     sh bin/start.sh start nameserver
# （4）检查服务是否启动
#     ./bin/openmldb --zk_cluster=172.27.128.31:7181,172.27.128.32:7181,172.27.128.33:7181 --zk_root_path=/openmldb_cluster --role=ns_client
# （5）启动 apiserver   （APIServer负责接收http请求，转发给OpenMLDB并返回结果。它是无状态的，而且并不是OpenMLDB必须部署的组件。 运行前需确保OpenMLDB cluster已经启动，否则APIServer将初始化失败并退出进程）
#     sh bin/start.sh start apiserver
# （6）启动 taskmanager
#     bin/start.sh start taskmanager
# （7）检查服务是否启动
#     ./bin/openmldb --zk_cluster=172.27.128.31:7181,172.27.128.32:7181,172.27.128.33:7181 --zk_root_path=/openmldb_cluster --role=sql_client


# 编译 C++ 代码文件：
# gcc XXX.cxx -o XXX -lstdc++ -std=c++17 -I.../include  -L.../lib -lopenmldbsdk -lpthread



# openmldb_api.h 和 sdk/result_set.h 是必须 include 的头文件，其中包含了使用 OpenMLDB 需要的各种函数。
# 由于文件包含关系，不能单独 include openmldb_api.h。

#include <openmldb_api.h>
#include <sdk/result_set.h>
#include <ctime>
#include <iostream>
#include <string>

int main()
{
      // 创建并初始化 OpenmldbHandler 对象
      // 单机版：参数（ip, port），如：OpenmldbHandler handler("127.0.0.1", 6527)；
      // 集群版：参数（ip:port, path），如：OpenmldbHandler handler("127.0.0.1:6527", "/openmldb")；
      // 在此以单机版为示例。
      OpenmldbHandler handler("127.0.0.1", 6527);

      // 定义数据库名
      std::time_t t = std::time(0);
      std::string db = "test_db" + std::to_string(t);

      // 创建 SQL 语句，创建数据库
      std::string sql = "create database " + db + ";";
      // 执行 SQL 语句，execute() 函数返回 bool 值，值为 true 表示正确执行
      std::cout << execute(handler, sql);

      // 创建 SQL 语句，使用数据库
      sql = "use " + db + ";";
      std::cout << execute(handler, sql);

      // 创建 SQL 语句，创建表
      sql = "create table test_table ("
            "col1 string, col2 bigint,"
            "index(key=col1, ts=col2));";
      std::cout << execute(handler, sql);

      // 创建 SQL 语句，向表中插入行
      sql = "insert test_table values(\"hello\", 1)";
      std::cout << execute(handler, sql);
      sql = "insert test_table values(\"Hi~\", 2)";
      std::cout << execute(handler, sql);

      // 普通模式
      sql = "select * from test_table;";
      std::cout << execute(handler, sql);
      // 获得最近一次 SQL 的执行结果
      auto res = get_resultset();
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
      execute_parameterized(handler, db, sql, para);
      res = get_resultset();
      print_resultset(res);
      // 本示例中输出应该为：
      //      +------+--------+
      //      | col1 | col2 |
      //      +------+-------+
      //      | Hi~  | 2      |
      //      +------+--------+


      // 请求模式
      sql = "select col1, sum(col2) over w as w_col2_sum from test_table "
            "window w as (partition by test_table.col1 order by test_table.col2 "
            "rows between 2 preceding and current row);";
      RequestRow req(&handler, db, sql);
      req << "Hi~" << 3l;
      execute_request(req);
      res = get_resultset();
      print_resultset(res);
      // 本示例中输出应该为：
      //      +------+--------------------+
      //      | col1 | w_col2_sum |
      //      +------+--------------------+
      //      | Hi~  | 5                 |
      //      +------+--------------------+
}