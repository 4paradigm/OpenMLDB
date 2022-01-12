# OpenMLDB 快速上手指南（单机模式）

本教程提供基于单机模式的快速上手体验，通过建立数据库、导入数据、离线特征计算、SQL 方案上线、在线实时特征计算，演示OpenMLDB 的基本使用流程。

## 1. 准备工作

> :warning: docker engine版本需求 >= 18.03

本教程均基于 OpenMLDB CLI 进行开发和部署，因此首先需要下载样例数据并且启动 OpenMLDB CLI。我们推荐使用准备好的 docker 镜像来快速体验使用：

1. 拉取镜像（镜像下载大小大约 500 MB，解压后约 1.3 GB）和启动 docker 容器

   ```bash
   docker run -it 4pdosc/openmldb:0.3.2 bash
   ```

   :bulb: **成功启动容器以后，以下命令均在容器内执行。**

2. 下载样例数据

   ```bash
   curl https://raw.githubusercontent.com/4paradigm/OpenMLDB/main/demo/standalone/data/data.csv --output ./data/data.csv
   ```

3. 启动 OpenMLDB 服务和 CLI

   ```bash
   # 1. initialize the environment
   ./init.sh standalone
   # 2. Start the OpenMLDB CLI for the standalone mode
   ../openmldb/bin/openmldb --host 127.0.0.1 --port 6527
   ```

以下截图显示了以上 docker 内命令正确执行以及 OpenMLDB CLI 正确启动以后的画面

![image-20211209133608276](../../images/cli.png)

## 2. 使用指南

:bulb: 以下演示的命令如无特别说明，默认均在 OpenMLDB CLI 下执行（CLI 命令以提示符 `>` 开头以作区分）。

### 2.1. 创建数据库和表

```sql
> CREATE DATABASE demo_db;
> USE demo_db;
> CREATE TABLE demo_table1(c1 string, c2 int, c3 bigint, c4 float, c5 double, c6 timestamp, c7 date, INDEX(key=c1, ts=c6));
```
`CREATE TABLE` 的 `INDEX` 函数接受两个重要的参数  `key` 和 `ts`。关于这两个重要参数的如下说明：

- `key` 代表了索引列。如果创建表的时候不确定，也可以不进行指定，系统将默认使用第一个符合条件的列作为索引列。在部署上线的过程中，OpenMLDB 将会根据实际使用的 SQL 脚本，自动进行优化和创建所需要的索引。
- `ts` 列是指定的有序列，并且用来作为 `ORDER BY` 的列。只有 `timestamp` 或者 `bigint` 类型的列才能作为 `ts` 列。

### 2.2. 导入数据
 导入之前下载的样例数据（在 [1. 准备工作](#1-准备工作) 中已经下载）作为离线数据，用于特征计算。
```sql
> LOAD DATA INFILE 'data/data.csv' INTO TABLE demo_table1;
```
### 2.3. 离线特征计算

如下的 SQL 脚本将会执行特征抽取脚本，并且将生成的特征存储在一个文件中，供后续的模型训练使用。

```sql
> SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature.csv';
```
### 2.4. SQL 方案上线
将探索好的SQL方案部署到线上，注意部署上线的 SQL 方案需要与对应的离线特征计算的 SQL 方案保持一致。
```sql
> DEPLOY demo_data_service SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
```
上线后可以通过命令 `SHOW DEPLOYMENTS` 查看已部署的 SQL 方案；也可以使用 `DROP DEPLOYUMENT` 命令将已上线方案删除。

:bulb: 注意，在本例子中，我们使用了同一份数据做线下和线上特征计算。在实际部署中，往往需要倒入另一份最新的数据来做 SQL 方案上线。

### 2.5 退出 CLI

```sql
> quit;
```

至此我们已经完成了全部基于 OpenMLDB CLI 的开发部署工作，并且已经回到了操作系统命令行下。

### 2.6. 实时特征计算

实时线上服务可以通过如下 Web API 提供服务：
```
http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service
        \___________/      \____/              \_____________/
              |               |                        |
        APIServer地址     Database名字            Deployment名字
```
实时请求的输入数据接受 `json` 格式，我们把一行数据放到请求的 `input` 域中。如下示例:

```bash
curl http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service -X POST -d'{"input": [["aaa", 11, 22, 1.2, 1.3, 1635247427000, "2021-05-20"]]}'
```

如下为该查询预期的返回结果（计算得到的特征被存放在 `data` 域）：

```json
{"code":0,"msg":"ok","data":{"data":[["aaa",11,22]],"common_cols_data":[]}}
```

## 3. Demo 

我们也提供了一个更为完整的实际应用开发 demo 来演示整个开发和上线流程，具体运行步骤可以在此[查看](https://github.com/4paradigm/OpenMLDB/tree/main/demo)。

