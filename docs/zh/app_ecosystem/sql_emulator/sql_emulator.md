# OpenMLDB SQL Emulator

OpenMLDB SQL Emulator 是一个[OpenMLDB](https://github.com/4paradigm/OpenMLDB)的轻量级的SQL模拟器，旨在于更加高效方便的开发、调试 OpenMLDB SQL。

为了高效的实现时序特征计算，OpenMLDB SQL对标准SQL做了改进和扩展，因此初学者在使用OpenMLDB SQL的时候，经常会碰到语法不熟悉、执行模式混淆等问题。如果直接在OpenMLDB集群上进行开发、调试，由于部署、构建索引、大数据量等问题，经常会浪费大量时间在无关任务上，并且可能无法找到SQL本身的错误原因。OpenMLDB SQL Emulator是一个轻量级OpenMLDB SQL模拟开发调试工具，可以在脱离OpenMLDB集群部署的情况下，进行SQL的验证和调试操作。我们强烈推荐此工具给我们的应用开发人员，可以首先基于此工具快速验证SQL的正确性、可上线性以后，再切换到OpenMLDB真实环境上进行部署上线。

## 安装和启动

从[项目页面](https://github.com/vagetablechicken/OpenMLDBSQLEmulator/releases)下载运行包 `emulator-1.0.jar`，使用如下方式启动（注意当前发布的 1.0 版本对应于 OpenMLDB 0.8.3 的 SQL 语法）：

```bash
java -jar emulator-1.0.jar
```

注意，如果想使用`run`命令执行 SQL来验证计算结果，还需要同时下载该页面下的`toydb_run_engine`，并且存放在系统`/tmp`目录下。

## 使用流程

启动emulator后，将直接进入到默认的数据库 emudb，不需要额外创建数据库。
- 数据库不需要被显式创建，只需要`use <db name>`或建表时指定数据库名，即可自动创建数据库。
- 使用命令`addtable`或者`t`来创建虚拟表，重复创建同名表就是更新操作，将使用最新的表schema。我们使用简化的类SQL语法管理表，比如下面的例子创建了一个含有两列的表。
```sql
addtable t1 a int, b int64
```
- 使用命令`showtables`或者`st`来查看当前所有的数据库和表。

### 验证 OpenMLDB SQL

通常情况下，如需要验证OpenMLDB SQL 是否可以上线，可以在真实集群中使用`DEPLOY`进行上线测试。使用这种方法需要管理`DEPLOYMENT`与索引。例如，如果不需要某些测试用的`DEPLOYMENT`，需要手动删除；如果创建了不需要的索引，还需要清理索引。所以，我们建议在Emulator中测试验证。

你可以使用`val`和`valreq`分别进行在线批模式和在线请求模式（即服务部署上线）的OpenMLDB SQL验证。例如，我们测试一个SQL是否能被`DEPLOY`上线，使用`valreq`命令：

```sql
# table creations - t/addtable: create table
addtable t1 a int, b int64

# validate in online request mode
valreq select count(*) over w1 from t1 window w1 as (partition by a order by b rows between unbounded preceding and current row);
```

如果测试不通过，将打印SQL编译错误；通过则打印`validate * success`。整个过程在虚拟环境中，无需担心建表后的资源占用，也没有任何副作用。只要`valreq`验证通过的 SQL，则一定能在真实集群中上线。

### 测试运行 OpenMLDB SQL

OpenMLDB SQL Emulator也可以返回计算结果，用于测试SQL的计算是否符合预期。你可以在其中不断进行计算和上线验证，直到调试得到最终的上线SQL。该功能可以通过Emulator的`run`命令实现。

注意，使用`run`命令需要额外的`toydb_run_engine`支持，可以使用自带`toydb`的`emulator`包，或在[此页面下载](https://github.com/vagetablechicken/OpenMLDBSQLEmulator/releases)`toydb` 程序，并将其直接放入`/tmp`中。

假设`Emulator`已有`toydb`，测试运行步骤如下：

```
# step 1, generate a yaml template
gencase

# step 2 modify the yaml file to add table and data
# ...

# step 3 load yaml and show tables
loadcase
st

# step 4 use val/valreq to validate the sql
valreq select count(*) over w1 from t1 window w1 as (partition by id order by std_ts rows between unbounded preceding and current row);

# step 5 dump the sql you want to run next, this will rewrite the yaml file
dumpcase select count(*) over w1 from t1 window w1 as (partition by id order by std_ts rows between unbounded preceding and current row);

# step 6 run sql using toydb
run
```

#### 步骤解释
**step 1:** 运行命令`gencase`生成一个yaml模版文件，默认创建目录为是`/tmp/emu-case.yaml`。

范例yaml文件：
```yaml
# call toydb_run_engine to run this yaml file
# you can generate yaml cases for reproduction by emulator dump or by yourself

# you can set the global default db
db: emudb
cases:
  - id: 0
    desc: describe this case
    # you can set batch mode
    mode: request
    db: emudb # you can set default db for case, if not set, use the global default db
    inputs:
      - name: t1
        db: emudb # you can set db for each table, if not set, use the default db(table db > case db > global db)
        # must set table schema, emulator can't do this
        columns: ["id int", "pk1 string","col1 int32", "std_ts timestamp"]
        # gen by emulator, just to init table, not the deployment index
        indexs: []
        # must set the data, emulator can't do this
        data: |
          1, A, 1, 1590115420000
          2, B, 1, 1590115420000
    # query: only support single query, to check the result by `expect`
    sql: |

    # optional, you can just check the output, or add your expect
    # expect:
    #   schema: id:int, pk1:string, col1:int, std_ts:timestamp, w1_col1_sum:int, w2_col1_sum:int, w3_col1_sum:int
    #   order: id
    #   data: |
    #     1, A, 1, 1590115420000, 1, 1, 1
    #     2, B, 1, 1590115420000, 1, 1, 1
```

**step 2:** 编辑这个yaml文件，编辑需要注意以下几点：
- 必须修改表名，表schema及其数据，这些不可在Emulator中修改。
- 可以修改运行`mode`，接受`batch`或`request`模式。
- 可以不填写SQL，可以在Emulator中通过`dumpcase <sql>`写入文件。常见使用方法是，先validate SQL，SQL通过校验后dump到case中，再使用`run`命令确认 SQL 的计算符合预期。
- 表的indexs也无需手动填写，`dumpcase`时可以根据表schema自动生成（indexs并非特殊的索引，与SQL也无关，仅仅是创建表时需要创建至少一个索引）。如果不使用`dumpcase`，那么请手动填写至少一个索引，索引没有特别要求。手动创建范例：`["index1:c1:c2", ".."]`，`["index1:c1:c4:(10m,2):absolute"]`。

**step 3:** 执行`loadcase`，这个case的表信息将被加载到Emulator中，通过`st/showtables`确认 case 的表加载成功，显示信息如下：
```bash
emudb> st
emudb={t1=id:int32,pk1:string,col1:int32,std_ts:timestamp}
```

**step 4:** 使用`valreq`来确认我们编写的 SQL 是语法正确且可以上线的。

**step 5 & 6:** 对这个SQL进行计算测试，使用命令`dumpcase`和`run`。 `dumpcase`实际是将SQL与默认索引写入case文件中，`run`命令运行该case文件。 如果你足够熟练，也可以直接修改case文件，再在Emulator中使用`run`运行它，或直接使用`toydb_run_engine --yaml_path=...`来运行。


## 更多信息
### 编译

你可以自行编译Emulator，如果需要使用`run`命令验证SQL计算结果，需要将`toydb_run_engine`放在`src/main/resources`中并执行编译。

```bash
# pack without toydb
mvn package -DskipTests

# pack with toydb
cp toydb_run_engine src/main/resources
mvn package
```

从源码编译`toydb_run_engine`：
```
git clone https://github.com/4paradigm/OpenMLDB.git
cd OpenMLDB
make configure
cd build
make toydb_run_engine -j<thread> # minimum build
```

### OpenMLDB适配版本
Emulator使用`openmldb-jdbc`进行验证，目前支持的OpenMLDB版本为：
|Emulator Version | Compatible OpenMLDB Versions |
|--|--|
| 1.0 | 0.8.3 |

### 常用命令

#### 创建类命令
注意，如果新建表已存在，将会替换原有表。

默认虚拟数据库为`emudb`。

- `use <db>` 使用数据库，如果不存在，将会创建。
- `addtable <table_name> c1 t1,c2 t2, ...` 创建/替换先数据库表
    - 简写: `t <table_name> c1 t1,c2 t2, ...`

- `adddbtable <db_name> <table_name> c1 t1,c2 t2, ...` 创建/替换指定数据库中的表
    - 简写: `dt <table_name> c1 t1,c2 t2, ...`
- `sql <create table sql>` 使用sql创建表
- `showtables` / `st` 显示所有表

#### `genddl <sql>`

可以帮助用户根据SQL直接生成最佳索引的建表语句，避免冗余索引（目前仅支持单数据库）。

- 范例1
```
t t1 a int, b bigint
t t2 a int, b bigint
genddl select *, count(b) over w1 from t1 window w1 as (partition by a order by b rows between 1 preceding and current row)
```
输出：
```
CREATE TABLE IF NOT EXISTS t1(
  a int,
  b bigint,
  index(key=(a), ttl=1, ttl_type=latest, ts=`b`)
);
CREATE TABLE IF NOT EXISTS t2(
  a int,
  b bigint
);
```
因为SQL不涉及t2的操作，所以t2创建为简单创建表格。t1创建为有索引的表格创建。

- 范例2
```
t t1 a int, b bigint
t t2 a int, b bigint
genddl select *, count(b) over w1 from t1 window w1 as (union t2 partition by a order by b rows_range between 1d preceding and current row)
```
输出：
```
CREATE TABLE IF NOT EXISTS t1(
  a int,
  b bigint,
  index(key=(a), ttl=1440m, ttl_type=absolute, ts=`b`)
);
CREATE TABLE IF NOT EXISTS t2(
  a int,
  b bigint,
  index(key=(a), ttl=1440m, ttl_type=absolute, ts=`b`)
);
```
因为SQL涉及union window，t1和t2均为有索引的表格创建。

#### SQL验证命令

- `val <sql>` 在线批模式验证
- `valreq <sql>` 在线请求模式验证
```
t t1 a int, b int64
val select * from t1 where a == 123;
valreq select count(*) over w1 from t1 window w1 as (partition by a order by b rows between unbounded preceding and current row);
```
#### toydb运行命令

`run <yaml_file>` 

在toydb中运行yaml文件。 可使用`gencase`生成。目前支持单个case。case中应包含创建表命令和单个SQL。默认模式为`request`，可以更改为`batch`模式。

由于Emulator中不支持表的添加和删除，请在yaml文件中添加相关操作。

该yaml文件也可用于错误的复现。如需帮助，可向我们提供对应的yaml文件。

#### 其他命令
- `#` 注释.
- 单个命令**不可**写为多行，例如`val select * from t1;`不能写为：
```
# 错误写法
val select *
from
t1;
```
- `?help` 提示
- `?list` 查看所有命令
- `!run-script $filename` 从文件中读取并运行命令。文件可为任意包含命令的文本文件，例如：
```
!run-script src/test/resources/simple.emu
```
- `!set-display-time true/false` 开启/关闭命令运行时间。单位为毫秒（ms），为方法运行的物理时间。
- `!enable-logging filename` and `!disable-logging` shell输入输出log控制。

### CLI框架

我们使用`cliche`作为CLI框架，详见[操作手册](https://code.google.com/archive/p/cliche/wikis/Manual.wiki) 和[source](https://github.com/budhash/cliche)。
