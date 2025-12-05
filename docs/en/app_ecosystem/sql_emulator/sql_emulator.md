# Quickstart

OpenMLDB SQL Emulator is a lightweight SQL simulator for [OpenMLDB](https://github.com/4paradigm/OpenMLDB), designed to facilitate more efficient and convenient development and debugging of OpenMLDB SQL without the cumbersome deployment of a running OpenMLDB cluster.

To efficiently perform time-series feature calculations, OpenMLDB SQL has been improved and extended from standard SQL. Therefore, beginners using OpenMLDB SQL often encounter issues related to unfamiliar syntax and confusion regarding execution modes. Developing and debugging directly on an OpenMLDB cluster can lead to significant time wasted on irrelevant tasks such as deployment, index building, handling large volumes of data, and may also make it challenging to pinpoint the root cause of SQL errors.

The OpenMLDB SQL Emulator serves as a lightweight tool for simulating the development and debugging of OpenMLDB SQL queries without the need for deployment within an OpenMLDB cluster. We highly recommend this tool to our application developers, as it allows them to initially validate SQL correctness and suitability for deployment before transitioning to the actual OpenMLDB environment for deployment and production.


## Installation and Startup

From [release page](https://github.com/vagetablechicken/OpenMLDBSQLEmulator/releases), download the runtime package `emulator-1.0.jar` and launch it using the following command (please note that the current release version 1.0 corresponds to the SQL syntax of OpenMLDB 0.8.3):

```bash
java -jar emulator-1.0.jar
```
Please note that in order to execute SQL queries using the `run` command to validate computation results, you'll also need to download the `toydb_run_engine` from the same page, and put this file in the `/tmp` directory of your system.

## Usage
Upon starting the emulator, it will directly enter the default database, emudb. No additional database creation is required.

- Databases do not need explicit creation. You can either use the command `use <db name>` or specify the database name when creating a table to automatically create the database.
- Use the commands `addtable` or `t` to create virtual tables. Repeatedly creating a table with the same name will perform an update operation, using the most recent table schema. Simplified SQL-like syntax is used to manage tables. For instance, the following example creates a table with two columns:

```sql
addtable t1 a int, b int64
```
- Use the command `showtables` or `st` to view all current databases and tables.


### OpenMLDB SQL Validation

Typically, to validate whether OpenMLDB SQL can be deployed, you can do so in a real cluster using `DEPLOY`. However, using this method requires managing `DEPLOYMENT` and indexes. For instance, you may need to manually delete unnecessary `DEPLOYMENT` or clean up indexes if they are created unnecessarily. Therefore, we recommend testing and validating in the Emulator environment.

You can use `val` and `valreq` to respectively perform validation of OpenMLDB SQL in online batch mode and online request mode (i.e., deploying as a service). For instance, to test if an SQL query can be deployed, you can use the `valreq` command:

```sql
# table creations - t/addtable: create table
addtable t1 a int, b int64

# validate in online request mode
valreq select count(*) over w1 from t1 window w1 as (partition by a order by b rows between unbounded preceding and current row);
```
If the test fails, it will print the SQL compilation error. If it passes, it will print `validate * success`. The entire process takes place in a virtual environment, without concerns about resource usage after table creation or any side effects. Any SQL that passes validation through `valreq` will definitely be deployable in a real cluster.

### OpenMLDB SQL Computation Test

The OpenMLDB SQL Emulator is also capable of returning computation results, facilitating the testing of whether the SQL computations align with expectations. You can recursively perform calculations and validations until the final satisfactory SQL is obtained. This functionality can be achieved using the `run` command in the Emulator.

Please note that the `run` command requires support from `toydb_run_engine`. You can either use the pre-existing emulator package containing `toydb` or download the `toydb` program from [this page](https://github.com/vagetablechicken/OpenMLDBSQLEmulator/releases) and place it into the `/tmp` directory.

Assuming the Emulator already has `toydb`, the steps for computation test are as follows:

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
#### Explanations

**step 1:** Run command `gencase` to generate a template yaml file. The default directory is `/tmp/emu-case.yaml`.

Example yaml file:
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

**step 2:** Edit this yaml file. Note the following: 
- You must modify the table name, table schema, and its data; these cannot be modified within the Emulator.
- You can modify the `mode` of operation, which accepts either `batch` or `request` mode.
- It's not necessary to fill in the SQL. You can write it into a file in the Emulator using `dumpcase <sql>`. The common practice is to first validate the SQL, then dump it into the case once the SQL passes validation. Afterwards, use the `run` command to confirm that the SQL computation aligns with expectations.
- The table's indexes don't need to be manually filled in. They can be automatically generated during `dumpcase` based on the table schema (indexes are not specific to SQL and are unrelated to SQL queries; they are only required when creating a table). If not using `dumpcase`, then manually enter at least one index. Indexes have no specific requirements. Examples of manual creation: `["index1:c1:c2", ".."]`, `["index1:c1:c4:(10m,2):absorlat"]`.

**step 3:** Execute `loadcase`, and the table information from this case will be loaded into the Emulator. Confirm the successful loading of the case's table by using `st/showtables`. The displayed information should be similar to:
```bash
emudb> st
emudb={t1=id:int32,pk1:string,col1:int32,std_ts:timestamp}
```

**step 4:** Use `valreq` to confirm that the SQL we've written is syntactically correct and suitable for deployment.

**step 5 & 6:** Perform computation testing on this SQL using the `dumpcase` and `run` commands. `dumpcase` effectively writes the SQL and default indexes into a case file, while the `run` command executes this case file. If you are proficient enough, you can also directly modify the case file and run it in the Emulator using `run`, or directly use `toydb_run_engine --yaml_path=...` to run it.

## Additional Information
### Build
You can build the Emulator on your own. If you need to verify SQL computation results using the `run` command, you must place `toydb_run_engine` in `src/main/resources` and then execute the build process.

```bash
# pack without toydb
mvn package -DskipTests

# pack with toydb
cp toydb_run_engine src/main/resources
mvn package
```

To build `toydb_run_engine` from the source code:
```
git clone https://github.com/4paradigm/OpenMLDB.git
cd OpenMLDB
make configure
cd build
make toydb_run_engine -j<thread> # minimum build
```

### Compatible OpenMLDB Versions

The Emulator employs `openmldb-jdbc` for validations. The current compatible OpenMLDB version is:
|Emulator Version | Compatible OpenMLDB Versions |
|--|--|
| 1.0 | 0.8.3 |

### Command List


#### Commands for Creation
Note that if a table already exists, the creation of a new table will replace the existing table. The default database is `emudb`.


- `use <db>` Use a database. If it doesn't exist, it will be created.
- `addtable <table_name> c1 t1,c2 t2, ...`  Create/replace a table in the current database.
    - abbreviate: `t <table_name> c1 t1,c2 t2, ...`

- `adddbtable <db_name> <table_name> c1 t1,c2 t2, ...`  Create/replace a table in the specified database. If the database doesn't exist, it will be created.
    - abbreviate: `dt <table_name> c1 t1,c2 t2, ...`
- `sql <create table sql>` Create a table by SQL.

- `showtables` / `st` list all tables.


#### `genddl <sql>`

If you want to create tables without redundant indexes, you can use `genddl` to generate ddl from the query SQL.

Note that method `genDDL` in `openmldb-jdbc` does not support multiple databases yet, so we can't use this method to parse SQLs that have multiple dbs.

- Example1
```
t t1 a int, b bigint
t t2 a int, b bigint
genddl select *, count(b) over w1 from t1 window w1 as (partition by a order by b rows between 1 preceding and current row)
```
output:
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
Since the SQL doesn't involve operations on t2, the SQL that creates t2 is just a simple create table, while the SQL that creates t1 is a create table with an index.

- Example2
```
t t1 a int, b bigint
t t2 a int, b bigint
genddl select *, count(b) over w1 from t1 window w1 as (union t2 partition by a order by b rows_range between 1d preceding and current row)
```
output:
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
Since there's a union window, the SQLs that create t1 and t2 both have an index.

#### SQL Validation Commands

- `val <sql>` validates SQL in batch mode; tables should be created beforehand
- `valreq <sql>` validates SQL in request mode; tables should be created beforehand
```
t t1 a int, b int64
val select * from t1 where a == 123;
valreq select count(*) over w1 from t1 window w1 as (partition by a order by b rows between unbounded preceding and current row);
```

#### toydb Execution Commands

`run <yaml_file>` 

Run the yaml file in toydb. You can use `gencase` to generate it. Currently, a single case is supported. The case should include table creation commands and a single SQL query. The default mode is `request` but can be changed to `batch` mode.

Since the Emulator does not support add/del table or table data, please include the relevant operations in the yaml file.

This yaml file can also be used to reproduce errors. If you need assistance, please provide us with the corresponding yaml file.

#### Miscellaneous
- `#` comment.
- You **cannot** run a command in multi-lines, e.g. `val select * from t1;` cannot be written as
```
# wrong
val select *
from
t1;
```
- `?help` hint
- `?list` list all cmds
- `!run-script $filename` reads and executes commands from a given script file. The script file is just a text file with commands in it. e.g.
```
!run-script src/test/resources/simple.emu
```
- `!set-display-time true/false` toggles display of command execution time. Time is shown in milliseconds and is physical time of the method.
- `!enable-logging filename` and `!disable-logging` control logging settings, i.e. duplication of all Shell's input and output in a file.

### CLI Framework

We use `cliche` for the CLI interface. See [Manual](https://code.google.com/archive/p/cliche/wikis/Manual.wiki) and [source](https://github.com/budhash/cliche).
