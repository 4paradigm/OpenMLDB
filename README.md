# 介绍

HybridSE(Hybrid SQL Engine)是基于C++和LLVM实现的高性能混合SQL执行引擎，为AI应用、OLAD数据库、HTAP系统、SparkSQL、Flink Streaming SQL等提供一致性的SQL加速优化。

<img src="./docs/img/HybridSE.png" alt="image-20210301164207172" style="width:600px" align="left"/>

# 背景

HybridSE是一个模块化的SQL编译器和执行器，提供了SQL语法校验、逻辑计划生成和优化、表达式优化、离线或在线物理计划生成、Native代码生成以及单机或分布式Runner实现等功能。开发者使用HybridSE可以快速实现一个支持SQL的高性能数据库，也可以用HybridSE来优化离线SQL执行引擎的性能。相比与MySQL、SparkSQL等内置实现的SQL执行引擎，HybridSE不仅性能更优，而且针对AI场景进行了语法拓展和优化，更加适应现代SQL引擎的需求，HybridSE的特性如下。

- **高性能**
  基于LLVM JIT即时编译技术，针对不同硬件环境动态生成二进制码，内置数十种SQL编译优化过程，还有更灵活的内存管理，保证性能在所有SQL引擎中处于前列。
- **拓展性强**
  HybridSE模块化的设计，对外可生成不同阶段的逻辑计划和物理计划，加上丰富完善的多编程语言SDK，无论是实时的OLAD数据库，还是离线的分布式OLAP系统、流式SQL系统都可以使用HybridSE进行SQL优化和加速。
- **针对机器学习优化**
  提供机器学习场景常用的特殊拼表操作以及定制化UDF/UDAF支持，基本满足生产环境下机器学习特征抽取和上线的应用需求。
- **离线在线一致性**
  同一套SQL语法解析和CodeGen代码生成逻辑，保证使用HybridSE的离线和在线系统落地时计算语意一致，SQL中内置UDF/UDAF语法也避免跨语言系统的函数一致性问题。

# 快速开始

## 编译

使用Docker镜像。

```shell
docker run -it develop-registry.4pd.io/centos6_gcc7_fesql:master bash
```

从源码编译项目

```shell
git clone git@github.com:4paradigm/hybridse.git

mkdir -p ./hybridse/build
cd ./hybridse/build/

cmake ..
make -j4
```

### 使用C++编程接口

HybridSE提供C++编程接口，用户可以在C/C++项目中使用来编译SQL以及生成最终的可执行代码

```C++

TEST_F(EngineTest, SimpleEngineTest) {
    // Build Simple Catalog
    auto catalog = BuildSimpleCatalog();

    // database simple_db
    fesql::type::Database db;
    db.set_name("simple_db");

    // prepare table t1 schema and data
    fesql::type::TableDef table_def;
    std::vector<Row> t1_rows;
  
    fesql::sqlcase::CaseDataMock::BuildOnePkTableData(table_def, t1_rows, 10);
    table_def.set_name("t1");
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index12");
    index->add_first_keys("col1");
    index->add_first_keys("col2");
    index->set_second_key("col5");
  
    // add t1 into db
    *(db.add_tables()) = table_def;
  
    // add simple_db into catalog
    catalog->AddDatabase(db);

    // insert data into simple_db
    catalog->InsertRows("simple_db", "t1", t1_rows);

    // Build Simple Engine
    EngineOptions options;
    Engine engine(catalog, options);
    std::string sql =
        "select col0, col1, col2, col3, col4, col5, col6, col1+col2 as col12, "
        "col3+col4 as col34 from t1;";
    {
        base::Status get_status;
        BatchRunSession session;
        // compile sql
        ASSERT_TRUE(engine.Get(sql, "simple_db", session, get_status));
        ASSERT_EQ(get_status.code, common::kOk);
        std::vector<Row> outputs;
        ASSERT_EQ(10, session.Run(outputs));
        PrintRows(session.GetSchema(), outputs);
    }
}

```



### 使用Java编程接口

HybridSE也提供Java编程接口，基于Java/Scala的项目也可以使用来实现SQL语法支持，详情参考[HybridSE Java SDK](https://github.com/4paradigm/hybridse)。

```scala
private def withSQLEngine[T](sql: String, db: Database, config: FeSQLConfig)(body: SQLEngine => T): T = {
  var engine: SQLEngine = null
 
  val engineOptions = SQLEngine.createDefaultEngineOptions()
 
  if (config.enableWindowParallelization) {
    logger.info("Enable window parallelization optimization")
    engineOptions.set_enable_batch_window_parallelization(true)
  } else {
    logger.info("Disable window parallelization optimization, enable by setting fesql.window.parallelization")
  }
 
  if (config.enableUnsafeRowOptimization) {
    engineOptions.set_enable_spark_unsaferow_format(true)
  }
 
  try {
    engine = new SQLEngine(sql, db, engineOptions)
    val res = body(engine)
    res
  } finally {
    if (engine != null) {
      engine.close()
    }
  }
}
```



## 示例

### 一个简单NewSQL数据库

开发者使用HybridSE可以快速实现一个支持SQL的高性能数据库。examples/toydb就是一个简易的单机版面向实时决策的NewSQL数据库。

#### 编译ToyDB

```shell
cd hybridse 
mkdir build/ 
cmake .. -DEXAMPLES_ENABLE=ON 
make -j4
```

#### 启动ToyDB

```
cd hybridse/examples/toydb/onebox
sh start_all.sh
sh start_cli.sh
```



toydb支持基本的NewSQL数据库的操作，具体操作细节，参详[ToyDB快速使用手册](./docs/zh-hans/developer_guide/toydb_tutorial/toydb_usage.md)

### 编程接口

### C++编程接口使用

HybridSE提供C++编程接口，用户可以在C/C++项目中使用来编译SQL以及生成最终的可执行代码
## Java编程接口使用
HybridSE也提供Java编程接口，基于Java/Scala的项目也可以使用来实现SQL语法支持，详情参考HybridSE Java SDK。

[Hybrid SQL语法文档](./docs/zh-hans/language_guide/reference.md)

## 生态项目

| 项目                                                         | 状态   | 描述                                             |
| :----------------------------------------------------------- | :----- | :----------------------------------------------- |
| [FEDB](https://wiki.4paradigm.com/display/PlatformRD/FEDB-README.md) | 开源   | 一个面向实时决策的NewSQL数据库                   |
| [NativeSpark](https://wiki.4paradigm.com/display/PlatformRD/NativeSpark+README.md) | 开源   | 基于LLVM优化的Spark兼容的高性能原生执行引擎      |
| ToyDB                                                        | 开发中 | 基于HybridSE开发的简易SQL数据库                  |
| NativeFlink                                                  | 开发中 | 基于HybridSE开发的高性能批流一体FlinkSQL执行引擎 |

## 项目贡献



## 未来规划

### SQL兼容

HybridSE已经兼容主流的DDL、DML语法，并将逐步增强对ANSI SQL语法的兼容性，从而简化用户从其他SQL引擎迁移的成本。

- [2021H1&H2]完善Window的标准语法，支持Where, Group By, Join等操作
- [2021H1&H2]针对AI场景扩展特有的语法特性和UDAF函数

### 性能优化

HybridSE内置数十种SQL表达式和逻辑计划优化，提供标准的优化Pass实现接口，未来将会接入更多的SQL优化从而提升系统性能。

- [2021H1]面向批式数据处理和请求式数据处理场景支持逻辑和物理计划优化
- [2021H1]支持高性能分布式执行计划生成和代码生成
- [2021H2]基于LLVM的表达式编译和生成代码优化
- [2021H2]更多经典SQL表达式优化过程支持

### 生态集成

HybridSE可拓展适配NoSQL、OLAP、OLTP等系统，已支持SparkSQL和FEDB底层替换，未来将兼容接入更多开源生态系统。

* [2021H2]适配流式等主流开源SQL计算框架等，如优化FlinkSQL性能等
* [2021H2]适配多种行编码格式和列编码格式，兼容Apache Arrow格式和生态
* [2021H2]支持主流编程语言接口，包括C++, Java, Python, Go, Rust SDK等

## 许可证

Apache License 2.0