[![HybridSE CI](https://github.com/4paradigm/HybridSE/actions/workflows/hybridse-ci.yml/badge.svg)](https://github.com/4paradigm/HybridSE/actions/workflows/hybridse-ci.yml)

- [**Slack Channel**](https://hybridsql-ws.slack.com/archives/C01R7LAF6AY)
- [**Discussions**](https://github.com/4paradigm/HybridSE/discussions)

# What is HybridSE

HybridSE(Hybrid SQL Engine)是基于C++和LLVM实现的高性能混合SQL执行引擎，为AI应用、OLAD数据库、HTAP系统、SparkSQL、Flink Streaming SQL等提供一致性的SQL加速优化。

<img src="./images/HybridSE.png" alt="image-20210301164207172" style="width:600px" align="left"/>

HybridSE是一个模块化的SQL编译器和执行器，提供了SQL语法校验、逻辑计划生成和优化、表达式优化、离线或在线物理计划生成、Native代码生成以及单机或分布式Runner实现等功能。开发者使用HybridSE可以快速实现一个支持SQL的高性能数据库，也可以用HybridSE来优化离线SQL执行引擎的性能。相比与MySQL、SparkSQL等内置实现的SQL执行引擎，HybridSE不仅性能更优，而且针对AI场景进行了语法拓展和优化，更加适应现代SQL引擎的需求，HybridSE的特性如下。

- __高性能__

  基于LLVM JIT即时编译技术，针对不同硬件环境动态生成二进制码，内置数十种SQL编译优化过程，还有更灵活的内存管理，保证性能在所有SQL引擎中处于前列。

- __拓展性强__

  HybridSE模块化的设计，对外可生成不同阶段的逻辑计划和物理计划，加上丰富完善的多编程语言SDK，无论是实时的OLAD数据库，还是离线的分布式OLAP系统、流式SQL系统都可以使用HybridSE进行SQL优化和加速。

- __针对机器学习优化__

  提供机器学习场景常用的特殊拼表操作以及定制化UDF/UDAF支持，基本满足生产环境下机器学习特征抽取和上线的应用需求。

- __离线在线一致性__

  同一套SQL语法解析和CodeGen代码生成逻辑，保证使用HybridSE的离线和在线系统落地时计算语意一致，SQL中内置UDF/UDAF语法也避免跨语言系统的函数一致性问题。

注:目前还处于unstable状态并且有许多功能待补齐,不能运用于生产环境

# Getting Started

## Build


```shell
git clone git@github.com:4paradigm/HybridSE.git
cd HybridSE
docker run -v `pwd`:/HybridSE -it ghcr.io/4paradigm/centos6_gcc7_hybridsql:latest
cd /depends && tar -zxf thirdparty.tar.gz
cd /Hybridse
source tools/init_env.profile.sh
ln -sf /depends/thirdparty thirdparty
mkdir -p build && cd build
cmake ..
# just compile the core library
make -j4 hybridse_core
```

### 使用C++编程接口

HybridSE提供C++编程接口，用户可以在C/C++项目中使用来编译SQL以及生成最终的可执行代码。[C++API文档](./docs/zh-hans/developer_guide/api/c++/reference.md)

```c++
using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)
namespace fesql {
namespace cmd {
// ...
int run() {
    // build Simple Catalog
    auto catalog = std::make_shared<SimpleCatalog>(true);
    // database simple_db
    fesql::type::Database db;
    db.set_name("simple_db");

    // prepare table t1 schema and data
    fesql::type::TableDef table_def;
    {
        table_def.set_name("t1");
        table_def.set_catalog("db");
        {
            ::fesql::type::ColumnDef* column = table_def.add_columns();
            column->set_type(::fesql::type::kVarchar);
            column->set_name("col0");
        }
        {
            ::fesql::type::ColumnDef* column = table_def.add_columns();
            column->set_type(::fesql::type::kInt32);
            column->set_name("col1");
        }
        {
            ::fesql::type::ColumnDef* column = table_def.add_columns();
            column->set_type(::fesql::type::kInt64);
            column->set_name("col2");
        }
    }
    *(db.add_tables()) = table_def;
    catalog->AddDatabase(db);

    // insert data into simple_db
    std::vector<Row> t1_rows;
    for (int i = 0; i < 10; ++i) {
        std::string str1 = "hello";
        codec::RowBuilder builder(table_def.columns());
        uint32_t total_size = builder.CalTotalLength(str1.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString(str1.c_str(), str1.size());
        builder.AppendInt32(i);
        builder.AppendInt64(1576571615000 - i);
        t1_rows.push_back(Row(base::RefCountedSlice::Create(ptr, total_size)));
    }
    if (!catalog->InsertRows("simple_db", "t1", t1_rows)) {
        return SIMPLE_ENGINE_DATA_ERROR;
    }

    // build simple engine
    EngineOptions options;
    Engine engine(catalog, options);
    std::string sql = "select col0, col1, col2, col1+col2 as col12 from t1;";
    {
        base::Status get_status;
        BatchRunSession session;
        // compile sql
        if (!engine.Get(sql, "simple_db", session, get_status) ||
            get_status.code != common::kOk) {
            return SIMPLE_ENGINE_COMPILE_ERROR;
        }
        std::vector<Row> outputs;
        // run sql query
        if (0 != session.Run(outputs)) {
            return SIMPLE_ENGINE_RUN_ERROR;
        }
        PrintRows(session.GetSchema(), outputs);
    }
    return SIMPLE_ENGINE_RET_SUCCESS;
}

}  // namespace cmd
}  // namespace fesql

int main(int argc, char** argv) {
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return fesql::cmd::run();
}

```

### Simple Engine Demo

```shell
cd /HybridSE
mkdir build
cd build
cmake ..
make -j4 hybridse_proto && make -j4 hybridse_parser && make -j4 simple_engine_demo
./src/simple_engine_demo
```

### ToyDB Demo

#### 编译ToyDB

```shell
cd /HybridSE
mkdir build 
cmake .. -DEXAMPLES_ENABLE=ON 
make -j4 hybridse_proto && make -j4 hybridse_parser && make toydb -j4
```

#### 启动ToyDB

```
cd /HybridSE/examples/toydb/onebox
sh start_all.sh
sh start_cli.sh
```

Toydb支持基本的NewSQL数据库的操作，具体操作细节可参考[ToyDB使用手册](../usage/toydb_tutorial/toydb_usage.md)

## 生态项目

| 项目                                                    | 状态   | 描述                                             |
| :------------------------------------------------------ | :----- | :----------------------------------------------- |
| [FEDB](https://github.com/4paradigm/fedb)               | 开源   | 面向实时推理和决策的NewSQL数据库                 |
| [NativeSpark](https://github.com/4paradigm/NativeSpark) | 开源   | 基于LLVM优化的Spark兼容的高性能原生执行引擎      |
| NativeFlink                                             | 开发中 | 基于HybridSE开发的高性能批流一体FlinkSQL执行引擎 |

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

## 反馈与参与

- Bug、疑惑、修改欢迎提在[Github Issue](https://github.com/4paradigm/HybridSE/issues)
- 想了解更多或者有想法可以参与到[Github Discussions](https://github.com/4paradigm/HybridSE/discussions)和[slack](https://hybridsql-ws.slack.com/archives/C01R7LAF6AY)交流

## 许可证

[Apache License 2.0](https://github.com/4paradigm/HybridSE/blob/main/LICENSE)