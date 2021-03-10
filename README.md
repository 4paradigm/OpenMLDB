## What is Hybrid SQL Engine

Hybrid SQL Engine是基于C++和LLVM实现的高性能SQL编译器。能基于场景优化支持多种应用场景（ai应用 olda数据库，分布式hatp数据库, spark, streaming sql(flink) 等）

<img src="./docs/img/HybridSE.png" alt="image-20210301164207172" style="width:600px" align="left"/>

[Hybrid SQL Language Guide](./docs/en/language_guide/reference.md)

[Hybrid SQL语法文档](./docs/zh-hans/language_guide/reference.md)

Developer Guide

## How to build

```shell
% cd ${PROJECT_ROOT_DIR}
% mkdir -p build
% cd build
% cmake ..
% make -j8
```

## Example （待补充）

```
% cd ${PROJECT_ROOT_DIR}
% cd example
% 

```

## HyBridSE Roadmap

### ANSI SQL兼容

HybridSE已经兼容主流的DDL、DML语法，并将逐步增强对ANSI SQL语法的兼容性，从而简化用户从其他SQL引擎迁移的成本。

* [2021H1&H2] 完善Window的标准语法，支持Where, Group By, Join等操作
* [2021H1&H2] 针对AI场景扩展特有的语法特性和UDAF函数

### 性能

HybridSE内置数十种SQL表达式和逻辑计划优化，提供标准的优化Pass实现接口，未来将会接入更多的SQL优化从而提升系统性能。

* [2021H1 ]面向批式数据处理和请求式数据处理场景支持逻辑和物理计划优化
* [2021H1] 支持高性能分布式执行计划生成和代码生成
* [2021H2] 基于LLVM(MLIR)的表达式编译和生成代码优化
* [2021H2] 更多经典SQL表达式优化过程支持

### 生态

HybridSE可拓展适配NoSQL、OLAP、OLTP等系统，已支持SparkSQL和FEDB底层替换，未来将兼容接入更多开源生态系统。

* [2021H2] 存储可感知的编译优化(storage-aware optimizing), 基于底层存储特性进行SQL的编译优化
* [2021H2] 适配多种行编码格式和列编码格式，兼容Apache Arrow格式和生态
* [2021H2] 支持主流编程语言接口，包括C++, Java, Python, Go, Rust SDK等



## Versions



## License

Apache License 2.0