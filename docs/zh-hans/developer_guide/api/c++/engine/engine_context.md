# EngineMode

## Summary

`EngineMode`定义了若干Engine模式

```c++
enum EngineMode { kBatchMode, kRequestMode, kBatchRequestMode };
```

| Enum Type           | Description                                                  |
| :------------------ | ------------------------------------------------------------ |
| `kBatchMode`        | 批式数据处理模式，是传统的SQL执行模式。                      |
| `kRequestMode`      | 请求式数据处理模式，是`OLDA`下的处理处理模式。一次SQL查询需要传入一条请求数据，在窗口聚合计算时，将以该请求数据作为`CURRENT_ROW`来划定窗口范围计算。 |
| `kBatchRequestMode` | 批量请求式数据处理模式，`请求式数据处理模式`的变体，唯一的区别是，一次SQL查询可以传入一条或者多条请求数据。 |

# BatchRequestInfo



# CompileType

## Summary

`CompileType`定义来HybridSE的编译类型，目前仅支持`SQL`编译

```c++
enum ComileType {
    kCompileSQL,
};
```



# CompileInfo

## Summary

编译信息接口，提供基础的编译信息查询能力

| Public functions                            | Return type               |
| :------------------------------------------ | ------------------------- |
| [GetCompileType](#GetCompileType)           | `ComileType`              |
| [GetEngineMode](#GetEngineMode)             | `EngineMode`              |
| [GetSQL](#GetSQL)                           | `const std::string&`      |
| [GetSchema](#GetSchema)                     | `const Schema`            |
| [GetEncodedSchema](#GetEncodedSchema)       | `const std::string&`      |
| [GetRequestName](#GetRequestName)           | `const std::string&`      |
| [GetBatchRequestInfo](#GetBatchRequestInfo) | `const BatchRequestInfo&` |
| [GetIRBuffer](#GetIRBuffer)                 | `bool`                    |
| [GetIRSize](#GetIRSize)                     | `size_t`                  |
| [DumpPhysicalPlan](#DumpPhysicalPlan)       | `void`                    |
| [DumpClusterJob](DumpClusterJob)            | `void`                    |

```c++
virtual bool get_ir_buffer(const base::RawBuffer& buf) = 0;
    virtual size_t get_ir_size() = 0;
    virtual const EngineMode GetEngineMode() const = 0;
    virtual const std::string& GetSQL() const = 0;
    virtual const Schema& GetSchema() const = 0;
    virtual const ComileType GetCompileType() const = 0;
    virtual const std::string& GetEncodedSchema() const = 0;
    virtual const Schema& GetRequestSchema() const = 0;
    virtual const std::string& GetRequestName() const = 0;
    virtual const fesql::vm::BatchRequestInfo& GetBatchRequestInfo() const = 0;
    virtual void DumpPhysicalPlan(std::ostream& output,
                                  const std::string& tab) = 0;
    virtual void DumpClusterJob(std::ostream& output,
                                const std::string& tab) = 0;
```

## Public functions

#### GetCompileType

```c++
const ComileType GetCompileType() const
```

获取编译的类型，目前HybridSE仅支持一种编译类型[ComileType](#CompileType)

#### GetEngineMode

```c++
const EngineMode GetEngineMode() const
```

返回编译的[`EngineMode`](#EngineMode) 类型信息

#### GetSQL

```c++
const std::string& GetSQL() const
```

返回编译的`SQL`语句

#### GetSchema

```c++
const Schema& GetSchema() const
```

返回查询输出[`Schema`](../catalog/table.md#Schema)

#### GetEncodedSchema

```c++
const std::string& GetEncodedSchema() const
```

返回查询输出的Schema序列化结果字符串

#### GetRequestName

```c++
const std::string& GetRequestName() const
```

返回查询请求表名，若不存在请求表，则返回`""`。

>  注意，仅仅在[`kRequestMode`](#EngineMode)或者[`kBatchRequestMode`](#EngineMode)模式下，查询存在请求表，其他模式下，该接口返回空字符串。



#### GetBatchRequestInfo

```c++
const BatchRequestInfo& GetBatchRequestInfo() const
```

返回批请求信息。

> 注意，仅仅在[`kBatchRequestMode`](#EngineMode)模式下，查询编译信息种存在批请求信息[`BatchRequestInfo`](#BatchRequestInfo)。