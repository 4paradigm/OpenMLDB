# hybridse::vm::DataHandler

`include <vm/catalog.h>`

## Summary

`DataHandler`继承自` ListV<Row>`定义了HybridSE的标准行编码数据处理接口，定义了一系列数据集的访问接口

| Constructors and Destructors |
| :--------------------------- |
| [DataHandler](#DataHandler)  |

| Public functions                          | Return type          |
| :---------------------------------------- | -------------------- |
| [GetSchema](#GetSchema)                   | `const Schema*`      |
| [GetName](#GetName)                       | `const std::string&` |
| [GetDatabase](#GetDatabase)               | `const std::string&` |
| [GetHanlderType](#GetHanlderType)         | `const HandlerType`  |
| [GetHandlerTypeName](#GetHandlerTypeName) | `const std::string`  |

```
class DataHandler : public ListV<Row> {
 public:
    DataHandler() {}
    virtual ~DataHandler() {}
    // get the schema of table
    virtual const Schema* GetSchema() = 0;

    // get the table name
    virtual const std::string& GetName() = 0;

    // get the db name
    virtual const std::string& GetDatabase() = 0;
    virtual const HandlerType GetHanlderType() = 0;
    virtual const std::string GetHandlerTypeName() = 0;
    virtual base::Status GetStatus() { return base::Status::OK(); }
};
```

## Public functions

#### GetHanlderType

```c++
DataHandler() {}
```

DataHandler的构造函数。

#### GetHanlderType

```c++
virtual HandlerType GetHanlderType() = 0;
```

返回数据集的类型

```c++
enum HandlerType { kRowHandler, kTableHandler, kPartitionHandler };
```

#### GetHandlerTypeName

```c++
// get handler type name
virtual const std::string GetHandlerTypeName() = 0;
```

返回数据集的类型名字