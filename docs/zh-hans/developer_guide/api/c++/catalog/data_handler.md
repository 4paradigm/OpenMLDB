# hybridse::vm::DataHandler

`include <vm/catalog.h>`



## Summary

`DataHandler`定义了HybridSE的标准数据处理，定义了一系列数据集的访问接口

| Constructors and Destructors |
| :--------------------------- |
| [Catalog](#Catalog)          |

| Public functions                      | Return type                                |
| :------------------------------------ | ------------------------------------------ |
| [IndexSupport](#IndexSupport)         | `Bool`                                     |
| [GetDatabase](#GetDatabase)           | `std::shared_ptr<type::Database`>          |
| [GetTable](#GetTable)                 | `std::shared_ptr<TableHandler>`            |
| [GetProcedureInfo](#GetProcedureInfo) | std::shared_ptr<fesql::sdk::ProcedureInfo> |

```

```

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



#### GetHandlerTypeName

```c++
// get handler type name
virtual const std::string GetHandlerTypeName() = 0;
```

返回数据集的类型名字

#### GetTable

```c++
// get table handler
virtual std::shared_ptr<TableHandler> GetTable(
    const std::string& db, const std::string& table_name) = 0;
```

返回Catalog中`db.table_name`所对应的表

#### GetProcedureInfo

```c++
virtual std::shared_ptr<fesql::sdk::ProcedureInfo> GetProcedureInfo(
    const std::string& db, const std::string& sp_name) = 0;
```

根据给定数据库名`db`以及存储过程名`sp_name`，搜索并返回相应的存储过程信息。