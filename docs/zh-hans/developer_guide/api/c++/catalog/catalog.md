# hybridse::vm::Catalog

`include <vm/catalog.h>`



## Summary

`Catalog`定义啦HybridSE的标准数据接口，包含一组纯虚函数（或虚函数），用户需要继承并实现自己的`CatalogImpl`

| Constructors and Destructors |
| :--------------------------- |
| [Catalog](#Catalog)          |

| Public functions                      | Return type                                |
| :------------------------------------ | ------------------------------------------ |
| [IndexSupport](#IndexSupport)         | `Bool`                                     |
| [GetDatabase](#GetDatabase)           | `std::shared_ptr<type::Database`>          |
| [GetTable](#GetTable)                 | `std::shared_ptr<TableHandler>`            |
| [GetProcedureInfo](#GetProcedureInfo) | std::shared_ptr<fesql::sdk::ProcedureInfo> |

## Public functions

#### Catalog

```c++
Catalog() {}
```

Catalog的构造函数。

#### IndexSupport

```c++
virtual bool IndexSupport() = 0;
```

返回`true`时，Catalog支持索引优化。否则，Catalog不支持索引优化。

#### GetDatabase

```c++
// get database information
virtual std::shared_ptr<type::Database> GetDatabase(
    const std::string& db) = 0;
```

根据给定的数据库名`db`,搜索并返回相应的数据库实例

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