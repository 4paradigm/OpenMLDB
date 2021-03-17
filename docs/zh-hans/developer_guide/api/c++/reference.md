# HybridSE C++ API Reference

## Catalog

| Members                                       | Description |
| --------------------------------------------- | ----------- |
| [hybridse::vm::Catalog](./catalog/catalog.md) |             |
| [DataHandler](./catalog/data_handler.md)      |             |
| TableHandler                                  |             |
| PartitionHandler                              |             |
| RowHandler                                    |             |
| AysncRowHandler                               |             |
| ErrorTableHandler                             |             |
| ErrorRowHandler                               |             |
| Tablet                                        |             |
|                                               |             |
|                                               |             |

## Codec

| Members       |      |
| ------------- | ---- |
| Row           |      |
| RowView       |      |
| RowIOBufView  |      |
| RowFormat     |      |
| ColInfo       |      |
| StringColInfo |      |

## EngineContext

| Members          |                |
| ---------------- | -------------- |
| CompileInfo      |                |
| CompileInfoCache |                |
| Engine           | New EngineImpl |
| EngineOptions    |                |
| ExplainOutput    |                |
| RunSession       |                |

## EngineImpl

| Members                 |            |
| ----------------------- | ---------- |
| CompileInfo             |            |
| CompileInfoCache        |            |
| Engine                  | New EngineImpl |
| EngineOptions           |            |
| ExplainOutput           |            |
| RunSession              |            |
| BatchRunSession         |            |
| BatchRequestRunSession  |            |
| RequestRunSession       |            |
|                         |            |
| LocalTablet             |            |
| LocalTabletTableHandler |            |



## SDK

| Members       |      |
| ------------- | ---- |
|               |      |
| DatabaseDef   |      |
| ExecuteRequst |      |
| ExecuteResult |      |
| DataType      |      |
| DBMSSdk       |      |
| GroupDef      |      |
| RequestRow    |      |
| ResultSet     |      |
| Schema        |      |
| SchemaImpl    |      |
| Status        |      |
| Table         |      |
| TableImpl     |      |
| TableSet      |      |
| TableSetImpl  |      |
| Value         |      |
|               |      |