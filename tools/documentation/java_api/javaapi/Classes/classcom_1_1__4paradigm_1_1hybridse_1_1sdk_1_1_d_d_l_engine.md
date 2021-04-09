---
title: com::_4paradigm::hybridse::sdk::DDLEngine

---
# com::_4paradigm::hybridse::sdk::DDLEngine



## Summary


|  Public functions|            |
| -------------- | -------------- |
|**[resolveColumnIndex](/hybridse/usage/api/c++/Classes/classcom_1_1__4paradigm_1_1hybridse_1_1sdk_1_1_d_d_l_engine.md#function-resolvecolumnindex)**(ExprNode expr, PhysicalOpNode planNode)| int  |
|**[genDDL](/hybridse/usage/api/c++/Classes/classcom_1_1__4paradigm_1_1hybridse_1_1sdk_1_1_d_d_l_engine.md#function-genddl)**(String sql, String schema, int replicanum, int partitionnum)| String  |
|**[genDDL](/hybridse/usage/api/c++/Classes/classcom_1_1__4paradigm_1_1hybridse_1_1sdk_1_1_d_d_l_engine.md#function-genddl)**(String sql, String schema)| String  |
|**[parseWindowOp](/hybridse/usage/api/c++/Classes/classcom_1_1__4paradigm_1_1hybridse_1_1sdk_1_1_d_d_l_engine.md#function-parsewindowop)**(PhysicalOpNode node, Map< String, RtidbTable > rtidbTables)| void <br>只对window op做解析，因为fesql node类型太多了，暂时没办法做通用性解析  |
|**[parseLastJoinOp](/hybridse/usage/api/c++/Classes/classcom_1_1__4paradigm_1_1hybridse_1_1sdk_1_1_d_d_l_engine.md#function-parselastjoinop)**(PhysicalOpNode node, Map< String, RtidbTable > rtidbTables)| void  |
|**[parseRtidbIndex](/hybridse/usage/api/c++/Classes/classcom_1_1__4paradigm_1_1hybridse_1_1sdk_1_1_d_d_l_engine.md#function-parsertidbindex)**(List< PhysicalOpNode > nodes, Map< String, TypeOuterClass.TableDef > tableDefMap)| Map< String, RtidbTable >  |
|**[findDataProviderNode](/hybridse/usage/api/c++/Classes/classcom_1_1__4paradigm_1_1hybridse_1_1sdk_1_1_d_d_l_engine.md#function-finddataprovidernode)**(PhysicalOpNode node)| PhysicalDataProviderNode  |
|**[dagToList](/hybridse/usage/api/c++/Classes/classcom_1_1__4paradigm_1_1hybridse_1_1sdk_1_1_d_d_l_engine.md#function-dagtolist)**(PhysicalOpNode node, List< PhysicalOpNode > list)| void  |
|**[getFesqlType](/hybridse/usage/api/c++/Classes/classcom_1_1__4paradigm_1_1hybridse_1_1sdk_1_1_d_d_l_engine.md#function-getfesqltype)**(String type)| TypeOuterClass.Type  |
|**[getDDLType](/hybridse/usage/api/c++/Classes/classcom_1_1__4paradigm_1_1hybridse_1_1sdk_1_1_d_d_l_engine.md#function-getddltype)**(TypeOuterClass.Type type)| String  |
|**[getRtidbIndexType](/hybridse/usage/api/c++/Classes/classcom_1_1__4paradigm_1_1hybridse_1_1sdk_1_1_d_d_l_engine.md#function-getrtidbindextype)**(TTLType type)| String  |
|**[getTableDefs](/hybridse/usage/api/c++/Classes/classcom_1_1__4paradigm_1_1hybridse_1_1sdk_1_1_d_d_l_engine.md#function-gettabledefs)**(String jsonObject)| List< TypeOuterClass.TableDef >  |
|**[addEscapeChar](/hybridse/usage/api/c++/Classes/classcom_1_1__4paradigm_1_1hybridse_1_1sdk_1_1_d_d_l_engine.md#function-addescapechar)**(List< String > list, String singleChar)| List< String >  |
|**[sql2Feconfig](/hybridse/usage/api/c++/Classes/classcom_1_1__4paradigm_1_1hybridse_1_1sdk_1_1_d_d_l_engine.md#function-sql2feconfig)**(String sql, String schema)| String  |
|**[sql2Feconfig](/hybridse/usage/api/c++/Classes/classcom_1_1__4paradigm_1_1hybridse_1_1sdk_1_1_d_d_l_engine.md#function-sql2feconfig)**(String sql, TypeOuterClass.Database db)| String  |
|**[parseOpSchema](/hybridse/usage/api/c++/Classes/classcom_1_1__4paradigm_1_1hybridse_1_1sdk_1_1_d_d_l_engine.md#function-parseopschema)**(PhysicalOpNode plan)| String  |



| **Public attributes**|    |
| -------------- | -------------- |
| **[SQLTableName](/hybridse/usage/api/c++/Classes/classcom_1_1__4paradigm_1_1hybridse_1_1sdk_1_1_d_d_l_engine.md#variable-sqltablename)**| String  |

## Public Functions

#### function resolveColumnIndex

```cpp
static inline int resolveColumnIndex(
    ExprNode expr,
    PhysicalOpNode planNode
)
```


#### function genDDL

```cpp
static inline String genDDL(
    String sql,
    String schema,
    int replicanum,
    int partitionnum
)
```


#### function genDDL

```cpp
static inline String genDDL(
    String sql,
    String schema
)
```


**Parameters**: 

  * **sql** 
  * **schema** json format 


**Return**: 

#### function parseWindowOp

```cpp
static inline void parseWindowOp(
    PhysicalOpNode node,
    Map< String, RtidbTable > rtidbTables
)
```

只对window op做解析，因为fesql node类型太多了，暂时没办法做通用性解析 

#### function parseLastJoinOp

```cpp
static inline void parseLastJoinOp(
    PhysicalOpNode node,
    Map< String, RtidbTable > rtidbTables
)
```


#### function parseRtidbIndex

```cpp
static inline Map< String, RtidbTable > parseRtidbIndex(
    List< PhysicalOpNode > nodes,
    Map< String, TypeOuterClass.TableDef > tableDefMap
)
```


#### function findDataProviderNode

```cpp
static inline PhysicalDataProviderNode findDataProviderNode(
    PhysicalOpNode node
)
```


#### function dagToList

```cpp
static inline void dagToList(
    PhysicalOpNode node,
    List< PhysicalOpNode > list
)
```


#### function getFesqlType

```cpp
static inline TypeOuterClass.Type getFesqlType(
    String type
)
```


#### function getDDLType

```cpp
static inline String getDDLType(
    TypeOuterClass.Type type
)
```


#### function getRtidbIndexType

```cpp
static inline String getRtidbIndexType(
    TTLType type
)
```


#### function getTableDefs

```cpp
static inline List< TypeOuterClass.TableDef > getTableDefs(
    String jsonObject
)
```


#### function addEscapeChar

```cpp
static inline List< String > addEscapeChar(
    List< String > list,
    String singleChar
)
```


#### function sql2Feconfig

```cpp
static inline String sql2Feconfig(
    String sql,
    String schema
)
```


#### function sql2Feconfig

```cpp
static inline String sql2Feconfig(
    String sql,
    TypeOuterClass.Database db
)
```


#### function parseOpSchema

```cpp
static inline String parseOpSchema(
    PhysicalOpNode plan
)
```


## Public Attributes

### variable SQLTableName

```cpp
static String SQLTableName = "sql_table";
```


