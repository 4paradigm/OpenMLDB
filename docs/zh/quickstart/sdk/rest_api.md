# REST API

## 重要信息

- REST APIs 通过 APIServer 和 OpenMLDB 的服务进行交互，因此 APIServer 模块必须被正确部署才能有效使用。APISever 在安装部署时是可选模块，参照 [APIServer 部署文档](../../deploy/install_deploy.md#部署-apiserver)。
- 现阶段，APIServer 主要用来做功能测试使用，并不推荐用来测试性能，也不推荐在生产环境使用。APIServer 的默认部署目前并没有高可用机制，并且引入了额外的网络和编解码开销。生产环境推荐使用 Java SDK，功能覆盖最完善，并且在功能、性能上都经过了充分测试。

## JSON Body

与APIServer的交互中，请求体均为JSON格式，并支持一定的扩展格式。注意以下几点：

- 传入超过整型或浮点数最大值的数值，将会解析失败，比如，double类型传入`1e1000`。
- 非数值浮点数：在传入数据时，支持传入`NaN`、`Infinity`、`-Infinity`，与缩写`Inf`、`-Inf`（注意是unquoted的，并非字符串，也不支持其他变种写法）。在返回数据时，支持返回`NaN`、`Infinity`、`-Infinity`（不支持变种写法）。如果你需要将三者转换为null，可以配置 `write_nan_and_inf_null`。
- 可以传入整型数字到浮点数，比如，`1`可被读取为double。
- float浮点数可能有精度损失，比如，`0.3`读取后将不会严格等于`0.3`，而是`0.30000000000000004`。我们不拒绝精度损失，请从业务层面考虑是否需要对此进行处理。传入超过float max但不超过double max的值，在读取后将成为`Inf`。
- `true/false`、`null`并不支持大写，只支持小写。
- timestamp类型暂不支持传入年月日字符串，只支持传入数值，比如`1635247427000`。
- date类型请传入**年月日字符串**，中间不要包含任何空格。

## 数据插入

请求地址：http://ip:port/dbs/{db_name}/tables/{table_name}

请求方式：PUT

请求体: 

```JSON
  {
       "value": [
        [v1, v2, v3]
      ]
  } 
```

- 目前仅支持插入一条数据。
- 数据需严格按照 schema 排列。

请求数据样例：

```Bash
curl http://127.0.0.1:8080/dbs/db/tables/trans -X PUT -d '{
"value": [
    ["bb",24,34,1.5,2.5,1590738994000,"2020-05-05"]
]}'
```

响应：

```JSON
{
    "code":0,
    "msg":"ok"
}
```

## 实时特征计算

请求地址：http://ip:port/dbs/{db_name}/deployments/{deployment_name}

请求方式：POST

请求体

- array 格式：

```JSON
{
    "input": [["row0_value0", "row0_value1", "row0_value2"], ["row1_value0", "row1_value1", "row1_value2"], ...],
    "need_schema": false,
    "write_nan_and_inf_null": false
}
```

- JSON 格式：

```JSON
{
    "input": [
      {"col0":"row0_value0", "col1":"row0_value1", "col2":"row0_value2", "foo": "bar"}, 
      {"col0":"row1_value0", "col1":"row1_value1", "col2":"row1_value2"}, 
      ...
    ]
}
```

- 可以支持多行，其结果与返回的 response 中的 data.data 字段的数组一一对应。
- need_schema 可以设置为 true, 返回就会有输出结果的 schema。可选参数，默认为 false。
- write_nan_and_inf_null 可以设置为 true，可选参数，默认为false。如果设置为 true，当输出数据中有 NaN、Inf、-Inf 时，会将其转换为 null。
- input 为 array 格式/JSON 格式时候返回结果也是 array 格式/JSON 格式，一次请求的 input 只支持一种格式，请不要混合格式。
- JSON 格式的 input 数据可以有多余列。

**请求数据样例**

例 1：array 格式

```Plain
curl http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service -X POST -d'{
        "input": [["aaa", 11, 22, 1.2, 1.3, 1635247427000, "2021-05-20"]]
    }'
```

响应：

```JSON
{
    "code":0,
    "msg":"ok",
    "data":{
        "data":[["aaa",11,22]]
    }
}
```

示例 2：JSON 格式

```JSON
curl http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service -X POST -d'{"input": [{"c1":"aaa", "c2":11, "c3":22, "c4":1.2, "c5":1.3, "c6":1635247427000, "c7":"2021-05-20", "foo":"bar"}]}'
```

响应：

```JSON
{
    "code":0,
    "msg":"ok",
    "data":{
        "data":[{"c1":"aaa","c2":11,"w1_c3_sum":22}]
    }
}
```

## 查询

请求地址：http://ip:port/dbs/{db_name}

请求方式：POST

请求体：

```JSON
{
    "mode": "",
    "sql": "",
    "input": {
        "schema": [],
        "data": []
    },
    "write_nan_and_inf_null": false
}
```

请求参数：

| **参数** | **类型** | **必需** | **说明**                                                     |
| -------- | -------- | -------- | ------------------------------------------------------------ |
| mode     | String        | 是       | 可配 `offsync` , `offasync`, `online`                        |
| sql      | String      | 是       |                                                              |
| input    | Object      | 否       |                                                              |
| schema   | Array         | 否       | 可支持数据类型（大小写不敏感）：`Bool`, `Int16`, `Int32`, `Int64`, `Float`, `Double`, `String`, `Date` and `Timestamp`. |
| data     | Array      | 否       |                                                              |

**请求数据样例**

例 1：普通查询

```JSON
{
  "mode": "online",
  "sql": "select 1"
}
```

响应：

```JSON
{
  "code":0,
  "msg":"ok",
  "data": {
    "schema":["Int32"],
    "data":[[1]]
  }
}
```

例 2：参数化查询

```JSON
{
  "mode": "online",
  "sql": "SELECT c1, c2, c3 FROM demo WHERE c1 = ? AND c2 = ?",
  "input": {
    "schema": ["Int32", "String"],
    "data": [1, "aaa"]
  }
}
```

响应：

```JSON
{
    "code":0,
    "msg":"ok",
    "data": {
      "schema": ["Int32", "String", "Float"],
      "data": [[1, "aaa", 1.2], [1, "aaa", 3.4]]
    }
}
```

## 查询 Deployment 信息

请求地址：http://ip:port/dbs/{db_name}/deployments/{deployment_name}

请求方式：GET

响应：

```JSON
{
  "code": 0,
  "msg": "ok",
  "data": {
    "name": "",
    "procedure": "",
    "input_schema": [

    ],
    "input_common_cols": [
      
    ],
    "output_schema": [

    ],
    "output_common_cols": [
      
    ],
    "dbs": [

    ],
    "tables": [

    ]
  }
}
```

## 获取所有库名

请求地址：http://ip:port/dbs

请求方式：GET

响应：

```JSON
{
  "code": 0,
  "msg": "ok",
  "dbs": [

  ]
}
```

## 获取所有表名

请求地址：http://ip:port/dbs/{db}/tables

请求方式：GET

响应：

```JSON
{
  "code": 0,
  "msg": "ok",
  "tables": [
    {
      "name": "",
      "table_partition_size": 8,
      "tid": ,
      "partition_num": 8,
      "replica_num": 2,
      "column_desc": [
        {
          "name": "",
          "data_type": "",
          "not_null": false
        }
      ],
      "column_key": [
        {
          "index_name": "",
          "col_name": [

          ],
          "ttl": {
            
          }
        }
      ],
      "added_column_desc": [
        
      ],
      "format_version": 1,
      "db": "",
      "partition_key": [
        
      ],
      "schema_versions": [
        
      ]
    }
  ]
}
```

## 刷新 APIServer 元数据缓存

创建删除表、Deploymennt等操作后，元数据同步有一定的滞后性。如果你发现APIServer找不到新建的表或Deployment，请先尝试刷新缓存。

请求地址：http://ip:port/refresh

请求方式：POST

响应：

```JSON
{
    "code":0,
    "msg":"ok"
}
```