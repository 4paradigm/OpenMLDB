# REST API

## 重要信息

- REST APIs 通过 APIServer 和 OpenMLDB 的服务进行交互，因此 APIServer 模块必须被正确部署才能有效使用。APISever 在安装部署时是可选模块，参照 [APIServer 部署文档](../deploy/install_deploy.md#部署-APIServer)。
- 现阶段，APIServer 主要用来做功能测试使用，并不推荐用来测试性能，也不推荐在生产环境使用。APIServer 的默认部署目前并没有高可用机制，并且引入了额外的网络和编解码开销。

## 数据插入

reqeust url: http://ip:port/dbs/{db_name}/tables/{table_name}

http method: PUT 

request body: 
```
{
    "value": [
    	[v1, v2, v3]
    ]
}
```

+ 目前仅支持一条插入，不可以插入多条数据。
+ 数据需严格按照 schema 排列。

**数据插入举例**

```
curl http://127.0.0.1:8080/dbs/db/tables/trans -X PUT -d '{
"value": [
    ["bb",24,34,1.5,2.5,1590738994000,"2020-05-05"]
]}'
```
response:

```
{
    "code":0,
    "msg":"ok"
}
```

## 实时特征计算

reqeust url: http://ip:port/dbs/{db_name}/deployments/{deployment_name}

http method: POST

request body: 

- array style
```json
{
    "input": [["row0_value0", "row0_value1", "row0_value2"], ["row1_value0", "row1_value1", "row1_value2"], ...],
    "need_schema": false
}
```
- json style
```json
{
    "input": [
      {"col0":"row0_value0", "col1":"row0_value1", "col2":"row0_value2", "foo": "bar"}, 
      {"col0":"row1_value0", "col1":"row1_value1", "col2":"row1_value2"}, 
      ...
    ]
}
```

+ 可以支持多行，其结果与返回的 response 中的 data.data 字段的数组一一对应。
+ need_schema 可以设置为 true, 返回就会有输出结果的 schema。可选参数，默认为 false。
+ input为array style时返回结果也是array style，input为json style则是返回json style的结果。一次request请求的input只支持一种格式，请不要混合格式。
+ json style的input数据可以有多余列。

**实时特征计算举例**
- array style
```
curl http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service -X POST -d'{
        "input": [["aaa", 11, 22, 1.2, 1.3, 1635247427000, "2021-05-20"]]
    }'
```

response:

```json
{
    "code":0,
    "msg":"ok",
    "data":{
        "data":[["aaa",11,22]]
    }
}
```

- json style
```
curl http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service -X POST -d'{
        "input": [{"c1":"aaa", "c2":11, "c3":22, "c4":1.2, "c5":1.3, "c6":1635247427000, "c7":"2021-05-20", "foo":"bar"}]
    }'
```

response:

```json
{
    "code":0,
    "msg":"ok",
    "data":{
        "data":[{"c1":"aaa","c2":11,"w1_c3_sum":22}]
    }
}
```

## 查询

The request URL: http://ip:port/dbs/{db_name}

HTTP method: POST
 
request body: 

```json
{
    "mode": "",
    "sql": "",
    "input": {
        "schema": [],
        "data": []
    }
}
```

- "mode"可配: "offsync", "offasync", "online"
- "input"是可选参数
- "schema"可支持数据类型(大小写不敏感):
`Bool`, `Int16`, `Int32`, `Int64`, `Float`, `Double`, `String`, `Date` and `Timestamp`.

**查询举例**

- 普通查询: 

```json
{
  "mode": "online",
  "sql": "select 1"
}
```

The response:

```json
{
  "code":0,
  "msg":"ok",
  "data": {
    "schema":["Int32"],
    "data":[[1]]
  }
}
```

- 参数化查询:

```json
{
  "mode": "online",
  "sql": "SELECT c1, c2, c3 FROM demo WHERE c1 = ? AND c2 = ?",
  "input": {
    "schema": ["Int32", "String"],
    "data": [1, "aaa"]
  }
}
```

The response:

```json
{
    "code":0,
    "msg":"ok",
    "data": {
      "schema": ["Int32", "String", "Float"],
      "data": [[1, "aaa", 1.2], [1, "aaa", 3.4]]
    }
}
```

## 查询Deployment信息


The request URL: http://ip:port/dbs/{db_name}/deployments/{deployment_name}

HTTP method: Get

The response:

```json
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

The request URL: http://ip:port/dbs

HTTP method: Get

The response:

```json
{
  "code": 0,
  "msg": "ok",
  "dbs": [

  ]
}
```

## 获取所有表名

The request URL: http://ip:port/dbs/{db}/tables

HTTP method: Get

The response:

```json
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

The request URL: http://ip:port/refresh

HTTP method: POST

Empty request body.

The response:

```json
{
    "code":0,
    "msg":"ok"
}
```
