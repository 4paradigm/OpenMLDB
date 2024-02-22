# REST API

## Important

- REST APIs interact with the services of APIServer and OpenMLDB, so the APIServer module must be properly deployed to be used effectively. APIServer is an optional module during installation and deployment. Refer to [APIServer Deployment](../../deploy/install_deploy.md).

- At this stage, APIServer is mainly used for functional testing, not recommended for performance testing, nor recommended for the production environment. The default deployment of APIServer does not have a high availability mechanism at present and introduces additional network and codec overhead.

## JSON Body

In interactions with the APIServer, the request body is in JSON format and supports some extended formats. Note the following points:

- Numeric values exceeding the maximum value for integers or floating-point numbers will result in parsing failure, for example, a double type with the value `1e1000`.
- Non-numeric floating-point numbers: When passing data, support for `NaN`, `Infinity`, `-Infinity`, and their abbreviations `Inf`, `-Inf` is provided (note that these are unquoted and not strings, and other variant notations are not supported). When returning data, support is provided for `NaN`, `Infinity`, and `-Infinity` (variant notations are not supported). If you want to convert these to `null`, you can configure `write_nan_and_inf_null`.
- Integer numbers can be passed as floating-point numbers, for example, `1` can be read as a double.
- Floating-point numbers may have precision loss, for example, `0.3` will not strictly equal `0.3` when read but rather `0.30000000000000004`. We do not reject precision loss, so consider whether you need to handle it from the business perspective. Values passed that exceed `float` max but do not exceed `double` max will become `Inf` when read.
- `true/false`, `null` are not case-sensitive and only support lowercase.
- The timestamp type currently does not support passing in year-month-day strings; it only supports passing in numeric values, for example, `1635247427000`.
- For the date type, please pass in a **year-month-day string** without any spaces in between.

## Data Insertion

Request url: http://ip:port/dbs/{db_name}/tables/{table_name}

http method: PUT

request body:

```JSON
  {
       "value": [
        [v1, v2, v3]
      ]
  } 
```

- Currently, it only supports inserting one piece of data.
- The data should be arranged in strict accordance with the schema.

Sample request data:

```bash
curl http://127.0.0.1:8080/dbs/db/tables/trans -X PUT -d '{
"value": [
    ["bb",24,34,1.5,2.5,1590738994000,"2020-05-05"]
]}'
```

Response:

```json
{
    "code":0,
    "msg":"ok"
}
```

## Real-Time Feature Computing

Request url: http://ip:port/dbs/{db_name}/deployments/{deployment_name}

http method: POST

request body: 

- Array format:

```json
{
    "input": [["row0_value0", "row0_value1", "row0_value2"], ["row1_value0", "row1_value1", "row1_value2"], ...],
    "need_schema": false,
    "write_nan_and_inf_null": false
}
```

- JSON format:

```json
{
    "input": [
      {"col0":"row0_value0", "col1":"row0_value1", "col2":"row0_value2", "foo": "bar"}, 
      {"col0":"row1_value0", "col1":"row1_value1", "col2":"row1_value2"}, 
      ...
    ]
}
```

- It can support multiple rows, and its results correspond to the array of data.data fields in the returned response one by one.
- need_schema can be set to true, and the schema with output results will be returned. For optional parameter, the default is false.
- `write_nan_and_inf_null` can be set to true, it is an optional parameter, and the default is false. If set to true, when there are NaN, Inf, or -Inf in the output data, they will be converted to null.
- When the input is in array format/ JSON format, the returned result is also in array format/ JSON format. The input requested at a time only supports one format. Please do not mix formats.
- Input data in JSON format can have redundant columns.

**Sample Request Data**

Example 1: Array format

```Plain
curl http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service -X POST -d'{
        "input": [["aaa", 11, 22, 1.2, 1.3, 1635247427000, "2021-05-20"]]
    }'
```

Response:

```JSON
{
    "code":0,
    "msg":"ok",
    "data":{
        "data":[["aaa",11,22]]
    }
}
```

Example 2: JSON format

```JSON
curl http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service -X POST -d'{"input": [{"c1":"aaa", "c2":11, "c3":22, "c4":1.2, "c5":1.3, "c6":1635247427000, "c7":"2021-05-20", "foo":"bar"}]}'
```

Response:

```JSON
{
    "code":0,
    "msg":"ok",
    "data":{
        "data":[{"c1":"aaa","c2":11,"w1_c3_sum":22}]
    }
}
```

## Query

Request url: http://ip:port/dbs/{db_name}

http method: POST

request bpdy:

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

Request parameters:

| **Parameters** | **Type**   | **Requirement** | **Description**                                                  |
| ---------- | ------ | ----------- | ------------------------------------------------------------ |
| mode       | String | Yes         | Set to `offsync` , `offasync`, `online`                      |
| sql        | String | Yes         |                                                              |
| input      | Object | No          |                                                              |
| schema     | Array  | No          | Support data types (case insensitive): `Bool`, `Int16`, `Int32`, `Int64`, `Float`, `Double`, `String`, `Date and Timestamp` |
| data       | Array  | No          | Schema and data must both exist                              |

**Sample Request Data**

Example 1: General query

```JSON
{
  "mode": "online",
  "sql": "select 1"
}
```

Response:

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

Example 2: Parametric query

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

Response:

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

## Query Deployment Information

Request url: http://ip:port/dbs/{db_name}/deployments/{deployment_name}

http method: GET

Response:

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

## Acquire All Library Names

Request url: http://ip:port/dbs

http method: GET

Response:

```json
{
  "code": 0,
  "msg": "ok",
  "dbs": [

  ]
}
```

## Acquire All Table Names

Request url: http://ip:port/dbs/{db}/tables

http method: GET

Response:

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

## Refresh APIServer Metadata Cache

After performing operations such as creating or deleting tables, deployments, etc., there may be some latency in metadata synchronization. If you find that the APIServer cannot locate newly created tables or deployments, please try refreshing the cache first.

Request address: http://ip:port/refresh

Request method: POST

Response:

```json
{
    "code":0,
    "msg":"ok"
}
```

