# REST API

## Important information

REST APIs interact with the services of APIServer and OpenMLDB, so the APIServer module must be properly deployed to be used effectively. APIServer is an optional module during installation and deployment. Refer to the APIServer deployment document.

At this stage, APIServer is mainly used for functional testing, not recommended for performance testing, nor recommended for the production environment. The default deployment of APIServer does not have a high availability mechanism at present and introduces additional network and codec overhead.

## Data insertion

Request address: http://ip:port/dbs/{db_name}/tables/{table_name}

Request method: PUT

The requestor:

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

## Real-time feature computing

Request address: http://ip:port/dbs/{db_name}/deployments/{deployment_name}

Request method: POST

Requestor

- Array format:

```json
{
    "input": [["row0_value0", "row0_value1", "row0_value2"], ["row1_value0", "row1_value1", "row1_value2"], ...],
    "need_schema": false
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

- When the input is in array format/ JSON format, the returned result is also in array format/ JSON format. The input requested at a time only supports one format. Please do not mix formats.

- Input data in JSON format can have redundant columns.

**Sample request data**

Example 1: Array format

```plain
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
curl http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service -X POST -d'{
  "input": [{"c1":"aaa", "c2":11, "c3":22, "c4":1.2, "c5":1.3, "c6":1635247427000, "c7":"2021-05-20", "foo":"bar"}]
    }'
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

Request address: http://ip:port/dbs/ {db_name}

Request method: POST

Requestor:

```JSON
{
    "mode": "",
    "sql": "",
    "input": {
        "schema": [],
        "data": []
    }
}
```

Request parameters:

| Parameters | Type   | Requirement | Description                                                  |
| ---------- | ------ | ----------- | ------------------------------------------------------------ |
| mode       | String | Yes         | Available for `offsync` , `offasync`, `online`               |
| sql        | String | Yes         |                                                              |
| input      | Object | No          |                                                              |
| schema     | Array  | No          | Support data types (case insensitive): `Bool`, `Int16`, `Int32`, `Int64`, `Float`, `Double`, `String`, `Date and Timestamp` |
| data       | Array  | No          |                                                              |

**Sample request data**

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

## Query deployment information

Request address: http://ip:port/dbs/{db_name}/deployments/{deployment_name}

Request method: GET

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

## Acquire all library names

Request address: http://ip:port/dbs

Request method: GET

Response:

```json
{
  "code": 0,
  "msg": "ok",
  "dbs": [

  ]
}
```

## Acquire all table names

Request address: http://ip:port/dbs/{db}/tables

Request method: GET

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

## Refresh APIServer metadata cache

Request address: http://ip:port/refresh

Request method: POST

Response:

```json
{
    "code":0,
    "msg":"ok"
}
```

