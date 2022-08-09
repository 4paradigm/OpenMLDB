# REST APIs

## Data Insertion

The request URL: http://ip:port/dbs/{db_name}/tables/{table_name}

HTTP method: PUT 

The request body: 
```json
{
    "value": [
    	[v1, v2, v3]
    ]
}
```

+ Only one record can be inserted at a time.
+ The data layout should be arranged according to the schema strictly.

**Example**

```batch
curl http://127.0.0.1:8080/dbs/db/tables/trans -X PUT -d '{
"value": [
    ["bb",24,34,1.5,2.5,1590738994000,"2020-05-05"]
]}'
```
The response:

```json
{
    "code":0,
    "msg":"ok"
}
```

## Real-Time Feature Extraction

The request URL: http://ip:port/dbs/{db_name}/deployments/{deployment_name}

HTTP method: POST

The request body: 

```
{
    "input": [["row0_value0", "row0_value1", "row0_value2"], ["row1_value0", "row1_value1", "row1_value2"], ...],
    "need_schema": false
}
```

+ Multiple rows of input are supported, whose returned values correspond to the fields in the `data.data` array.
+ A schema will be returned if `need_schema`  is `true`. Default: `false`.

**Example**

```bash
curl http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service -X POST -d'{
        "input": [["aaa", 11, 22, 1.2, 1.3, 1635247427000, "2021-05-20"]],
    }'
```

The response:

```json
{
    "code":0,
    "msg":"ok",
    "data":{
        "data":[["aaa",11,22]]
    }
}
```

## Query

The request URL: http://ip:port/dbs/{db_name}

HTTP method: POST

The request body example: 

```json
{
    "mode": "online",
    "sql": "select 1"
}
```

mode: "offsync", "offasync", "online"

The response:

```json
{
    "code":0,
    "msg":"ok"
}
```

## Get Deployment Info


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


## List Database

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

## List Table

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

## Refresh

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