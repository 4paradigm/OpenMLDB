# REST APIs

## Data Insertion

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

+ Only one data can be inserted at a time.
+ The data should be arranged according to the schema strictly.

### Examples

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

## Real-Time Feature Computation

reqeust url: http://ip:port/dbs/{db_name}/deployments/{deployment_name}

http method: POST

request body: 

```
{
    "input": [["row0_value0", "row0_value1", "row0_value2"], ["row1_value0", "row1_value1", "row1_value2"], ...],
    "need_schema": false
}
```

+ Multiple rows input are supported which correspond to the array of data.data fields in the returned value.
+ A schema will be returned if `need_schema`  if `true`. Default: false.

### Examples

```
curl http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service -X POST -d'{
        "input": [["aaa", 11, 22, 1.2, 1.3, 1635247427000, "2021-05-20"]],
    }'
```

response:

```
{
    "code":0,
    "msg":"ok",
    "data":{
        "data":[["aaa",11,22]]
    }
}
```