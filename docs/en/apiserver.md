# APIServer

To deploy `APIServer` click this: [Deploy OpenMLDB](deploy.md).

Please refer to this [RFC](https://github.com/4paradigm/rfcs/blob/main/fedb/api-server.md) for the comparison of JSON and OpenMLDB data types.

## Instructions

Assuming you already have a database and a table for OpenMLDB:

```
create table trans(c1 string,
                   c3 int,
                   c4 bigint,
                   c5 float,
                   c6 double,
                   c7 timestamp,
                   c8 date,
                   index(key=c1, ts=c7));
```

then start APIServer at localhost, port is 8080.

Considering the transmission efficiency，the response uses Compact JSON format. But for the convenience of display, the repsonse below is formatted(Line breaks and indents).

### Put

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

Currently only support inserting one record, data arranged in strict accordance with the schema in table.

#### example

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

### GetProcedure

request url: http://ip:port/dbs/{db_name}/procedures/{procedure_name} 

http method: Get

#### example

First of all, procedure(`sp`) should be created:

```
create procedure sp (const c1 string, const c3 int, c4 bigint, c5 float, c6 double, const c7 timestamp, c8 date)
begin SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS(PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW); end;
```

then execute:

```
curl http://127.0.0.1:8080/dbs/db/procedures/sp
```

response:

```
{
    "code":0,
    "msg":"ok",
    "data":{
        "name":"sp",
        "procedure":"create procedure sp (const c1 string, const c3 int, c4 bigint, c5 float, c6 double, const c7 timestamp, c8 date)
begin SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS(PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW); end;",
        "input_schema":[
            {
                "name":"c1",
                "type":"string"
            },
            {
                "name":"c3",
                "type":"int32"
            },
            {
                "name":"c4",
                "type":"int64"
            },
            {
                "name":"c5",
                "type":"float"
            },
            {
                "name":"c6",
                "type":"double"
            },
            {
                "name":"c7",
                "type":"timestamp"
            },
            {
                "name":"c8",
                "type":"date"
            }
        ],
        "input_common_cols":[
            "c1",
            "c3",
            "c7"
        ],
        "output_schema":[
            {
                "name":"c1",
                "type":"string"
            },
            {
                "name":"c3",
                "type":"int32"
            },
            {
                "name":"w1_c4_sum",
                "type":"int64"
            }
        ],
        "output_common_cols":[
            "c1",
            "c3"
        ],
        "tables":[
            "trans"
        ]
    }
}
```

### Execute Procedure 

reqeust url: http://ip:port/dbs/{db_name}/procedures/{procedure_name}

http method: POST

request body: 

```
{
    "common_cols":["value1", "value2"],
    "input": [["value0", "value3"],["value0", "value3"]],
    "need_schema": true
}
```

+ common_cols could be empty if there is no data.
+ need_schema could be `false` or just empty, then there will be no schema field in response.

#### example

```
curl http://127.0.0.1:8080/dbs/db/procedures/sp -X POST -d'{
        "common_cols":["bb", 23, 1590738994000],
        "input": [[123, 5.1, 6.1, "2021-08-01"],[234, 5.2, 6.2, "2021-08-02"]],
        "need_schema": true
    }'
```

response:

```
{
    "code":0,
    "msg":"ok",
    "data":{
        "schema":[
            {
                "name":"c1",
                "type":"string"
            },
            {
                "name":"c3",
                "type":"int32"
            },
            {
                "name":"w1_c4_sum",
                "type":"int64"
            }
        ],
        "data":[
            [
                157
            ],
            [
                268
            ]
        ],
        "common_cols_data":[
            "bb",
            23
        ]
    }
}
```
