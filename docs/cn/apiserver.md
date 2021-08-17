# APIServer

APIServer的部署见[部署OpenMLDB](deploy.md)。

JSON数据类型与OpenMLDB数据类型对照，与各方法的具体细节，见[RFC](https://github.com/4paradigm/rfcs/blob/main/fedb/api-server.md)。 

## 使用说明

假设OpenMLDB已创建数据库db，与表trans：

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

APIServer为本地启动，端口8080。

考虑到传输效率，response使用紧凑型JSON格式，但为了方便展示，下文的repsonse都经过格式化处理（换行和缩进）。

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

目前仅支持一条插入，不可以插入多条数据。数据需严格按照schema排列。

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

首先应先创建procedure `sp`：

```
create procedure sp (const c1 string, const c3 int, c4 bigint, c5 float, c6 double, const c7 timestamp, c8 date)
begin SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS(PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW); end;
```

然后执行：

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

+ common_cols没有数据时，也可以不填入这个字段。
+ need_schema可以设置为false或者不填此字段，则response中不会有schema字段。

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
