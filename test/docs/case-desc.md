### case格式说明
- hybridsql的case采用yml格式。
- 具体字段含义如下：
    1. 全局字段说明

       | 字段 | 说明 |
               | :--- | :--- |
       |~~db~~|在哪一个数据库执行case，此字段后期可能删除|
       |debugs|用来调试单个case，字符串列表，需要填写case的desc|
       |cases|case列表|
       |sqlDialect|sql的方言，字符串列表，目前支持三种值：ANSISQL、HybridSQL、MYSQL|

    2. case字段说明

       | 字段 | 说明 |
               | :--- | :--- |
       |id|case的id|
       |desc|case的描述|
       |inputs|case执行前需要创建表的信息或需要插入的数据，列表，可以有多个表|
       |sql|执行的sql语句，sql中有一些特殊的标志，用来进行替换，可查看sql特殊标记列表|
       |expect|预期结果|
       |dataProvider|用来给sql语句做参数化，二位数组，会替换sql里的变量用来生成case，变量为d[数子]，数子为0找第一个列表里的数据依次替换生成不同的case，依次类推，多个列表生成的case采用笛卡尔积的方式,例如：一个2*3的二位数组则生成6个case|
       |expectProvider|dataProvider的预期结果，目前这个预期结果只支持dataProvider中index为0的列表的结果验证，这个一个map，key为对应的下标，value为结果验证同expect，如果有的字段expectProvider没有则从expect取，都没有不进行验证|

    3. input字段说明

       | 字段 | 说明 |
               | :--- | :--- |
       |name|表名，如果不指定则自动生成表名，格式为：auto_+8位随机字符|
       |columns|表的字段名称以及字段类型，列名，格式为："columnName columnType"|
       |indexs|表的索引，字符串列表，格式为："indexName:columnName:tsName:[过期条件]:[tsType]"，[]为可选项|
       |rows|插入的数据，二位数组|
       |create|创建表的sql语句|
       |insert|插入数据的sql语句|

    4. expect字段说明

       | 字段 | 说明 |
               | :--- | :--- |
       |success|验证sql语句执行成功还是失败|
       |columns|验证sql语句执行结果的列名和类型，同input里的columns|
       |rows|验证sql语句执行结果的数据，同input里的rows|
       |order|sql的执行结果根据哪一个字段进行排序，排序后验证结果|
       |count|验证sql语句执行结果的条数|

    5. case中特殊标记说明

       |标记|说明|使用范围|
               | :--- | :--- | :--- |
       |{auto}|自动生成表名|sql中使用|
       |{数字}|表名替换，使用inputs里创建的表的表名进行替换，数字表示使用第几个表的表名，从0开始，0表示第1个表的表名|sql中使用|
       |d[数字]|根据dataProvider生成新的case，数字表示使用dataProvider中第几个列表中的数据，从0开始|sql中使用|
       |{currentTime}|表示当前时间戳|在input的rows中使用|
       |{currentTime}+数字|根据当前时间戳加上响应的毫秒数|在input的rows中使用|
       |{currentTime}-数字|根据当前时间戳减去响应的毫秒数|在input的rows中使用|


- demo
    1. 基础case
    ```yaml
    db: test_zw      #数据库名字
    debugs: []       #调试case列表
    cases:           #case列表
      - id: 0        #case的id
        desc: 查询所有列    #case的描述信息
        inputs:           #case执行前需要创建表或插入的数据
          - columns: ["c1 string","c3 int","c4 bigint","c5 float","c6 double","c7 timestamp","c8 date"]  #表的schema信息
            indexs: ["index1:c1:c7"]    #表的索引信息
            rows:      #表插入的数据，二维列表
              - ["aa",2,3,1.1,2.1,1590738989000,"2020-05-01"]
        sql: select c1,c3,c4,c5,c6,c7,c8 from {0};    #测试的sql语句，注意后面要带分号
        expect:    #预期结果
          columns: ["c1 string","c3 int","c4 bigint","c5 float","c6 double","c7 timestamp","c8 date"]   #sql执行结果的schema
          rows:    #sql的执行结果，二维列表
            - ["aa",2,3,1.1,2.1,1590738989000,"2020-05-01"]
    ```
    2. dataprovider的case
    ```yaml
      - id: 2
        desc: 日期函数-normal
        inputs:
          -
            columns : ["id bigint","ts1 bigint","c1 string","c2 smallint","c3 int","c4 bigint","c5 float","c6 double","c7 timestamp","c8 date","c9 bool"]
            indexs: ["index1:id:ts1"]
            rows:
              - [1,1,"aa",30,-30,30,30.0,30.0,1590738989000,"2020-05-02",true]
              - [2,2,"aa",30,-30,NULL,30.0,30.0,NULL,NULL,true]
        dataProvider:    #配置dataProvider,用来替换sql里的变量，二维数组，这个case会生成3个case依次执行测试
          - ["{0}.c4","{0}.c7","{0}.c8"]
        sql: |
          select id as id,
            day(d[0]) as e1,
            dayofmonth(d[0]) as e2,
            dayofweek(d[0]) as e3,
            month(d[0]) as e4,
            week(d[0]) as e5,
            weekofyear(d[0]) as e6,
            year(d[0]) as e7
            from {0};
        expect:
          order: id
          columns: ["id bigint", "e1 int","e2 int","e3 int","e4 int","e5 int","e6 int","e7 int"]
        expectProvider:
          0:
            rows:
              - [1,1,1,5,1,1,1,1970]
              - [2,null,null,null,null,null,null,null]
          1:
            rows:
              - [1,29,29,6,5,22,22,2020]
              - [2,null,null,null,null,null,null,null]
          2:
            rows:
              - [1,2,2,7,5,18,18,2020]
              - [2,null,null,null,null,null,null,null]
    ```