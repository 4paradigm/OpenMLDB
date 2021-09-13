### CICD流程
HybridSQL-test工程的CICD提供两种方式：
+ 按版本进行测试
+ ~~dailybuild方式（还需要调整）~~

##### 按版本运行Case
按版本运行case，是在FEDB的Server或者SDK发布了版本后进行测试， 测试步骤如下：
1. 修改steps下的fedb_sdk_test.properties
   
   | 字段 | 说明 |
   | :--- | :--- |
   |FEDB_SDK_VERSION|表示要测试的FEDB JAVA SDK的版本号|
   |FEDB_SERVER_VERSION|表示要测试的FEDB SERVER的版本|
   |FEDB_PY_SDK_VERSION|表示要测试的FEDB PYTHON SDK的版本号|
   |FEDB_VERSIONS|表示对比的SERVER的版本号，这个目前只在执行自动生成的case时才生效|

2. 提交代码执行CICD即可。
3. 测试报告
   + 地址：http://auto.4paradigm.com/view/%E7%89%B9%E5%BE%81%E5%BC%95%E6%93%8E/job/fesql-test-report/
   + 根据pipeline_id和job_id查看对应的测试报告即可。
4. 其他说明
   + gen_case_standalone_level_0 为可选执行，根据自己的情况选择执行，如遇失败最好找一下原因。
   + CICD执行有可能出现失败的情况，重试即可，如三次失败需要查找原因。

##### dailybuild
+ dailybuild目前不需要进行配置
+ dailybuild是从github上拉取FEDB的代码，然后进行编译部署
+ JAVA SDK的版本号从代码的POM文件中读取
+ PYTHON SDK的WHL编译生成。
+ 每日凌晨四点执行测试
+ 目前case执行失败的成功率很低，调整后通知大家。

#### case失败处理步骤
1. 在本地单独执行失败的case，以确定原因，本地执行参考[测试运行说明](docs/run-case-desc.md)。
2. 如果本地执行没有问题，则可以重试CICD。
3. 如执行三次还是失败则需要进一步查找原因。
4. 排查可以看cicd的log，也可以看报告中的log辅助排查。
   