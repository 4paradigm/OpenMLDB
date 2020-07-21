# sql多节点执行

## sql计划节点传递方案

备选方案
* 传递生成好的ir, ir是文本
* 传递bitcode, bitcode是binary , 调研case查看 experiments/bitcode/bitcode_test.cc

对比需要测试
* ir->module效率
* bitcode->module效率 
* ir 字节大小 与 bitcode对比

```
; ModuleID = 'test'
source_filename = "test"

define i32 @main() {
EntryBlock:
   %addresult = add i32 2, 3
   ret i32 %addresult
}
```
初步对比结果
* ir 比 bitcode的字节数小很多,快一个数量级了


## scan op

生成迭代器op，所需字段列表
* db  database name
* table 从哪一张表生成迭代器

返回直为迭代器，这个op可以和其他op 合并

## project op

生成列op，所需要字段列表

* bitcode / ir, 待执行代码 
* 输出schema 
* id, 用于查找codegen函数的key
* fn_symbol, 函数的入口名称
* fn_proto_key, 函数定义key，用于获取函数的定义
