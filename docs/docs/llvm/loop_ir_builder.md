# loop ir builder 设计

loop ir builder会作为存储迭代器与计算逻辑的衔接点

## 输入参数

* 迭代器指针， 迭代器的value为行数据指针
* condition 函数指针，可以为空
* apply 函数指针, 可以为空
* limit, 满足条件条数限制

## 返回值 

迭代器指针，不会为空
