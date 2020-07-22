#类型系统

## 内置类型系统

内置类型是FESQL内部处理类型，它包括基础的存储类型，计算过程中生成的内置的复合类型以及用户自定义的类型（未来），包括：



```c++
enum {
	kInt16,
	kInt32,
	kInt64,
	kFloat,
	kDouble,
	kTimestamp,
	kDate,
  kList,
  kMap,
  kIterator,
  kFunction,
  kVoid,
  kNull
}
```



## 存储类型

FESQL的存储支持的类型，包括：
```c++
enum {
	kInt16,
	kInt32,
	kInt64,
	kFloat,
	kDouble,
	kTimestamp,
	kDate
}
```

## SDK类型

用户使用FESQL的SDK时候，使用的数据类型，包括：

```c++
enum {
	kInt16,
	kInt32,
	kInt64,
	kFloat,
	kDouble,
	kTimestamp,
	kDate
}
```

## LLVM类型

FESQL处理查询脚本后codegen使用的类型



上述几种类型之间的关系：

![image-20200221175229641](./image/type_convert.png)