# udf function let ir builder

## 输入参数

* row 指针，i8*
* output 指针，i8*

## 返回参数int32_t

## 外部函数调用

第一个函数为一行数据，
第二个指针分配内存大写可以根据builder输出的schema提前计算出来

```c++
// the row ptr
char* row = xxxx;
// 建议在栈上面快速分配内存
char output[size];

int32_t ret = udf_fn_apply(row,
output);
```

