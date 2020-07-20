# string 设计

## StringRef

在c/c++ 中输入为char*, 在llvm中转换成一个StringRef struct

```
; i32表示字符串长度， i8*表示字符串指针
type {i32, const i8*}
```

注：非优化版本

stringref
不会复制string，一般用于从行数据解析出字符串，不用关心内存问题，可以在栈上面分配

## String 

采用cow模式，类似std::string ，在llvm中转换一个String struct 

```
; i32表示字符串长度， i8*表示字符串指针
type {i32, i8*}
```





