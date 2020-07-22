# 基于llvm动态生成ir编解码row

row 格式参考[schema](../schema.md)

## 解析属性

假设schema为

```
create table test(
    column1 int,
    column2 timestamp,
    column3 int,
    column4 string,
    column5 int
)
```

通过llvm gep 访问第一列为

```
; 生成与schema映射的struct
%0 = type { i32, i64, i32, i16, i32}

; 输入指针已经是加上了两个偏移量,跳过header
define i32 @access(i8*) {
EntryBlock:
  ; 将指针转换成 struct
  %ptr_cast = bitcast i8* %0 to %0* 
  ; 获取column 1 的指针
  %get_int32 = getelementptr %0, %0* %ptr_cast, i32 0, i32 0
  ; 加载指针内容
  %load = load i32, i8* %get_int32
  ret i32 %load
}
```



通过llvm inttoptr 和 ptrtint 访问例子

```

```

另外一种就是通过手动操作offset(inttoptr/ptrtoint)去访问, 这两种方式区别参考[GetElementPtr](http://releases.llvm.org/8.0.0/docs/GetElementPtr.html#how-is-gep-different-from-ptrtoint-arithmetic-and-inttoptr)
使用gep优势是更简洁

## 存在问题

* int8/int16/bool 存在内存对齐问题 不能使用struct方式访问

既要节省内存，又要支持struct方式访问, 可以将列的顺序重新排列, 比如有两个int16 可以排在一起


