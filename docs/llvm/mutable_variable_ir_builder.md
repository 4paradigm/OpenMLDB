# mutable variable

SSA形式的IR主要特征是每个变量只赋值一次;

首先看一下维基对静态单一赋值（SSA）形式的定义：
>
	In compiler design, static single assignment form (often abbreviated as SSA form or simply SSA) is a property of an intermediate representation (IR), which requires that each variable is assigned exactly once, and every variable is defined before it is used.
	– From Wikipedia

简单说，在LLVM中，每个变量只能赋值一次；那么，在下面的函数中，z被赋值了三次。显然，z是一个可变变量。

```python

def test(x:int, y:int):int
  sum = 0
  sum = sum + x
  sum = sum + y
  sum = sum + 1
  return sum

```

为了解决这个场景的codegen，有一个技巧：虽然LLVM确实要求所有寄存器值都是SSA形式，但它不要求（或允许）存储器对象采用SSA形式。
即：为可变变量在堆栈中开辟空间，将变量的赋值操作转化成alloca-store-load的操作.

那么，上述这段函数定义可以codegen为IR代码如下：

```python
; ModuleID = 'test_fn'
source_filename = "test_fn"
define i32 @test(i32, i32) {
entry:
  %2 = alloca i32
  store i32 0, i32* %2
  %3 = load i32, i32* %2
  %expr_add = add i32 %3, %0
  store i32 %expr_add, i32* %2
  %4 = load i32, i32* %2
  %expr_add1 = add i32 %4, %1
  %5 = load i32, i32* %2
  store i32 %expr_add2, i32* %2
  %6 = load i32, i32* %2
  ret i32 %6
}
```

