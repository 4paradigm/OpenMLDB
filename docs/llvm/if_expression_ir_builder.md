# if expression ir builder

## 背景知识

同理，在条件控制语句中，也经常遇到一个变量在多个条件分支中被赋值的情况，此时，可以利用内存堆栈存储来解决这个问题。


``` python
def test(x:i32,y:i32):i32
	ret = 0
    if x > 1
    	ret = x+y
    elif y >2
    	ret = x-y
    else
    	ret = x*y
   	return ret
end
```

当然，有时候某些变量在一次执行过程中，实际只被赋值过一次，如上面例子的实际执行路径中，`ret`仅仅可能在if分支，elif分支以及else分支中被赋值一次。因此，实际上，ret变量是复合SSA原则，即ret是可以用寄存器来读写。为了优化这种场景的计算，一般有两种策略：

### 策略1
分析SSA列表，在适当的地方插入PHI语句。


### 策略2
使用堆栈存储的方式解决LLVM IR的寄存器重复赋值的问题。然后用LLVM的mem2reg的优化，自动将进行优化。

> 不难发现，这种方法虽然可以避免 PHI 结点的出现，却也引入了另外一个问题：由于每一次变量存取都需要访问内存，这导致了严重的性能问题。于是，LLVM 提供了一个叫做“mem2reg”的重要 pass 来处理该问题，这个 pass 可以把上述使用了 alloca 技术的 IR 转化成 SSA 形式。具体来说，它会把 alloca 指令分配的栈变量转化成 SSA 寄存器，并且在合适的地方插入 PHI 结点。

LLVM官方文档提示：
> mem2reg传递实现了用于构造SSA形式的标准“迭代优势边界”算法，并且具有许多加速（非常常见）简并情况的优化。mem2reg优化传递是处理可变变量的答案，我们强烈建议您依赖它。


关于SSA和PHI相关知识点，可以参考参考文献[1][2][3][4]



## 条件控制语句的IR builder设计


## ir builder的例子
假如给定一段函数定义如下，
```python
    def test(x:i32,y:i32):i32
    	ret = 0
        if x > 1
        	ret = x+y
        elif y >2
        	ret = x-y
        else
        	ret = x*y
       	return ret
    end
```

我们采用策略2（这也是官方推荐的策略）来进行条件控制语句的IR builder。
### 第一步：
生成if else 语句块的IR代码
```python
; ModuleID = 'custom_fn'
source_filename = "custom_fn"

define i32 @test(i32, i32) {
entry:
  %2 = alloca i32
  store i32 0, i32* %2
  br label %if_else_start

if_else_start:                                    ; preds = %entry
  %3 = icmp sgt i32 %0, 1
  br i1 %3, label %cond_true, label %cond_false

cond_true:                                        ; preds = %if_else_start
  %expr_add = add i32 %0, %1
  store i32 %expr_add, i32* %2
  br label %if_else_end

cond_false:                                       ; preds = %if_else_start
  %4 = icmp sgt i32 %1, 2
  br i1 %4, label %cond_true1, label %cond_false2

cond_true1:                                       ; preds = %cond_false
  %5 = sub i32 %0, %1
  store i32 %5, i32* %2
  br label %if_else_end

cond_false2:                                      ; preds = %cond_false
  %6 = mul i32 %0, %1
  store i32 %6, i32* %2
  br label %if_else_end

if_else_end:                                      ; preds = %cond_false2, %cond_true1, %cond_true
  %7 = load i32, i32* %2
  ret i32 %7
}
```
### 第二步：
应用LLVM的mem2reg优化：
```c++
createPromoteMemoryToRegisterPass()
```

```python
; ModuleID = 'custom_fn'
source_filename = "custom_fn"

define i32 @test(i32, i32) {
entry:
  br label %if_else_start

if_else_start:                                    ; preds = %entry
  %2 = icmp sgt i32 %0, 1
  br i1 %2, label %cond_true, label %cond_false

cond_true:                                        ; preds = %if_else_start
  %expr_add = add i32 %0, %1
  br label %if_else_end

cond_false:                                       ; preds = %if_else_start
  %3 = icmp sgt i32 %1, 2
  br i1 %3, label %cond_true1, label %cond_false2

cond_true1:                                       ; preds = %cond_false
  %4 = sub i32 %0, %1
  br label %if_else_end

cond_false2:                                      ; preds = %cond_false
  %5 = mul i32 %0, %1
  br label %if_else_end

if_else_end:                                      ; preds = %cond_false2, %cond_true1, %cond_true
  %.0 = phi i32 [ %expr_add, %cond_true ], [ %4, %cond_true1 ], [ %5, %cond_false2 ]
  ret i32 %.0
}
```

从这个例子可以看出，mem2reg优化后，从内存堆栈操作变为更为高效的虚拟计算器+Phi函数结合的方式来处理。优化后，指令从18条减少到13条。



### 参考文献

[1] LLVM AND SSA, by Steve Zdancewic at Penn. http://flint.cs.yale.edu/cs421/lectureNotes/Spring15/llvm.pdf
[2] Static single assignment form, From Wikipedia. https://en.wikipedia.org/wiki/Static_single_assignment_form
[3] Kaleidoscope: Extending the Language: Control Flow. http://llvm.org/docs/tutorial/MyFirstLanguageFrontend/LangImpl05.html
[4] LLVM SSA 介绍, by Enorsee. https://blog.csdn.net/qq_29674357/article/details/78731713