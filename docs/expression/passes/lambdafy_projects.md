## 表达式 Lambdafy

- 输入	前端语法表达式树
- 输出	Lambda表达式
- 是否必须	是
- 前序pass	-
- 后序pass	所有表达式优化pass
      
### 概述
- 名词：语法表达式树：前端parser解析sql表达式字符串生成的 ExprNode数据结构

- 为了在多种可能的计划节点环境下，内部表达式具有统一的语义和类型推断能力，该pass将语法形式的表达式树转换成lambda形式。
  ```
  e1, e2, e3,... en  ->  lambda row, window => [e1', e2', ... en']
  ```
  - 普通表达式和udf转化为代入row的形式，在内部解析中被视为 row => V
  - udaf转化为代入window的形式，在内部解析中被视为  window => V
  
  例子：`select sum(1 + hour(x)) from t1`
   ```
    "1"	 ->  Const(1)  ->  row => Const(1)
    
    "x"	 ->  Column("x")  ->  row => GetField(row, "x")
    
    "hour(x)"  ->  Call("hour", args=[Column("x")])  ->  row => Call("hour", args=[GetField(row, "x"])
    
    "1 + hour(x)"  ->  Add(Const(1), Call("hour", args=[Column("x")]))  ->  row => Add(Const(1),  Call("hour", args=[GetField(row, "x"]))
    
    "sum(1 + hour(x))"  ->  Call("sum", args=[Add(
                                    Const(1), Call("hour", args=[Column("x")]))])
                               
    update函数: (accum, row) => Add(accum, inner_lambda(row))
         => Add(accum, Add(Const(1),  Call("hour", args=[GetField(row, "x"])))                    
     ```

### 转化逻辑
- Lambda节点: 每个语法表达式的变形目标
    ```c++
    class LambdaNode : public FnDefNode {
     private:
        std::vector<node::ExprIdNode*> arguments_;  // lambda函数参数
        node::ExprNode* body_;  // 表达式函数体
    };
    ```

    和udf fn node不同的是，udf fn node是过程化的描述结构，而LambdaNode的body是一个表达式

- 表达式变形规则
    - global row: 全局ExprID row，表达当前输入行

    - global window: 全局ExprID表达式 w，表达当前窗口数据

    - 规则 `transform_child`
   
        - 如果child是udaf调用： `transform_child(c) = row => transform(c)(w)`
        - 否则： `transform_child(c) = row => transform(c)(`row)`
          
    - 规则 `transform`
        
        - 对于非聚合udaf的普通表达式（含udf call）
        ```
        transform(root(c1, c2, ..., cn)) = 
            row => root.replace(c1 → transform_child(c1)(row),
                                c2 → transform_child(c2)(row),
                                ...,
                                cn → transform_child(cn)(row))`
        ```

        - 对于UDAF, 需要通过函数名和参数个数识别udaf
        ```
        transform(Call(funcname, c1, c2, ..., cn)) =
           rows => udaf_transform(funcname, 
                                  transform_child(c1),
                                  transform_child(c2),
                                  ...,
                                  transform_child(cn)) 
        ```
    
    - Lambda合一规则
    
        在变形过程中，可以频繁地应用到该优化规则简化表达式结构
        
        `unify((x => body(x))(y))  =  body(y)`
      
    整体变换过程即自底向上对语法表达式反复执行上述规则，直到根表达式；然后最终lambda的body通过将全局row和window参数代入transform(root)结果
  
### 物理计划CodeGen
根据根表达式是否udaf，在生成物理计划时候可以确定是agg还是普通的project节点。每个需要codegen的物理计划节点输入现在必须是一组经过类型推断和udf解析后的Lambda节点
            
- 单行project节点：对每个lambda，绑定参数为当前行指针的llvm值，按原先ExprIRBuilder执行逻辑对函数体表达式codegen

- window agg节点：对每个lambda，绑定参数为当前窗口指针的llvm值，按原先udaf call的执行逻辑对函数体表达式codegen（一定是call udaf）

- 全表agg节点：lambda化后，udaf的update函数是(状态, 行) => 新状态的函数，更容易进行增量codegen
             

### UDAF解析逻辑
- 名词: 原型udaf，即当前方式定义的udaf

    和之前SimpleUDAF只支持单输入不同，现在拓展原型udaf的update函数支持多个输入，即
    
    `(State, T1, T2, ... Tk) => State`
    
    现在，udaf_call(funcname, args) 结构需要特化的解析机制，不再作为普通的udf call处理。解析结果是一个函数输入类型为 List of Row 的udaf def节点
             
    作为对比，原型定义的udaf的输入类型是List[V]，udaf解析现在需要将原型的udaf List[V] → R 转化为 List[Row] → R 
    原型udaf的init, merge和output都和行无关，无需改动，只有update函数需要变换
             
- 规则 `udaf_transform`
    和udf相同，要根据函数名找到对应的registry; 和udf不同，现在transform的输入不再是ExprNode的列表了，而是LambdaNode的列表f1, f2, ... fn，每个lambda都是Row => SomeV，并且已经完成递归的类型推断

    - registry使用参数lambda的返回值类型进行类型匹配，找到匹配到的类型确定的udaf原型实现

    - 从原型update函数生成最终的update函数定义
       ```
       transform_update(origin_update, f1, f2, ..., fn) =
           (state, row) => origin_update(state, f1(row), f2(row), ... fk(row))
       ```
    - 使用原型init, merge, output函数定义和更新的update函数创建最终的UDAFDefNode

    - 例子: `sum_where(x: float, y: int32 > 3)`
    ```
    "x":  普通表达式变换  f1 = row => GetField(row, "x")
    "y":  普通表达式变换  f2 = row => GetField(row, "y")
    "3":   普通表达式变换  f3 = row => Const(3)
    "y > 3":  普通表达式变换  f4 = row => GT(GetField(row, "y"), Const(3))
    
    "sum_where"
    (1) #参数=2   →  is udaf
    (2) 获取sum_where的udaf registry
    (3) 参数类型匹配： <float, bool>  → 原型udaf (
        init=Const(0), output=identity, 
        update=(state, a, b) => b ? state + a : state
      ) 
    (4) 变换update函数：
        （state, row) 
            => ((st_, a, b) => b ? (st_ + a) : st_))  (state, f1(row), f4(row))
            => f4(row) ? state : state + f1(row) 
            => GT(GetField(row, "y"), Const(3)) ? state : state + GetField(row, "x")
    ```
