## 表达式类型

- 输入	前端语法表达式树
- 输出	Lambda表达式
- 是否必须	是
- 后序pass	所有表达式优化pass
      
### 概述

- 自底向上遍历表达式树，推断每个表达式节点的类型和其他属性（eg, nullable）；如果遇到函数调用，则解析FnDefNode

- 遍历成功后：

    - 所有表达式的类型字段非空
    
    - 所有FnDefNode都经过形参和实参校验；lambda和udaf的内部数据结构也完成类型推导

- 表达式解析
    
    - pass调用`ExprNode::InferAttr()`函数完成对单个表达式节点的类型推导

- 函数解析

    - pass提供对FnDefNode的visit函数
    
    ```c++
    Status VisitFnDef(node::FnDefNode* fn,
                      const std::vector<const node::TypeNode*>& arg_types,
                      node::FnDefNode** output);
    
    Status VisitLambda(node::LambdaNode* lambda,
                       const std::vector<const node::TypeNode*>& arg_types,
                       node::LambdaNode** output);

    Status VisitUDFDef(node::UDFDefNode* lambda,
                       const std::vector<const node::TypeNode*>& arg_types,
                       node::UDFDefNode** output);

    Status VisitUDAFDef(node::UDAFDefNode* lambda,
                        const std::vector<const node::TypeNode*>& arg_types,
                        node::UDAFDefNode** output);
    ```