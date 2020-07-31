

接下来，我们介绍一下系统中用到 **表达式函数注册器**

ExprUDF注册器都关注两个事情：

1. Register: signature string --> ExprUDFGenBase

2. ResolveFunction： UDFResolveContext -->  ExprUDFGenBase

   

### ExprUDFRegistry：表达式函数注册器

```C++
class ExprUDFRegistry : public UDFRegistry {
 public:
  	// ...
  	Status ResolveFunction(UDFResolveContext* ctx,
                           node::FnDefNode** result) override;

    Status Register(const std::vector<std::string>& args,
                    std::shared_ptr<ExprUDFGenBase> gen_impl_func);
 private:
    ArgSignatureTable<std::shared_ptr<ExprUDFGenBase>> reg_table_;
    bool allow_window_;
    bool allow_project_;
};
```

关联函数签名 ---> ExprUDFGen



#### ExprUDFGen相关

目前系统维护两种表达式UDFGen：普通表达式UDF Gen`ExprUDFGen` 和 变参表达式UDF GEN `VariadicExprUDFGen`

```C++
struct ExprUDFGenBase {
    virtual ExprNode* gen(UDFResolveContext* ctx,
                          const std::vector<ExprNode*>& args) = 0;
};

// normal
template <typename... LiteralArgTypes>
struct ExprUDFGen : public ExprUDFGenBase {
  using FType = std::function<ExprNode*(
        UDFResolveContext*,
        typename std::pair<LiteralArgTypes, ExprNode*>::second_type...)>;
};

// variadic 
template <typename... LiteralArgTypes>
struct VariadicExprUDFGen : public ExprUDFGenBase {
};

```



这些ExprUDFGen的主要目的就是把根据`UDFResolveContext`和参数列表构造出一个`ExprNode`。

此外，ExprUDFGen还有一个小trick，就行进行了调用参数列表的转换:

```C++
ExprUDFGen<arg1, arg2, arg3> udf_gen;

-> Invoke
udf_gen.gen(UDFResolveContext* ctx,
                  const std::vector<ExprNode*>& args);
-> 转化为
-> Invoke: //  (sizeof(args) == 3)
	gen_func(ctx, args[0], args[1], args[2])
   
```

 

类似，`VariadicExprUDFGen`的主要目的就是:

```C++
VariadicExprUDFGen<arg1, arg2, arg3 > udf_gen;

-> Invoke
udf_gen.gen(UDFResolveContext* ctx,
                  const std::vector<ExprNode*>& args);
-> 转为为
-> Invoke: //  (sizeof(args) >= 3)
	gen_func(ctx, args[0], args[1], args[2], ...)
   
```

 

具体的使用场景其实就是：`ExprUDFRegistry::ResolveFunction`

```C++

Status ExprUDFRegistry::ResolveFunction(UDFResolveContext* ctx,
                                        node::FnDefNode** result) {
    // ... 
  	// 组装函数参数列表：表达式节点组成的vector
    std::vector<node::ExprNode*> func_params_to_gen;
    for (size_t i = 0; i < ctx->arg_size(); ++i) {
        std::string arg_name = "arg_" + std::to_string(i);
        auto arg_type = ctx->arg(i)->GetOutputType();

        auto arg_expr =
            nm->MakeExprIdNode(arg_name, node::ExprIdNode::GetNewId());
        func_params.emplace_back(arg_expr);
        func_params_to_gen.emplace_back(arg_expr);
        arg_expr->SetOutputType(arg_type);
    }

 		// gen_ptr->gen(ctx, func_params_to_gen); --->
  	// -> gen_fn(ctx, func_params_to_gen[0], 
  	// func_params_to_gen[1], ... func_params_to_gen[N])
  	// 这个gen_fn 一般是用户使用ExprUDFRegistryHelper.args(...)的时候定义的
    auto ret_expr = gen_ptr->gen(ctx, func_params_to_gen);
    CHECK_TRUE(ret_expr != nullptr && !ctx->HasError(),
               "Fail to create expr udf: ", ctx->GetError());

    vm::SchemaSourceList empty;
    vm::SchemasContext empty_schema(empty);
    vm::ResolveFnAndAttrs resolver(false, &empty_schema, nm, ctx->library());
    node::ExprNode* new_ret_expr = nullptr;
    CHECK_STATUS(resolver.Visit(ret_expr, &new_ret_expr));

    *result = nm->MakeLambdaNode(func_params, new_ret_expr);
    return Status::OK();
}

```





### ExprUDFRegistryHelper：表达式注册助手

#### 构建参数列表: `args`

```C++
template <typename... LiteralArgTypes> 
ExprUDFRegistryHelper& args(
        const typename ExprUDFGen<LiteralArgTypes...>::FType& func) {
        auto gen_ptr = std::make_shared<ExprUDFGen<LiteralArgTypes...>>(func);
        registry()->Register({DataTypeTrait<LiteralArgTypes>::to_string()...},
                             gen_ptr);
        return *this;
}

```

这里面args最重要的就是定义`ExprUDFGen`



看一个简单的例子：

```c++
library.RegisterExprUDF("add").args<AnyArg, AnyArg>(
        [](UDFResolveContext* ctx, ExprNode* x, ExprNode* y) {
            auto res =
                ctx->node_manager()->MakeBinaryExprNode(x, y, node::kFnOpAdd);
            res->SetOutputType(x->GetOutputType());
            return res;
        });
```

首先：匿名函数实现了`ctx + ExprNode*: x + ExprNode*: y ==> ExprNode*` 的构造逻辑. 为了方便描述，我们假设这个函数名字叫`add_expr_udf_gen_fn`

接着：因为`ExprUDFGen`类的作用，实际上实现了：

```c++
gen(ctx, std::vector<ExprNode*>(x,y)) 
  ---> add_expr_udf_gen_fn(ctx, x, y) 
  ---> res = {
  res = ctx->node_manager()->MakeBinaryExprNode(x, y, node::kFnOpAdd);
  res->SetOutputType(x->GetOutputType());
  return res;
}
```

### ExprUDFTemplateRegistryHelper: 

它可以支持一类的ExprUDFRegistry。

