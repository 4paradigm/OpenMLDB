

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

一般关联C Native函数

### ExprUDFRegistryHelper：表达式注册助手

> 