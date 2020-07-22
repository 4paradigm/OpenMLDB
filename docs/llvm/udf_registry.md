# 通用udf/udaf注册机制

代码路径：

https://gitlab.4pd.io/ai-native-db/fesql/merge_requests/178

# 名词解释：

**UDF/UDAF注册：**在系统函数库中登记UDF/UDAF函数，具体地，以map(函数签名→ 函数体)的形式保存注册信息

**注册器：**提供具体UDF/UDAF注册能力，系统目前有5类函数注册器

**注册表：**系统维护注册信息的结构体

**注册器Helper：**注册器的辅助类, 提高函数的注册效率

# 注册器

## 注册接口：UDFTransformRegistry

https://gitlab.4pd.io/ai-native-db/fesql/blob/develop/src/udf/udf_registry.h

接口UDFTransformRegistry(fname)： 提供未解析的sql udf调用形式 fname(args) 到表达式的过程抽象。所有的函数注册器都继承UDFTransformRegistry。

```C++
/**
 * Interface to implement resolve and transform
 * logic for sql function call with fn name.
 */
class UDFTransformRegistry {
    // transform "f(arg0, arg1, ...argN)" -> some expression
    virtual Status Transform(UDFResolveContext* ctx,
                         node::ExprNode** result) = 0;
    // ...
}   
```



目前，系统中有两类注册器: CompositeRegistry和UDFRegistry。 CompositeRegistry能够批处理注册器。具体的注册还是UDFRegistry类注册器实现。

## UDFRegistry

UDFRegistry接口，它提供ResolveFunction接口，将一个函数信息上下文（UDFResolvedContext）Resolved出一个FnDefNode节点，最终包装输出函数表达式节点。

目前系统大部分的注册器都继承UDFRegistry

```C++
/**
 * Interface to implement resolve logic for sql function
 * call without extra transformation.
 */
class UDFRegistry : public UDFTransformRegistry {
 public:
    explicit UDFRegistry(const std::string& name)
        : UDFTransformRegistry(name) {}
 
    // "f(arg0, arg1, ...argN)" -> resolved f
    virtual Status ResolveFunction(UDFResolveContext* ctx,
                                   node::FnDefNode** result) = 0;
 
    virtual ~UDFRegistry() {}
 
    Status Transform(UDFResolveContext* ctx, node::ExprNode** result) override {
        node::FnDefNode* fn_def = nullptr;
        CHECK_STATUS(ResolveFunction(ctx, &fn_def));
 
        *result =
            ctx->node_manager()->MakeFuncNode(fn_def, ctx->args(), ctx->over());
        return Status::OK();
    }
};
```

**目前有5中注册器：**

1. 内置c函数： ExternalFuncRegistry
2. 使用表达式函数构造udf：ExprUDFRegistry
3. 使用底层llvm逻辑构造udf： LLVMUDFRegistry
4. 从sql拓展udf语法构造的udf： 
5. udaf

以上几类注册器负责系统几乎所有函数的注册和解析。无论哪一种注册器都是在参数签名注册表（ArgSignatureTable）基础上工作的。

## ArgSignatureTable（参数签名注册表）

#### 提供Register接口，将(函数签名,函数节点)进行注册。

#### 提供Find接口，根据参数列表（存放在UDFResolveContext中）来匹配已注册的函数表达式以及函数签名。

#### 系统包含四类参数匹配规则：

1. 参数个数一致，参数类型一致
2. 参数个数一致，除AnyType类型外，参数类型一致
3. 包含变长参数，参数类型确定；固定参数部分，参数个数和类型一致
4. 包含可变参数，包含AnyType参数；固定参数部分，参数个数一致，除AnyType类型外，参数类型一致

以上四类匹配规则的匹配优先级为：1>2>3>4

在函数注册表的基础上，5中函数注册器的工作就简化为：

1. 注册函数到函数签名注册表: 

   ```C++
   reg_table_.Register(args, func);
   ```

2. 从函数签名表（ArgSignatureTable)解析函数: 

   ```C++
   CHECK_STATUS(reg_table_.Find(ctx, &fun_def, &signature, &variadic_pos),
    "Fail to resolve fn name \"", name(), "\"");
   ```

剩下的工作是注册函数时，准备好函数参数列表以及函数节点，这写部分工作交由各类UDFRegistryHelper来完成。XXXXXUDFRegistryHelper用来辅助XXXXXUDFRegistry的注册工作。包括构建参数列表、构建函数题等。



接口UDFLibrary: udf集合抽象，提供方便的注册和解析接口
DefaultUDFLibrary: 用于定义当前使用的udfs





## UDF Registry and Helper

接下来，我们介绍一下系统中用到的5种函数注册器以及相关的注册助手（XXXXUDFRegistryHelper)

### ExternalFuncRegistry：External函数注册器

```C++
/**
 * Interface to resolve udf to external native functions.
 */
class ExternalFuncRegistry : public UDFRegistry {
 public:
    explicit ExternalFuncRegistry(const std::string& name)
        : UDFRegistry(name), allow_window_(true), allow_project_(true) {}

    Status ResolveFunction(UDFResolveContext* ctx,
                           node::FnDefNode** result) override;

    Status Register(const std::vector<std::string>& args,
                    node::ExternalFnDefNode* func);

    void SetAllowWindow(bool flag) { this->allow_window_ = flag; }

    void SetAllowProject(bool flag) { this->allow_project_ = flag; }

    const ArgSignatureTable<node::ExternalFnDefNode*>& GetTable() const {
        return reg_table_;
    }

 private:
    ArgSignatureTable<node::ExternalFnDefNode*> reg_table_;
    bool allow_window_;
    bool allow_project_;
}
```

一般关联C Native函数