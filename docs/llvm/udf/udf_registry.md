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





## UDFResolveContext

> ```
> Overall information to resolve a sql function call.
> SQL UDF 函数的整体信息：包括参数列表、窗口（如果有得话），所在函数库等
> ```

```c++
class UDFResolveContext {
  // ...
 private:
    ExprListNode* args_;	// 函数参数列表
    const SQLNode* over_; // 窗口（如果有的话）
    node::NodeManager* manager_;	// NodeManager 辅助创建节点的
    udf::UDFLibrary* library_;	// 函数库

    node::ExprAnalysisContext* analysis_ctx_;
    std::string error_msg_;
};
```



### UDFRegistry Doc String

UDFRegistry提供DOC String接口，允许注册函数的时候，添加函数的使用说明文档。[doxygen的markdown语法](https://www.doxygen.nl/manual/markdown.html)

具体的使用方法

```c++
RegisterExternal("substring")
        .doc(R"(
Return a substring from string `str` starting at position `pos `.

example:
@code{.sql}


    select substr("hello world", 2);
    -- output "llo world"

@endcode

@param **str**
@param **pos** define the begining of the substring.

- If `pos` is positive, the begining of the substring is `pos` charactors from the start of string.
- If `pos` is negative, the beginning of the substring is `pos` characters from the end of the string, rather than the beginning.

@since 2.0.0.0
)")
   .args<StringRef, int32_t>(
            static_cast<void (*)(codec::StringRef*, int32_t,
                                 codec::StringRef*)>(udf::v1::sub_string))
   .return_by_arg(true);
```



Doc string遵循doxygen的格式。通常需要配置: 

1. 函数简单描述（必选）

2. 例子作代码示范（尽量有）

     ``` 
   @code
       这里面放代码
   @endcode
     ```

   

3. 参数@param

4. 返回值@return 如果需要特别说明

5. 函数支持的初始版本@since

6. 其他说明@note

