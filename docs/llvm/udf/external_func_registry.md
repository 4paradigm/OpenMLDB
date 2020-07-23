

接下来，我们介绍一下系统中用到外部函数注册器（一般用以注册C Native函数）

Externa函数注册器只关注两个事情：

1. Register: signature string --> FnDefNode
2. ResolveFunction： UDFResolveContext -->  FnDefNode
3. 

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

### ExternalFuncRegistryHelper：External 函数注册助手

> 我们已经清楚UDFRegistry最重要的一件事情就是映射函数签名和函数定义节点`FnDefNode`。因此，设计XXXXFuncRegistryHelper的目的就是用一切API来辅助构建函数定义节点`FnDefNode`

ExternalFuncRegistryHelper的重要API:

`allow_project` : 

`allow_window` : 允许函数over窗口

`returns` ： 配置函数返回值

`args` ： 配置函数参数列表、函数名、函数关联指针（externalRegistry特有的）

`variadic_args` : 配置可变参数的参数列表、函数名、函数关联指针（externalRegistry特有的）

```C++
class ExternalFuncRegistryHelper
    : public UDFRegistryHelper<ExternalFuncRegistry> {
 public:
  	// ...
    ExternalFuncRegistryHelper& allow_project(bool flag) {
    }

    ExternalFuncRegistryHelper& allow_window(bool flag) {
    }

    template <typename RetType>
    ExternalFuncRegistryHelper& returns() {
    }

    template <typename... LiteralArgTypes>
    ExternalFuncRegistryHelper& args(const std::string& name,
                                     const TypeAnnotatedFuncPtr& fn_ptr) {
    }

    template <typename... LiteralArgTypes>
    ExternalFuncRegistryHelper& args(const TypeAnnotatedFuncPtr& fn_ptr) {
    }

    template <typename... LiteralArgTypes>
    ExternalFuncRegistryHelper& args(const std::string& name, void* fn_ptr) {
    }

    template <typename... LiteralArgTypes>
    ExternalFuncRegistryHelper& args(void* fn_ptr) {
    }

    template <typename... LiteralArgTypes>
    ExternalFuncRegistryHelper& variadic_args(const std::string& name,
                                              void* fn_ptr) {
        if (cur_def_ != nullptr && cur_def_->ret_type() == nullptr) {
    }

    ExternalFuncRegistryHelper& return_by_arg(bool flag) {
    }

    node::ExternalFnDefNode* cur_def() const { return cur_def_; }

 private:
    node::ExternalFnDefNode* cur_def_;
};
```



`args` 的API的用法比较灵活，我们展开详述一下不同场景下使用`args`构建`FnDefNode`的方法。

一般来说，可以通过一下几种配置项组合构造函数定义种的参数列表信息：

- 函数名

- 函数指针

- 函数指针注解信息： 函数指针注解信息``TypeAnnotatedFuncPtr``

  - ```c++
    struct TypeAnnotatedFuncPtr {
      	// ... 先省略
        void* ptr; // 函数指针
        bool return_by_arg;	// 返回值是否作为最后一个参数（一般返回结构体的函数通常要配置）
        GetTypeF get_type_func; // 函数信息填充函数：填充函数返回类型和参数类型
    };
    
    ```

  - ```c++
    // 一个填充函数，函数内部逻辑负责填充第二和第三个参数指针的内容（其实就是修改返回值类型和参数类型列表
    using GetTypeF = typename std::function<void(
            node::NodeManager*, node::TypeNode**, std::vector<node::TypeNode*>*)>;
    ```

  - 构造函数指针类型注解有很多方式：

    - 第一种：通过`Ret (*fn)(Args...)`函数指针类型信息来提取**返回值类型** 和 **参数类型** 以及**函数指针**

      - ```c++
        template <typename Ret, typename... Args>
            TypeAnnotatedFuncPtr(Ret (*fn)(Args...))  // NOLINT
                : ptr(reinterpret_cast<void*>(fn)),
                  return_by_arg(false),
                  get_type_func([](node::NodeManager* nm, node::TypeNode** ret,
                                   std::vector<node::TypeNode*>* args) {
                      *ret = // Convert from Ret
                      *args = // Convert from Args
                      return;
                  }) {}
        ```

    - 第二种: 通过`void (*fn)(T1*)`： 这实际上是第一种函数的特化，即针对返回void，而参数是`T1*`的时候: `void (*fn)(T1*)` --> `T1 (*fn)()` 

      - ```c++
        template <typename T1>
            TypeAnnotatedFuncPtr(void (*fn)(T1*))  // NOLINT
                : ptr(reinterpret_cast<void*>(fn)),
                  return_by_arg(true),
                  get_type_func([](node::NodeManager* nm, node::TypeNode** ret,
                                   std::vector<node::TypeNode*>* args) {
                      *ret =
                          DataTypeTrait<typename CCallDataTypeTrait<T1*>::LiteralTag>::
                              to_type_node(nm);
                      *args = {};
                  }) {}
        ```

    - 第三种：也是第一种函数的特化，`void (*fn)(T1, T2*)` --> `T2 (*fn)(T1)`

      - ```c++
          template <typename T1, typename T2>
            TypeAnnotatedFuncPtr(void (*fn)(T1, T2*))  // NOLINT
                : ptr(reinterpret_cast<void*>(fn)),
                  return_by_arg(true),
                  get_type_func([](node::NodeManager* nm, node::TypeNode** ret,
                                   std::vector<node::TypeNode*>* args) {
                      *ret = // Convert From T2
                          
                      *args = // {T1}
             ) {}
        ```

      - e.g： `void next(ListRef*, Timestamp*)--> Timestamp next(ListRef*)   `  

    - 第四种、 第五种：同上

      -  `void (*fn)(T1, T2, T3*)` --> `T3 (*fn)(T1, T2)`

      -  `void (*fn)(T1, T2, T3, T4*)` --> `T4 (*fn)(T1, T2, T3)`

      - ```c++
        template <typename T1, typename T2, typename T3>
            TypeAnnotatedFuncPtr(void (*fn)(T1, T2, T3*)) 
        ```

      - e.g. `void substring(StringRef*, pos, StringRef*) --> StringRef substring(Stringref*, pos) `

      - e.g. `void substring(StringRef*, pos, len, StringRef) --> StringRef substring(StringRef*, pos, len)`



通过组合函数名、函数指针、函数指针类型注解，可以生成参数列表。



场景1：函数名+`TypeAnnotatedFuncPtr`

```c++
template <typename... LiteralArgTypes>
    ExternalFuncRegistryHelper& args(const std::string& name,
                                     const TypeAnnotatedFuncPtr& fn_ptr) {
       //...
  		fn_ptr.get_type_func(node_manager(), &ret_type, &arg_types);
  		args<LiteralArgTypes...>(name, fn_ptr.ptr);
      // TODO :  validate cur_def_.arg_types_ vs arg_types
      cur_def_->SetRetType(ret_type);
    }
```

字符串 `name` -- > 填充函数名

`TypeAnnotatedFuncPtr fn_ptr` 负责填充返回值类型、函数指针 （后续还可以做类型校验）



场景2：`TypeAnnotatedFuncPtr`

```c++
 template <typename... LiteralArgTypes>
    ExternalFuncRegistryHelper& args(const TypeAnnotatedFuncPtr& fn_ptr) 

```

函数指针类型注解`fn_ptr` 负责填充返回值类型、参数类型列表、函数指针



场景3: `name` + `fn_ptr`

```c++
template <typename... LiteralArgTypes>
    ExternalFuncRegistryHelper& args(const std::string& name, void* fn_ptr) {
        //...
    }
```

填充:

函数名: `name`

函数指针: `fn_ptr`

参数列表: `Conver from template <typename... LiteralArgTypes>`



场景4: fn_ptr

函数名=registry + Join(参数类型名)

函数指针: `fn_ptr`

参数列表: `Conver from template <typename... LiteralArgTypes>`