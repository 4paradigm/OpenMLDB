## Native UDF函数调用规范

UDF调用规范（UDF Call Convention）规定了使用native(llvm or C) function实现物理计划的函数调用时的

- native函数signature规范，包括参数和返回值的类型和位置

- 物理计划中抽象函数调用类型到native函数的映射规则

- 物理计划中的元组/空值在native函数中的传入/返回规则

- 维持所有设计跨越函数边界传递值的场景的统一性 (测试/C/llvm/udf)

### 函数签名规范

- 名词【抽象函数类型】：使用`TypeNode`表达参数和返回值类型的逻辑意义上的函数

- 对于抽象函数类型`R f(T1, T2, ..., Tk, variadics...)`, 对应native函数形如

    `【native返回值类型】 f(【定长参数部分】,【Return By Arg部分】,【变长参数部分】)`

    其中，抽象类型可以是（下面用对应C类型标签表示）
    
     - 基础数据类型 bool, int16_t, int32_t, int64_t, float, double
     
     - 结构体类型 Date, Timestamp, StringRef, ListRef\<V>
     
     - 不透明类型 Opaque\<T>
     
     - Row
     
     - 可空类型标签 Nullable\<T>
     
     - 元组类型标签 Tuple\<T1, T2, ...>
     
    在`return_by_arg`模式下，native函数返回类型必须是`void`
     
- 基础数据类型

     - 参数始终使用值类型传递
     
     - 允许直接使用函数ret返回值（也是唯一允许ret的情况）；如果使用`return_by_arg`；则在函数参数的ReturnByArg部分传入返回地址的指针
     
     ```c++
     // (int32_t, double) -> int32_t
     int32_t f(int32_t, double);
       
     // return by arg version
     void f(int32_t, double, int32_t*); 
     ```

- 结构体数据类型

     - 参数始终使用指针类型传递，有利于跨llvm/c边界传递参数
     
     - 返回值只能return by arg
     
     ```c++
     // (StringRef, StringRef) -> StringRef
     void f(StringRef*, StringRef*, StringRef*); 
     ```
     
- 不透明类型 Opaque\<T>
    
     - 无论参数和返回值始终使用 int8_t*, c函数侧可以使用 T* 
     
     ```c++
     // (Opaque<Set<double>>, double) -> Opaque<Set<double>>
     int8_t* f(int8_t*, double);
        
     // () -> Opaque<Set<double>>
     void f(int8_t*);
     ```

- Nullable\<T>

     - 表示抽象函数的参数可能为空值
         - 但目前nullable不是抽象函数类型签名的一部分，即比如udf不能同时定义`f(int)`和`f(nullable int)`
     
     - 作为参数只在定长部分支持; native函数按顺序传入两个参数： T对应native参数类型， bool
     
     - 返回值只能return by arg; 在ReturnByArg部分按顺序传入两个参数：T的指针类型, bool*
     
     ```c++
     // (Nullable<double>, double) -> double
     double f(double, bool, double);
       
     // (Nullable<Date>, Date) -> Date
     void f(Date*, bool, Date*, Date*);
      
     // Nullable<int32_t> -> Nullable<StringRef>
     void f(int32_t, bool, StringRef*, bool*);
     ```
     
- Tuple\<T1, T2...>

     - 表示抽象的元组类型
     
     - 不支持Nullable<Tuple<...>> 和 Tuple<>
     
     - 作为参数只在定长部分支持; 对每个域类型Tk, 递归执行参数映射规则向当前定长参数部分推入native参数；
     
     - 返回值只能return by arg; 对每个域类型Tk, 递归在ReturnByArg部分推入返回值地址
     
     - 无法根据native函数类型反推抽象函数类型
     
     - 元组的codegen实现：基于NativeValue，而不是打包成llvm结构体
     
     ```
     // (Tuple<float, float>, Tuple<Nullable<int32_t>, Tuple<double, double>>) -> 
     //     Tuple<Date, Tuple<StringRef, Nullable<int64_t>>>
     void f(float, float, int32_t, bool, double, double,
            Date*, StringRef*, int64_t*, bool*);
     ```
     
### UDF空值处理规范
- ExprUDF: 行为与表达式系统完全一致
    
- CodeGenUDF: 使用NativeValue定制化null处理逻辑

- 用户UDF脚本：预期行为与表达式系统完全一致 【undone】

- ExternalUDF:
    - 如果注册时定义了nullable，则空值处理由c函数负责
    - 对于未定义为nullable的参数，如果codegen时NativeValue可空，则
        - 返回值null flag = 所有此类NativeValue的null flag的或
        - c函数目前仍然会被调用
     
### 测试框架函数规范
- codegen测试框架要考虑构造测试用例的简便程度，因此在c端没有服从上述规范：
    - 对抽象函数类型`R f(T1, T2, ..., Tk)` 生成的codegen逻辑，测试框架直接封装相同函数类型的函数对象
    
    - 即测试utility始终传值并返回值，方便书写测试用例
    
    - 裸codegen测试
        ```c++
        auto test_func = ModuleFunctionBuilder()
            .args<T1, T2, ..., Tk>()
            .returns<R>
            .library(DefaultUDFLibrary::get())
            .build([](CodeGenContext* ctx) {
                // 具体函数codegen逻辑
        });
        ```
    
    - 衍生表达式codegen测试
        ```c++
        auto test_func = BuildExprFunction<R, T1, T2, ... Tk>([](
                 NodeManager*, ExprNode* e1, ... ExprNode* ek) {
            // 具体表达式构造逻辑
        });
        ```
    
    - 衍生udf codegen测试
        ```c++
        auto test_func = UDFFunctionBuilder("abs")
            .library(DefaultUDFLibrary::get())
            .args<double>
            .returns<double>
            .build();
        ```
        
    - 实现细节:
        - 测试框架内部仍然使用native函数convention（return by arg）生成测试的目标llvm函数
        
        - 函数对象将传入的参数值的地址（如果参数是null则传nullptr）和返回值地址（如果返回nullable，还有bool地址；如果返回tuple，则递归）保存到int8_t**数组中
        
        - 生成proxy llvm函数，接收int8_t**参数，内部解包所有这些地址构造::llvm::Value*，按call convention调用目标llvm函数
           