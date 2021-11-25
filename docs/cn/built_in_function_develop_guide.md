# OpenMLDB SQL 内置函数开发指南

## 背景

​	OpenMLDB内置了上百个SQL函数，以供数据科学家作数据分析和特征抽取。目前，我们提供聚合类函数(Aggregate Function)如 `SUM`, `AVG`, `MAX`, `MIN`, `COUNT`来支持全表聚合和窗口聚合。与此同时，我们还提供了单行函数(Scalar Function)如`ABS`, `SIN`, `COS`, `DATE`, `YEAR`等提供单行的数据特征抽取。

​	本文是SQL内置函数的开发入门指南，旨在指引开发者快速掌握基础的内置函数开发方法。我们诚挚欢迎更多的开发者加入社区，帮助我们扩展和开发内置函数集。

## Functions

OpenMLDB将函数分类两大类：聚合类函数和单行处理函数:

- 聚合函数((Aggregate Function)对数据集（如一列数据）执行计算，并返回单个值。
- 单行函数（Scalar Function)对若干数据执行计算，并返回单个值。根据函数功能，又可分为
  - 数学函数
  - 逻辑函数
  - 日期函数
  - 字符串函数

## 开发和注册内置函数

OpenMLDB provides `ExternalFuncRegistryHelper` to help developers registering built-in functions into the *default library*. After registering a function, users can access and call the function in SQL queries.  In this section, we are going to introduce the basic steps to registering built-in functions into OpenMLDB default library.

### RegisterExternal API

`RegisterExternal` can be used to registered a built-in function.

```c++
RegisterExternal(function_name)
  .args<arg_type, ...>(built_in_fn_pointer)
  .return_by_arg(bool_value)
  .returns<return_type>
  .doc(documentation)
```

- `RegisterExternal(function_name)`: create an instance of `ExternalFuncRegistryHelper` with specific register name. SQL can 
- `built_in_fn_pointer`: built-in function pointer
- `args<arg_type,...>`: configure argument types
-  `returns<return_type>`: configure return type:
-  `return_by_arg()`  : configure whether return value will be store in parameters or not.
  - When **return_by_arg(false)** , result will be return directly. OpenMLDB configure  `return_by_arg(false) ` by default.
  - When **return_by_arg(true)** , the result will be stored and returned by parameters.
    - if return type is ***non-nullable***, the result will be stored and returned via the last parameter.
    - if return type is **nullable**, the ***result value*** will be stored in the second-to-last parameter and the ***null flag*** will be stored in the last parameter.
- `doc()`: configure the documentation of the function by following template

### Register built-in function returns the result

`ExternalFuncRegistryHelper` provides api `return_by_arg` to configure if the result can be return by parameter or not. Normally, functions returned one numerical type (smallint, int, bigint, float, double) or bool type should be registered with `return_by_arg(false)`. Actually, `ExternalFuncRegistryHelper` configure `return_by_arg` as `false` by default.

#### step 1: implement built-in functions to be registered

We implement two c++ functions to get the month part for a given `timestamp` or `int64_t`. (Developer can implement function in `src/udf/udf.h` and  `src/udf/udf.cc`)

```c++
namespace v1 {
  int32_t month(int64_t ts) {
      time_t time = (ts + TZ_OFFSET) / 1000;
      struct tm t;
      gmtime_r(&time, &t);
      return t.tm_mon + 1;
  }
  int32_t month(codec::Timestamp *ts) { return month(ts->ts_); }
} // namespace v1
```

#### step 2: register built-in function into default library

We register `int32_t month(int64_t ts)` and  `int32_t month(codec::Timestamp *ts)` into default library with registered name `month`

```c++
RegisterExternal("month")
        .args<int64_t>(static_cast<int32_t (*)(int64_t)>(v1::month))
        .args<Timestamp>(static_cast<int32_t (*)(Timestamp*)>(v1::month))
        .doc(R"(
            @brief Return the month part of a timestamp or date

            Example:
            @code{.sql}
                select month(timestamp(1590115420000));
                -- output 5
            @endcode
            @since 0.1.0
        )");
```

Now, the `v1:month` has been registered into default library with name `month`. As a result, we are able to call `month` in SQL query:

```SQL
select month(timestamp(1590115420000)) as m1,  month(1590115420000) as m2;
 ---- ---- 
  m1   m2  
 ---- ---- 
  5    5   
 ---- ---- 
```



### Register built-in function returns a result in argument

If the registered function output a structural type result, like `timestamp`, `date`, `StringRef`, it should be implemented in a way that return the result by argument. In addition, we should configure `return_by_arg` as `true`. If the result is ***nullable***, we have to reserve another argument for the null flag.

#### step 1: implement built-in functions to be registered

We implement a function `timestamp_to_date`to get the month part for a given `timestamp`. The input is `timestamp` and the output is nullable `date` which is returned by argurements `codec::Date *output` and `bool *is_null`. One stores the output date value and the other one stores null flag.

```c++
namespace v1 {
  void timestamp_to_date(codec::Timestamp *timestamp,
       									 codec::Date *output /*result output*/, bool *is_null /*null flag*/) {
    time_t time = (timestamp->ts_ + TZ_OFFSET) / 1000;
    struct tm t;
    if (nullptr == gmtime_r(&time, &t)) {
        *is_null = true;
        return;
    }
    *output = codec::Date(t.tm_year + 1900, t.tm_mon + 1, t.tm_mday);
    *is_null = false;
    return;
	}
} // namespace v1
```

#### step 2: register built-in function into default library

The followed example registered built-in function ` v1::timestamp_to_date` into the default library with name `"date"`. 

Given the result is a nullable date type, we configure  **return_by_arg** as ***true*** and return type as `Nullable<Date>`

```c++
RegisterExternal("date")
        .args<codec::Timestamp>(reinterpret_cast<void*>(
            static_cast<void (*)(Timestamp*, Date*, bool*)>(
                v1::timestamp_to_date)))
        .return_by_arg(true)
        .returns<Nullable<Date>>()
        .doc(R"(
            @brief Cast timestamp or string expression to date

            Example:

            @code{.sql}
                select date(timestamp(1590115420000));
                -- output 2020-05-22
                select date("2020-05-22");
                -- output 2020-05-22
            @endcode
            @since 0.1.0)");
```

## Function Documentation

`ExternalFuncRegistryHelper` provides api `doc(doc_string)`  to document function. Documenting function is describing its use and functionality to the users. While it may be helpful in the development process, the main intended audience is the users.  So we expect the docstring to be **clear** and **legible**. 

Function docstrings should contain the following information:

- **@brief** command to add a brief summary of the function's purpose and behavior. 
- **@param** command to document the parameters.
- **Examples** of the function's usage from SQL queries. Demo sql should be placed in a `@code/@endcode` block
- **@since** command to specify the production version when the function was added to OpenMLDB 

**Example:**

```c++
RegisterExternal("my_function")
  			.args<int64_t>(static_cast<int32_t (*)(int64_t)>(v1::my_func))
        .doc(R"(
            @brief a brief summary of the my_function's purpose and behavior
            
            @param param1 a brief description of param1
            
            Example:

            @code{.sql}
                select my_function(1);
                -- output xxx
            @endcode
            @since 0.4.0)");
```



## Function Unit Test

Once registered/developed a function, the developer should add some related unit tests to make sure everything is going well.

### Add Unit Test to `UdfIRBuilderTest`

We provide  `CheckUdf` in `src/codegen/udf_ir_builder_test.cc` so that the developer can perform function checking easily.

```c++
CheckUdf<return_type, arg_type,...>("function_name", expect_result, arg_value,...);
```

For each function signature, we at least have to:

- Add a unit test with a normal result
- Add a unit test with a null result if the result is **nullable**

**Example**:

```c++
// month(timestamp) normal check
TEST_F(UdfIRBuilderTest, month_timestamp_udf_test) {
    Timestamp time(1589958000000L);
    CheckUdf<int32_t, Timestamp>("month", 5, time);
}
// date(timestamp) normal check
TEST_F(UdfIRBuilderTest, timestamp_to_date_test_0) {
    CheckUdf<Nullable<Date>, Nullable<Timestamp>>(
        "date", codec::Date(2020, 05, 20), codec::Timestamp(1589958000000L));
}
// date(timestamp) null check
TEST_F(UdfIRBuilderTest, timestamp_to_date_test_null_0) {
    CheckUdf<Nullable<Date>, Nullable<Timestamp>>("date", nullptr, nullptr);
}
```

