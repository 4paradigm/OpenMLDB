# OpenMLDB Build-In Function Develop Guide

## Backgroud

OpenMLDB contains hundreds of build-in functions helping data scientist to exetract features and analyze data. For example, now we have aggregation functions like `SUM`, `AVG`, `MAX`, `MIN`, `COUNT`, etc,  to aggregate data over a table or over a specific window. In addition to that, we also have scalar functions like `MINUTE`, `HOUR`, `SECOND`, `SIN`, `COS`, `LOG`, etc, to extract features based on one-row data. 

The article is a hands-on guide to developing built-in functions in OpenMLDB. We welcome developers to join our community and help us extend our functions.

## Functions

OpenMLDB classifies functions as aggregate, scalar, depending on the input data values and result values.

- An *aggregate function* receives a set of values for each argument (such as the values of a column) and returns a single-value result for the set of input values. 
- A *scalar function* receives a single value for each argument and returns a single value result. 

This article is focusing on the built-in scalar functions development. We will not dive into aggregate function development here.

## Register Built-In Function

OpenMLDB provides `ExternalFuncRegistryHelper` to help developers registering built-in functions into the *default library*. After registering a function, users can access and call the function in SQL queries.  In this section, we are going to introduce the basic steps to registering built-in functions into the OpenMLDB default library.

### ExternalFuncRegistryHelper API

`RegisterExternal` can be used to register a built-in function.

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
- `returns<return_type>`: configure return type:
- `return_by_arg()`  : configure whether return value will be store in parameters or not.
  - When **return_by_arg(false)** , result will be return directly. OpenMLDB configure  `return_by_arg(false) ` by default.
  - When **return_by_arg(true)**, the result will be stored and returned by parameters.
    - if the return type is ***non-nullable***, the result will be stored and returned via the last parameter.
    - if the return type is **nullable**, the ***result value*** will be stored in the second-to-last parameter and the ***null flag*** will be stored in the last parameter. if ***null flag*** is true, function result is **null**, otherwise, function result is obtained from second-to-last parameter.
- `doc()`: documenting the function

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

Now, the `v1:month` has been registered into the default library with the name `month`. As a result, we can call `month` in SQL query:

```SQL
select month(timestamp(1590115420000)) as m1,  month(1590115420000) as m2;
 ---- ---- 
  m1   m2  
 ---- ---- 
  5    5   
 ---- ---- 
```



### Register built-in function returns a result in argument

If the registered function output a structural type result, like `timestamp`, `date`, `StringRef`, it should be implemented in a way that returns the result by argument. In addition, we should configure `return_by_arg` as `true`. If the result is ***nullable***, we have to reserve another argument for the null flag.

#### step 1: implement built-in functions to be registered

We implement a function `timestamp_to_date`to get the month part for a given `timestamp`. The input is `timestamp` and the output is nullable `date` which is returned by arguments `codec::Date *output` and `bool *is_null`. One store the output date value and the other one stores the null flag.

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

`ExternalFuncRegistryHelper` provides API `doc(doc_string)`  to document function. Documenting function is describing its use and functionality to the users. While it may be helpful in the development process, the main intended audience is the users.  So we expect the docstring to be **clear** and **legible**. 

Function docstrings should contain the following information:

- **@brief** command to add a summary of the function's purpose and behaviour. 
- **@param** command to document the parameters.
- **Examples** of the function's usage from SQL queries. Demo SQL should be placed in a `@code/@endcode` block
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