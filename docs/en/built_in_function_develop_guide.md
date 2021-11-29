# OpenMLDB Build-In Function Develop Guide

## Backgroud

OpenMLDB contains hundreds of build-in functions helping data scientist to exetract features and analyze data. For example, now we have aggregation functions like `SUM`, `AVG`, `MAX`, `MIN`, `COUNT`, etc,  to aggregate data over a table or over a specific window. In addition to that, we also have scalar functions like `MINUTE`, `HOUR`, `SECOND`, `SIN`, `COS`, `LOG`, etc, to extract features based on one-row data. 

OpenMLDB classifies functions as aggregate, scalar, depending on the input data values and result values.

- An *aggregate function* receives a set of values for each argument (such as the values of a column) and returns a single-value result for the set of input values. 
- A *scalar function* receives a single value for each argument and returns a single value result. Scalar function can be classified as several groups:
  - Mathematical function
  - Logical function
  - Date & Time function
  - String function
  - Conversion function

The article is a hands-on guide for built-in scalar functions development in OpenMLDB. So we will not dive into aggregate function development here. We welcome developers to join our community and help us extend our functions.

## Develop and Register Built-In Function

In this section, we are going to introduce the basic steps to implement a C++ built-in function and register it to the OpenMLDB default library.

### 1. Develop Built-In C++ Function

Generally, develepers should implement a C++ function for each SQL function. Thus, uses will invoke the C++ function when they call the coresponding function from SQL.

Developers need to **take care of the following** rules when developing a function:

- Function Location

  - Declare function in [hybridse/src/udf/udf.h](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.h)
  - Implemente function in  [hybridse/src/udf/udf.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.cc)

- Function Name

  - SQL function name is case-insensitive.
  - SQL function name and C++ function name isn't necessary be consistent, since SQL function name will be linked to C++ function via registry.

- Data Type 

  - The correspondence between the SQL data type and the C++ data type is shown here:

  - | SQL Type  | C++ Type           |
    | :-------- | :----------------- |
    | BOOL      | `bool`             |
    | SMALLINT  | int16_t            |
    | INT       | `int32_t`          |
    | BIGINT    | `int64_t`          |
    | FLOAT     | `float`            |
    | DOUBLE    | `double`           |
    | STRING    | `codec::StringRef` |
    | TIMESTAMP | `codec::Timestamp` |
    | DATE      | `codec::Date`      |

- Parameters and result

  - SQL function parameters and C++ function parameters have the same position order.

  - SQL function return type:

    - If SQL function return BOOL or Numeric type(e.g., **BOOL**, **SMALLINT**, **INT**, **BIGINT**, **FLOAT**, **DOUBLE**), the C++ function should be designed to return corresponding C++  type（`bool`, `int16_t`, `int32_t`, `int64_t`, `float`, `double`).

      - ```c++
        // SQL: DOUBLE FUNC_DOUBLE(INT)
        double func_return_double(int); 
        ```

    - If SQL function return **STRING**, **TIMESTAMP** or **DATE**, the C++ function result should be returned in parameter with the corresponding C++ pointer type (`codec::StringRef*`, `codec::Timestamp*`, `codec::Date*`).

      - ```c++
        // SQL: STRING FUNC_STR(INT)
        void func_output_str(int32_t, codec::StringRef*); 
        ```

    - If SQL function return type is ***Nullable***, we need one more `bool*`parameter to return a `is_null` flag.

      - ```c++
        // SQL: Nullable<DATE> FUNC_NULLABLE_DATE(BIGINT)
        void func_output_nullable_date(int64_t, codec::Date*, bool*); 
        ```

### 2. Register Function to the DefaultUdfLibrary

#### OpenMLDB Default Library

OpenMLDB  `DefaultUdfLibrary` stores and manages the global built-in SQL functions. Developer need to register a C++ function to the `DefaultUdfLibrary` such that users can access the function from a SQL query.`DefaultUdfLibrary` has been declared at [hybridse/src/udf/default_udf_library.h](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.h) and implemented at [hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc). Developers can register functions in corresponding `DefaultUdfLibrary::InitXXXXUdf()` methods. For instance:

- **Mathematical function** can be registered in `void DefaultUdfLibrary::IniMathUdf()`
- **Logical function** can be registered in `void DefaultUdfLibrary::InitLogicalUdf()`
- **Date & Time function** can be registered in `void DefaultUdfLibrary::InitTimeAndDateUdf()`
- **String function** can be registered in void `DefaultUdfLibrary::InitStringUdf()`
- **Conversion function** can be registered in `void DefaultUdfLibrary::InitTypeUdf()`

### Register and configure function

`DefaultUdfLibrary::RegisterExternal` create an instance of `ExternalFuncRegistryHelper` with a name. The name will be the function's registered name. 

```c++
ExternalFuncRegistryHelper helper = RegisterExternal("register_func");
// ... ignore function configuration details
```

So users can invoke the function with the name ignoring case.

```SQL
SELECT register_func(col1) FROM t1;
```

 `ExternalFuncRegistryHelper`  provides a set of APIs to help developers to configure the functions and register it into the *default library*.

```c++
RegisterExternal(function_name)
  .args<arg_type, ...>(built_in_fn_pointer)
  .return_by_arg(bool_value)
  .returns<return_type>
  .doc(documentation)
```

- `args<arg_type,...>`: configure argument types.
- `built_in_fn_pointer`: built-in function pointer.
- `returns<return_type>`: configure return type. Notice that when function result is Nullable, we should configure ***return type*** as ***returns<Nullable<return_type>>*** explicitly.
- `return_by_arg()`  : configure whether return value will be store in parameters or not.
  - When **return_by_arg(false)** , result will be return directly. OpenMLDB configure  `return_by_arg(false) ` by default.
  - When **return_by_arg(true)**, the result will be stored and returned by parameters.
    - if the return type is ***non-nullable***, the result will be stored and returned via the last parameter.
    - if the return type is **nullable**, the ***result value*** will be stored in the second-to-last parameter and the ***null flag*** will be stored in the last parameter. if ***null flag*** is true, function result is **null**, otherwise, function result is obtained from second-to-last parameter.
- `doc()`: documenting the function.

### Case 1: Register a function which returns BOOL or Numeric type

If SQL function return BOOL or Numeric type(e.g., **BOOL**, **SMALLINT**, **INT**, **BIGINT**, **FLOAT**, **DOUBLE**), the C++ function should be designed to return corresponding C++ type（`bool`, `int16_t`, `int32_t`, `int64_t`, `float`, `double`).

The C++ function can be declared and implemented as followed:

```c++
# hybridse/src/udf/udf.h
namespace udf {
  namespace v1 {
    Ret func(Arg1 arg1, Arg2 arg2, ...);
  }
}

```

```c++
# hybridse/src/udf/udf.cc
namespace udf {
  namespace v1 {
    Ret func(Arg1 arg1, Arg2 arg2, ...) {
      // ...
      return ans; 
    }
  }
}
```

And the C++ function can be registered as followed:

```c++
RegisterExternal("my_func")
        .args<Arg1, Arg2, ...>(static_cast<R (*)(Arg1, Arg2, ...)>(udf::v1::func))
  			.return_by_arg(false)
        .doc(R"(
            documenting my_func
        )");
```

#### Example: Implement and register `INT Month(TIMESTAMP)` function

`INT Month(TIMESTAMP)` function return the month part for a given `timestamp` or `int64_t`. 

**Step 1: declare and implement C++ functions**

```c++
# hybridse/src/udf/udf.h
namespace udf{
  namespace v1 {
    int32_t month(int64_t ts);
    int32_t month(codec::Timestamp *ts);
  } // namespace v1
} // namespace udf
```

```c++
# hybridse/src/udf/udf.cc
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

**Step 2: register C++ function to default library**

We register the `int32_t month(codec::Timestamp *ts)` into default library with a registered name `month`.

```c++
RegisterExternal("month")
        .args<codec::Timestamp>(static_cast<int32_t (*)(codec::Timestamp*)>(udf::v1::month))
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

Now, the `udf::v1:month` has been registered into the default library with the name `month`. As a result, we can call `month` in SQL query.

```SQL
select month(timestamp(1590115420000)) as m1;
 ---- 
  m1     
 ----
  5     
 ----
```

### Case2: Register a function which returns result in parameter

If SQL function return **STRING**, **TIMESTAMP** or **DATE**, the C++ function result should be returned in parameter with the corresponding C++ pointer type (`codec::StringRef*`, `codec::Timestamp*`, `codec::Date*`).

Thus the C++ function can be declared and implemented as followed:

```c++
# hybridse/src/udf/udf.h
namespace udf {
  namespace v1 {
    void func(Arg1 arg1, Arg2 arg2, ..., Ret* result);
  } // namespace v1
} // namespace udf
```

```c++
# hybridse/src/udf/udf.cc
namespace udf {
  namespace v1 {
    void func(Arg1 arg1, Arg2 arg2, ..., Ret* ret) {
      // ...
      // *ret = result value
    }
  } // namespace v1
} // namespace udf
```

And the built-in function should be registered as followed: 

```c++
RegisterExternal("my_func")
        .args<Arg1, Arg2, ...>(static_cast<R (*)(Arg1, Arg2, ...)>(udf::v1::func))
  			.return_by_arg(true)
        .doc(R"(
            documenting my_func
        )");
```

#### Example: Implement and register `STRING String(BOOL)` function

`STRING String(BOOL)` function accepts a BOOL type input and converts it to a STRING type output.

**Step 1: declare and implement C++ functions**

Since the SQL function return **STRING**, the C++ function result should be returned by parameter `codec::StringRef * output`. 

```c++
# hybridse/src/udf/udf.h
namespace udf{
  namespace v1 {
    void bool_to_string(bool v, codec::StringRef *output);
  } // namespace v1
} // namespace udf
```

```c++
# hybridse/src/udf/udf.cc
namespace udf {
  namespace v1 {
      void bool_to_string(bool v, codec::StringRef *output) {
          if (v) {
              char *buffer = AllocManagedStringBuf(4);
              output->size_ = 4;
              memcpy(buffer, "true", output->size_);
              output->data_ = buffer;
          } else {
              char *buffer = AllocManagedStringBuf(5);
              output->size_ = 5;
              memcpy(buffer, "false", output->size_);
              output->data_ = buffer;
          }
      }
  } // namespace v1
} // namespace udf
```

**Step 2: register C++ function into default library**

The followed example registered built-in function ` v1::bool_to_string` into the default library with name `"string"`. 

Given the result is STRING type and should be return by parameter, we have to configure  **return_by_arg** as ***true***.

```c++
RegisterExternal("string")
        .args<bool>(static_cast<void (*)(bool, codec::StringRef*)>(
                        udf::v1::bool_to_string))
        .return_by_arg(true)
        .doc(R"(
            @brief Return string converted from bool expression

            Example:

            @code{.sql}
                select string(true);
                -- output "true"

                select string(false);
                -- output "false"
            @endcode
            @since 0.1.0)");
```

Now, the `udf::v1:bool_to_string` has been registered into the default library with the name `string`. As a result, we can call `string()` in SQL query.

```SQL
select STRING(true) as str_true, string(false) as str_false;
 ----------  ---------- 
  str_true   str_false  
 ----------  ---------- 
   true        true
 ----------  ---------- 
```

### Case3: Register a function which returns a Nullable result in argurement

If the registered function output a structural type result, like `timestamp`, `date`, `StringRef`, it should be implemented in a way that returns the result by argument. In addition, since the result is ***nullable***, we have to reserve another argument `bool*`for the null flag.

Thus the C++ function can be declared and implemented as followed:

```c++
# hybridse/src/udf/udf.h
namespace udf {
  namespace v1 {
    void func(Arg1 arg1, Arg2 arg2, ..., Ret* result, bool* null_flag);
  }
}

```

```c++
# hybridse/src/udf/udf.cc
namespace udf {
  namespace v1 {
    void func(Arg1 arg1, Arg2 arg2, ..., Ret* ret, bool* null_flag) {
      // ...
      // if result value is null
      // 	*null_flag = true
     	// else 
      // 	*ret = result value
      // *null_flag = false
    }
  }
}
```

And the built-in function should be registered as followed: 

```c++
RegisterExternal("my_func")
        .args<Arg1, Arg2, ...>(static_cast<R (*)(Arg1, Arg2, ...)>(udf::v1::func))
  			.return_by_arg(true)
  			.returns<Nullable<Ret>>()
        .doc(R"(
            documenting my_func
        )");
```

#### Example: Implement and register `DATE Date(TIMESTAMP)` function

`DATE Date(TIMESTAMP)()` function converts **TIMESTAMP** type to **DATE** type.

**Step 1: declare and implement built-in functions**

We implement a function `timestamp_to_date`to convert `timestamp`  to date type. The input is `timestamp` and the output is nullable `date` which is returned by arguments `codec::Date *output` and `bool *is_null`. 

```c++
# hybridse/src/udf/udf.h
namespace udf{
  namespace v1 {
    void timestamp_to_date(codec::Timestamp *timestamp,
      		codec::Date *ret /*result output*/, bool *is_null /*null flag*/);
  } // namespace v1
} // namespace udf
```

```c++
# hybridse/src/udf/udf.cc
namespace udf{
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
} // udf
```

**Step 2: register built-in function into default library**

The followed example registered built-in function ` v1::timestamp_to_date` into the default library with name `"date"`. 

Given the result is a nullable date type, we configure  **return_by_arg** as ***true*** and return type as `Nullable<Date>`.

```c++
RegisterExternal("date")
        .args<codec::Timestamp>(reinterpret_cast<void*>(
            static_cast<void (*)(Timestamp*, Date*, bool*)>(
                udf::v1::timestamp_to_date)))
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

Now, the `udf::v1:timestamp_to_date` has been registered into the default library with the name `date`. As a result, we can call `date()` in SQL query.

```SQL
select date(timestamp(1590115420000)) as dt;
 ----------
     dt
 ------------
  2020-05-22 
 ------------
```

###  RegisterAlias

Sometimes, we don't have to implement and register a function when it is alias to another function already exist in the default library. We can simply use api `RegisterAlias` to link current register function name with an existing registered name.

```c++
// substring() is registered into default library already 
RegisterAlias("substr", "substring");
// ceil() is registered into default library already 
RegisterAlias("ceiling", "ceil");
```



## Function Documentation

`ExternalFuncRegistryHelper` provides API `doc(doc_string)`  to document a function. Documenting function is describing its use and functionality to the users. While it may be helpful in the development process, the main intended audience is the users.  So we expect the docstring to be **clear** and **legible**. 

Function docstrings should contain the following information:

- **@brief** command to add a summary of the function's purpose and behaviour. 
- **@param** command to document the parameters.
- **Examples** of the function's usage from SQL queries. Demo SQL should be placed in a `@code/@endcode` block.
- **@since** command to specify the production version when the function was added to OpenMLDB. The version can be obtained from project's [CMakeList.txt](https://github.com/4paradigm/OpenMLDB/blob/main/CMakeLists.txt): ` ${OPENMLDB_VERSION_MAJOR}.${OPENMLDB_VERSION_MINOR}.${OPENMLDB_VERSION_BUG}`

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

### Add Unit Test to `UdfIRBuilderTest`(Required)

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

Compile and unit test:

```bash
cd ./hybridse
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DTESTING_ENABLE=ON
make udf_ir_builder_test -j4
SQL_CASE_BASE_DIR=${OPENMLDB_DIR} ./src/codegen/udf_ir_builder_test
```

### Add Integration Test Case (Optional)

Developers can add integration yaml case in [cases/query/udf_query.yaml](https://github.com/4paradigm/OpenMLDB/blob/main/cases/query/udf_query.yaml) to test newly registered function end-to-end

```yaml
cases:
  - id: 1
    desc: test substring(col, position)
    inputs:
      - name: t1
        columns: col1:int32, std_ts:timestamp, col_str:string
        indexs: ["index2:col1:std_ts"]
        rows: 
          - [1, 1, hello_world]
          - [2, 2, abcdefghig]
    sql: |
      select col1 as id, substring(col_str, 3) as col1 from t1;
    expect:
      columns: ["id:int32", "col1:string"]
      order: id
      rows:
        - [1, "llo_world"]
        - [2, "cdefghig"]
```

Compile and test udf case:

```bash
cd ./hybridse
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DTESTING_ENABLE=ON -DEXAMPLES_ENABLE=ON
make toydb_engine_test -j4
SQL_CASE_BASE_DIR=${OPENMLDB_DIR} ./examples/toydb/src/testing/toydb_engine_test --gtest_filter=EngineUdfQuery*
```



