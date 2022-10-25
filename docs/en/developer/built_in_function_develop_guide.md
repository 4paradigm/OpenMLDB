# Built-In Function Development

## 1. Background

OpenMLDB contains hundreds of built-in functions that help data scientists extract features and analyze data. For instance, we now have aggregation functions like `SUM`, `AVG`, `MAX`, `MIN`, `COUNT`, etc, to aggregate data over a table or a specific window. In addition to that, we also have scalar functions like `ABS`, `SIN`, `COS`, `DATE`, `YEAR`, etc, to extract features based on one-row data. 

OpenMLDB classifies functions as aggregate or scalar depending on the input data values and result values.

- An *aggregate function* receives **a set of** values for each argument (such as the values of a column) and returns a single-value result for the set of input values. 

- A *scalar function* receives **a single value** for each argument and returns a single value result. A scalar function can be classified into several groups:

- - Mathematical function
  - Logical function
  - Date & Time function
  - String function
  - Conversion function

This article is a hands-on guide for built-in scalar function development in OpenMLDB. We will not dive into aggregate function development in detail. We truly welcome developers who want to join our community and help extend our functions.

## 2. Develop a Built-In SQL Function

In this section, we are going to introduce the basic steps to implement and register an SQL built-in function. Built-in SQL function development involves the following steps:

- Develop a built-in C++ function
- Register and configure the function
- Create function unit tests

### 2.1 Develop a Built-In C++ Function

Generally, developers should implement a C++ function for each SQL function. Thus, users will invoke the C++ function when they call the corresponding function from SQL.

Developers need to **take care of the following** rules when developing a function:

#### 2.1.1 Code Location

Developers can declare function in [hybridse/src/udf/udf.h](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.h) and implement it in  [hybridse/src/udf/udf.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.cc) within namespace `hybridse::udf::v1`.

#### 2.1.2 C++ Function Naming Rules

- Function names are all lowercase, with underscores between words. Check [snake_case](https://en.wikipedia.org/wiki/Snake_case) for more details.
- Function names should be clear and readable. Use names that describe the purpose or intent of the function.

#### 2.1.3 C++ and SQL Data Type

C++ built-in functions can use limited data types, including BOOL, Numeric, String, Timestamp and Date. The correspondence between the SQL data type and the C++ data type is shown as follows:

- | SQL Type  | C++ Type           |
  | :-------- | :----------------- |
  | BOOL      | `bool`             |
  | SMALLINT  | `int16_t`          |
  | INT       | `int32_t`          |
  | BIGINT    | `int64_t`          |
  | FLOAT     | `float`            |
  | DOUBLE    | `double`           |
  | STRING    | `codec::StringRef` |
  | TIMESTAMP | `codec::Timestamp` |
  | DATE      | `codec::Date`      |

#### 2.1.4 Parameters and Result

- SQL function parameters and C++ function parameters have the same position order.

- C++ function parameter types should match the SQL types. Check [2.1.3 C++ and SQL Data Type](#2.1.3-C++ and SQL Data Type) for more details.

- SQL function return type:

  - If SQL function return BOOL or Numeric type (e.g., **BOOL**, **SMALLINT**, **INT**, **BIGINT**, **FLOAT**, **DOUBLE**), the C++ function should be designed to return corresponding C++  type（`bool`, `int16_t`, `int32_t`, `int64_t`, `float`, `double`).

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

  - Notice that return types have greater impact on built-in function developing behaviours. We will cover the details in a later section [3. Built-in Function Development Template](#3.-Built-in Function Development Template).

#### 2.1.5 Memory Management

- Operator `new` operator or method `malloc` are forbidden in C++ built-in function implementation.
- Developers can call `hybridse::udf::v1::AllocManagedStringBuf(size)` to allocate space. OpenMLDB `ByteMemoryPool` will assign continous space to the function and will release it when safe.
- If allocated size < 0, allocation will fail. `AllocManagedStringBuf` return null pointer.
- If allocated size exceed the MAX_ALLOC_SIZE which is 2048, the allocation will fail. `AllocManagedStringBuf` return null pointer.

**Example**:

In this example, we are going to implement `bool_to_string()` which is responsible to casting a bool value to a string (e.g, "true", "false"). So we use `AllocManagedStringBuf` to allocate spaces for the string.

```c++
namespace hybridse {
  namespace udf {
    namespace v1 {
      void bool_to_string(bool v, hybridse::codec::StringRef *output) {
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
    }
  }
}
```



### 2.2 Register And Configure Built-in Function

#### 2.2.1 DefaultUdfLibrary

OpenMLDB  `DefaultUdfLibrary` stores and manages the global built-in SQL functions. Developers need to register a C++ function to the `DefaultUdfLibrary` such that users can access the function from an SQL query. `DefaultUdfLibrary` has been declared at [hybridse/src/udf/default_udf_library.h](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.h) and implemented at [hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc). Developers can register functions in the corresponding `DefaultUdfLibrary::InitXXXXUdf()` methods. For instance:

- **Mathematical function** can be registered in `void DefaultUdfLibrary::InitMathUdf()`
- **Logical function** can be registered in `void DefaultUdfLibrary::InitLogicalUdf()`
- **Date & Time function** can be registered in `void DefaultUdfLibrary::InitTimeAndDateUdf()`
- **String function** can be registered in void `DefaultUdfLibrary::InitStringUdf()`
- **Conversion function** can be registered in `void DefaultUdfLibrary::InitTypeUdf()`

#### 2.2.2 Register Name

- Users access an SQL function by its register name
- Register names are all lowercase, with underscores between words. Check [snake_case](https://en.wikipedia.org/wiki/Snake_case) for more details.
- The SQL function name does not have to be the same as the C++ function name, since the SQL function name will be linked to the C++ function via the registry.
- SQL function names are case-insensitive. For instance, given register name "aaa_bb", the users can access it by calling `AAA_BB()`, `Aaa_Bb()`, `aAa_bb()` in SQL.

#### 2.2.3 Register and Configure Function

`DefaultUdfLibrary::RegisterExternal` create an instance of `ExternalFuncRegistryHelper` with a name. The name will be the function's registered name. 

```c++
ExternalFuncRegistryHelper helper = RegisterExternal("register_func_name");
// ... ignore function configuration details
```

 `ExternalFuncRegistryHelper`  provides a set of APIs to help developers to configure the functions and register it into the *default library*.

```c++
RegisterExternal("register_func_name")
  .args<arg_type, ...>(built_in_fn_pointer)
  .return_by_arg(bool_value)
  .returns<return_type>
  .doc(documentation)
```

- `args<arg_type,...>`: Configure argument types.
- `built_in_fn_pointer`: Built-in function pointer.
- `returns<return_type>`: Configure return type. Notice that when function result is Nullable, we should configure ***return type*** as ***returns<Nullable<return_type>>*** explicitly.
- `return_by_arg()`  : Configure whether return value will be store in parameters or not.
  - When **return_by_arg(false)** , result will be return directly. OpenMLDB configure  `return_by_arg(false) ` by default.
  - When **return_by_arg(true)**, the result will be stored and returned by parameters.
    - if the return type is ***non-nullable***, the result will be stored and returned via the last parameter.
    - if the return type is **nullable**, the ***result value*** will be stored in the second-to-last parameter and the ***null flag*** will be stored in the last parameter. if ***null flag*** is true, function result is **null**, otherwise, function result is obtained from second-to-last parameter.

#### 2.2.4 Documenting Function

`ExternalFuncRegistryHelper` provides API `doc(doc_string)`  to document a function. Documenting function is describing its use and functionality to the users. While it may be helpful in the development process, the main intended audience is the users.  So we expect the docstring to be **clear** and **legible**. 

Function docstrings should contain the following information:

- **@brief** command to add a summary of the function's purpose and behaviour. 
- **@param** command to document the parameters.
- **Examples** of the function's usage from SQL queries. Demo SQL should be placed in a `@code/@endcode` block.
- **@since** command to specify the production version when the function was added to OpenMLDB. The version can be obtained from the project's [CMakeList.txt](https://github.com/4paradigm/OpenMLDB/blob/main/CMakeLists.txt): ` ${OPENMLDB_VERSION_MAJOR}.${OPENMLDB_VERSION_MINOR}.${OPENMLDB_VERSION_BUG}`

```c++
RegisterExternal("register_func_name")
//...
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

#### 2.2.5 RegisterAlias

Sometimes, we don't have to implement and register a function when it is an alias to another function that already exists in the default library. We can simply use api `RegisterAlias("alias_func", "original_func")` to link the current register function name with an existing registered name.

```c++
RegisterExternal("substring")
        .args<StringRef, int32_t, int32_t>(
            static_cast<void (*)(codec::StringRef*, int32_t, int32_t,
                                 codec::StringRef*)>(udf::v1::sub_string))
        .return_by_arg(true)
        .doc(R"(
            @brief Return a substring `len` characters long from string str, starting at position `pos`.
            Alias function: `substr`

            Example:

            @code{.sql}

                select substr("hello world", 3, 6);
                -- output "llo wo"

            @endcode

            @param str
            @param pos: define the begining of the substring.

             - If `pos` is positive, the begining of the substring is `pos` charactors from the start of string.
             - If `pos` is negative, the beginning of the substring is `pos` characters from the end of the string, rather than the beginning.

            @param len length of substring. If len is less than 1, the result is the empty string.

            @since 0.1.0)");

// substring() is registered into default library already 
RegisterAlias("substr", "substring");
```

## 2.3 Function Unit Test

Once a function is registered/developed, the developer should add some related unit tests to make sure everything is going well. 

#### 2.3.1 Add Unit Tests

Generally, developers can add `TEST_F` cases to [src/codegen/udf_ir_builder_test.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/codegen/udf_ir_builder_test.cc).

OpenMLDB provides `CheckUdf` in  [src/codegen/udf_ir_builder_test.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/codegen/udf_ir_builder_test.cc) so that the developer can perform function checking easily.

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

#### 2.3.2 Compile and Test

```bash
cd ./hybridse
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DTESTING_ENABLE=ON
make udf_ir_builder_test -j4
SQL_CASE_BASE_DIR=${OPENMLDB_DIR} ./src/codegen/udf_ir_builder_test
```



## 3. Built-in Function Development Template

We classified built-in function into 3 types based on its return type:

- SQL functions return **BOOL** or Numeric types, e.g., **SMALLINT**, **INT**, **BIGINT**, **FLOAT**, **DOUBLE**

- SQL functions return **STRING**, **TIMESTAMP** or **DATE**

  - ```c++
    // SQL: STRING FUNC_STR(INT)
    void func_output_str(int32_t, codec::StringRef*); 
    ```

- SQL functions return ***Nullable*** type

Return types have a greater impact on the built-in function's behaviour. We will cover the details of the three types of SQL functions in the following sections.

### 3.1 SQL Functions Return **BOOL** or Numeric Types

If an SQL function returns a BOOL or Numeric type (e.g., **BOOL**, **SMALLINT**, **INT**, **BIGINT**, **FLOAT**, **DOUBLE**), then the C++ function should be designed to return the corresponding C++ type（`bool`, `int16_t`, `int32_t`, `int64_t`, `float`, `double`).

The C++ function can be declared and implemented as follows:

```c++
# hybridse/src/udf/udf.h
namespace hybridse {
  namespace udf {
    namespace v1 {
      Ret func(Arg1 arg1, Arg2 arg2, ...);
    }
  }
}
```

```c++
# hybridse/src/udf/udf.cc
namespace hybridse {
  namespace udf {
    namespace v1 {
      Ret func(Arg1 arg1, Arg2 arg2, ...) {
        // ...
        return ans; 
      }
    }
  }
}
```

Configure and register function into `DefaultUdfLibary` in[hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc):

```c++
# hybridse/src/udf/default_udf_library.cc
RegisterExternal("my_func")
        .args<Arg1, Arg2, ...>(static_cast<R (*)(Arg1, Arg2, ...)>(v1::func))
        .doc(R"(
            documenting my_func
        )");
```

### 3.2 SQL Functions Return **STRING**, **TIMESTAMP** or **DATE**

If an SQL function returns **STRING**, **TIMESTAMP** or **DATE**, then the C++ function result should be returned in the parameter with the corresponding C++ pointer type (`codec::StringRef*`, `codec::Timestamp*`, `codec::Date*`).

Thus the C++ function can be declared and implemented as follows:

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

Configure and register the function into `DefaultUdfLibary` in[hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc):

```c++
# hybridse/src/udf/default_udf_library.cc
RegisterExternal("my_func")
        .args<Arg1, Arg2, ...>(static_cast<R (*)(Arg1, Arg2, ..., Ret*)>(v1::func))
  			.return_by_arg(true)
        .doc(R"(
            documenting my_func
        )");
```

### 3.3 SQL Functions Return ***Nullable*** type

If an SQL function return type is ***Nullable***, then we need one more `bool*` parameter to return a `is_null` flag.

Thus the C++ function can be declared and implemented as follows:

```c++
# hybridse/src/udf/udf.h
namespace hybridse {
  namespace udf {
    namespace v1 {
      void func(Arg1 arg1, Arg2 arg2, ..., Ret* result, bool* null_flag);
    }
  }
}

```

```c++
# hybridse/src/udf/udf.cc
namespace hybridse {
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
} 
```

Configure and register the function into `DefaultUdfLibary` in[hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc):

```c++
# hybridse/src/udf/default_udf_library.cc
RegisterExternal("my_func")
        .args<Arg1, Arg2, ...>(static_cast<R (*)(Arg1, Arg2, ...)>(v1::func))
  			.return_by_arg(true)
  			.returns<Nullable<Ret>>()
        .doc(R"(
            documenting my_func
        )");
```

### 3.4 SQL Functions Handle Nullable Argument

Generally, OpenMLDB will return a ***NULL*** for a function when any one of its argurements  is ***NULL***. 

If we wants to handler the null argurement with specific strategy, we have to configure this argurment as an ***Nullable*** argurement and pass another **bool** flag to specify if this argurement is null or not.

The C++ function can be declared and implemented as follows:

```c++
# hybridse/src/udf/udf.h
namespace hybridse {
  namespace udf {
    namespace v1 {
      // we are going to handle null arg2 in the function
      Ret func(Arg1 arg1 Arg2 arg2, bool is_arg2_null, ...);
    }
  }
}

```

```c++
# hybridse/src/udf/udf.cc
namespace hybridse {
  namespace udf {
    namespace v1 {
      Ret func(Arg1 arg1, Arg2 arg2, bool is_arg2_null, ...) {
        // ...
        // if is_arg1_null
        // 	return Ret(0)
        // else 
        // 	compute and return result
      }
    } 
  } 
} 
```

Configure and register the function into `DefaultUdfLibary` in[hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc):

```c++
# hybridse/src/udf/default_udf_library.cc
RegisterExternal("my_func")
        .args<Arg1, Nulable<Arg2>, ...>(static_cast<R (*)(Arg1, Arg2, bool, ...)>(v1::func))()
        .doc(R"(
            documenting my_func
        )");
```

## 4. SQL Functions Development Examples

### 4.1 SQL Functions Return **BOOL** or Numeric Types: `INT Month(TIMESTAMP)` Function

`INT Month(TIMESTAMP)` function returns the month for a given `timestamp`. Check [3.1 SQL functions return **BOOL** or Numeric types](#3.1-SQL functions return **BOOL** or Numeric types) for more details. 

#### Step 1: Declare and Implement C++ Functions

Declare `month()` function in [hybridse/src/udf/udf.h](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.h)：

```c++
# hybridse/src/udf/udf.h
namespace hybridse {
  namespace udf{
    namespace v1 {
      int32_t month(int64_t ts);
      int32_t month(codec::Timestamp *ts);
    } // namespace v1
  } // namespace udf
} // namepsace hybridse
```

Implement `month()` function in [hybridse/src/udf/udf.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.cc)：

```c++
# hybridse/src/udf/udf.cc
namespace hybridse {
  namespace udf {
    namespace v1 {
      int32_t month(int64_t ts) {
          time_t time = (ts + TZ_OFFSET) / 1000;
          struct tm t;
          gmtime_r(&time, &t);
          return t.tm_mon + 1;
      }
      int32_t month(codec::Timestamp *ts) { return month(ts->ts_); }
    } // namespace v1
  } // namespace udf
} // namepsace hybridse
```

#### Step 2: Register C++ Function to Default Library

Configure and register `month()` into `DefaultUdfLibary` in[hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc):

```c++
namespace hybridse {
namespace udf {
  void DefaultUdfLibrary::InitTimeAndDateUdf() {
    // ...
    RegisterExternal("month")
          .args<Timestamp>(static_cast<int32_t (*)(Timestamp*)>(v1::month))
          .doc(R"(
              @brief Return the month part of a timestamp

              Example:
              @code{.sql}
                  select month(timestamp(1590115420000));
                  -- output 5
              @endcode
              @since 0.1.0
          )");
  }
} // namespace udf
} // namepsace hybridse
```

#### Step3: Function Unit Test

[Add unit tests](Add Unit Tests) in [src/codegen/udf_ir_builder_test.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/codegen/udf_ir_builder_test.cc). Then [compile and test it](2.3.2 Compile and test).

```c++
// month(timestamp) normal check
TEST_F(UdfIRBuilderTest, month_timestamp_udf_test) {
    Timestamp time(1589958000000L);
    CheckUdf<int32_t, Timestamp>("month", 5, time);
}
```

Now, the `udf::v1:month` has been registered into the default library with the name `month`. As a result, we can call `month` in an SQL query while ignoring upper and lower cases.

```SQL
select MONTH(TIMESTAMP(1590115420000)) as m1, month(timestamp(1590115420000)) as m2;
 ---- ---- 
  m1   m2  
 ---- ---- 
  5    5  
 ---- ---- 
```

### 4.2 SQL Functions Return **STRING**, **TIMESTAMP** or **DATE** -  `STRING String(BOOL)`

The `STRING String(BOOL)` function accepts a BOOL type input and converts it to an output of type STRING. Check [3.2 SQL functions return **STRING**, **TIMESTAMP** or **DATE**](#3.2-SQL functions return **STRING**, **TIMESTAMP** or **DATE**) for more details.

#### Step 1: Declare and Implement C++ Functions

Since the SQL function returns **STRING**, the C++ function result should be returned by parameter `codec::StringRef * output`. 

Declare `bool_to_string()` function in [hybridse/src/udf/udf.h](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.h)：

```c++
# hybridse/src/udf/udf.h
namespace hybridse {
  namespace udf{
    namespace v1 {
      void bool_to_string(bool v, hybridse::codec::StringRef *output);
    } // namespace v1
  } // namespace udf
} // namepsace hybridse
```

Implement the `bool_to_string()` function in [hybridse/src/udf/udf.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.cc)：

```c++
# hybridse/src/udf/udf.cc
namespace hybridse {
  namespace udf {
    namespace v1 {
        void bool_to_string(bool v, hybridse::codec::StringRef *output) {
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
 } // namepsace hybridse
```

Notice that we used `AllocManagedStringBuf` to allocate memory from OpenMLDB Memory pool instead of using the `new` operator or the `malloc` api.

#### Step 2: Register C++ Function into Default Library

`STRING String(BOOL)` is a type conversion function, which the developer should configure and register within `DefaultUdfLibrary::InitTypeUdf()` in [hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc).

```c++
namespace hybridse {
	namespace udf {
    void DefaultUdfLibrary::InitTypeUdf() {
      // ...    
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
    }
  } // namespace udf
} // namepsace hybridse
```

#### Step3: Function Unit Test

[Add unit tests](Add Unit Tests) in [src/codegen/udf_ir_builder_test.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/codegen/udf_ir_builder_test.cc). Then [compile and test it](2.3.2 Compile and test).

```c++
// string(bool) normal check
TEST_F(UdfIRBuilderTest, bool_to_string_test) {
    Timestamp time(1589958000000L);
    CheckUdf<StringRef, bool>("string", "true", true);
  	CheckUdf<StringRef, bool>("string", "false", false);
}
```

Now, the `udf::v1:bool_to_string()` function has been registered into the default library with the name `string`. As a result, we can call `string` in an SQL query while ignoring upper and lower cases.

```SQL
select STRING(true) as str_true, string(false) as str_false;
 ----------  ---------- 
  str_true   str_false  
 ----------  ---------- 
   true        true
 ----------  ---------- 
```



### 4.3 SQL Functions Return ***Nullable*** Type - `DATE Date(TIMESTAMP)`

`DATE Date(TIMESTAMP)()` function converts **TIMESTAMP** type to **DATE** type. Check [3.3 SQL functions return ***Nullable*** type](#3.3-SQL functions return ***Nullable*** type) and  [3.2 SQL functions return **STRING**, **TIMESTAMP** or **DATE**](#3.2-SQL functions return **STRING**, **TIMESTAMP** or **DATE**) for more details.

#### Step 1: Declare and Implement Built-In Functions

We implement a function `timestamp_to_date`to convert `timestamp` to the date type. The input is `timestamp` and the output is nullable `date` which is returned by arguments `codec::Date *output` and `bool *is_null`. 

Declare the `timestamp_to_date()` function in [hybridse/src/udf/udf.h](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.h)：

```c++
# hybridse/src/udf/udf.h
namespace hybridse {
  namespace udf{
    namespace v1 {
      void timestamp_to_date(codec::Timestamp *timestamp,
            codec::Date *ret /*result output*/, bool *is_null /*null flag*/);
    } // namespace v1
  } // namespace udf
} // namespace hybridse
```

Implement the `timestamp_to_date()` function in [hybridse/src/udf/udf.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.cc)：

```c++
# hybridse/src/udf/udf.cc
namespace hybridse {
  namespace udf {
    namespace v1 {
        void timestamp_to_date(codec::Timestamp *timestamp,
              codec::Date *ret /*result output*/, bool *is_null /*null flag*/) {
          time_t time = (timestamp->ts_ + TZ_OFFSET) / 1000;
          struct tm t;
          if (nullptr == gmtime_r(&time, &t)) {
              *is_null = true;
              return;
          }
          *ret = codec::Date(t.tm_year + 1900, t.tm_mon + 1, t.tm_mday);
          *is_null = false;
          return;
        }
    } // namespace v1
  } // namespace udf
} // namespace hybridse
```

#### Step 2: Register Built-In Function into Default Library

The following example registers the built-in function ` v1::timestamp_to_date` into the default library with the name `"date"`. 

Given the result is a nullable date type, we configure  **return_by_arg** as ***true*** and return type as `Nullable<Date>`.

`DATE Date(TIMESTAMP)` is Date&Time function, developer should configure and register within `DefaultUdfLibrary::InitTimeAndDateUdf()` in [hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc).

```c++
namespace hybridse {
  namespace udf {
    void DefaultUdfLibrary::InitTimeAndDateUdf() {
      // ...    
        RegisterExternal("date")
          .args<codec::Timestamp>(
              static_cast<void (*)(Timestamp*, Date*, bool*)>(v1::timestamp_to_date))
          .return_by_arg(true)
          .returns<Nullable<Date>>()
          .doc(R"(
              @brief Cast timestamp or string expression to date
              Example:

              @code{.sql}
                  select date(timestamp(1590115420000));
                  -- output 2020-05-22
              @endcode
              @since 0.1.0)");
    }
  } // namespace udf
} // namespace hybridse
```

#### Step3: Function Unit Test

[Add unit tests](Add Unit Tests) in [src/codegen/udf_ir_builder_test.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/codegen/udf_ir_builder_test.cc). Then [compile and test it](2.3.2 Compile and test).

```c++
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

Now, the `udf::v1:timestamp_to_date` has been registered into the default library with the name `date`. As a result, we can call `date()` in an SQL query.

```SQL
select date(timestamp(1590115420000)) as dt;
 ----------
     dt
 ------------
  2020-05-22 
 ------------
```



## 5. Document Management

Documents for all built-in functions can be found in [Built-in Functions](http://4paradigm.github.io/OpenMLDB/zh/main/reference/sql/functions_and_operators/Files/udfs_8h.html). It is a markdown file automatically generated from source, so please do not edit it directly.

- If you are adding a document for a new function, please refer to [2.2.4 Documenting Function](#224-documenting-function). 
- If you are trying to revise a document of an existing function, you can find source code in the files of `hybridse/src/udf/default_udf_library.cc` or `hybridse/src/udf/default_defs/*_def.cc` .

There is a daily workflow that automatically converts the source code to a readable format, which are the contents inside the `docs/*/reference/sql/functions_and_operators` directory. The document website will also be updated accordingly. If you are interested in this process, you can refer to the source directory [udf_doxygen](https://github.com/4paradigm/OpenMLDB/tree/main/hybridse/tools/documentation/udf_doxygen).

