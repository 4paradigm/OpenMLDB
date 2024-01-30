# Built-In Function Development

## 1. Background

OpenMLDB contains hundreds of built-in functions that help data scientists extract features and analyze data. For instance, we now have aggregation functions like `SUM`, `AVG`, `MAX`, `MIN`, `COUNT`, etc, to aggregate data over a table or a specific window. In addition to that, we also have scalar functions like `ABS`, `SIN`, `COS`, `DATE`, `YEAR`, etc, to extract features based on one-row data. 

OpenMLDB classifies functions as aggregate or scalar depending on the input data values and result values.

- A *scalar function* receives **a single value** for each argument and returns a single value result. A scalar function can be classified into several groups:
  - Mathematical function
  - Logical function
  - Date & Time function
  - String function
  - Conversion function

- An *aggregate function* receives **a set of** values for each argument (such as the values of a column) and returns a single-value result for the set of input values. 

This article serves as an introductory guide to developing SQL built-in functions, aiming to guide developers in quickly grasping the basic methods of developing custom functions.

First, we will provide a detailed overview of the development steps, classification, and examples of scalar function development. This will enable developers to understand the basic development and registration patterns of custom functions.

Subsequently, we will transition to the details of developing complex aggregate functions. We sincerely welcome more developers to join our community and assist us in expanding and developing the built-in function collection.

## 2. Develop a Built-In SQL Function

In this section, we are going to introduce the basic steps to implement and register an SQL built-in function. Built-in SQL function development involves the following steps:

- Develop a built-in C++ function
- Register and configure the function
- Create function unit tests

### 2.1 Develop a Built-In C++ Function

Generally, developers should implement a C++ function for each SQL function. Thus, users will invoke the C++ function when they call the corresponding function from SQL.

Developers need to **take care of the following** rules when developing a function:

#### 2.1.1 Code Location

Developers can declare function in [hybridse/src/udf/udf.h](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.h) and implement it in  [hybridse/src/udf/udf.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.cc).
If the function is complex, developers can declare and implement in separate `.h` and `.cc` files in [hybridse/src/udf/](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/). 

The functions are usually within namespace `hybridse::udf::v1`.

- ```c++
  # hybridse/src/udf/udf.h
  namespace hybridse {
    namespace udf {
      namespace v1 {
        // declare built-in function
      } // namespace v1
    } // namespace udf
  } // namespace hybridse
  ```

- ```c++
  # hybridse/src/udf/udf.cc
  namespace hybridse {
    namespace udf {
      namespace v1 {
        // implement built-in function
      } // namespace v1
    } // namespace udf
  } // namespace hybridse
  ```

#### 2.1.2 C++ Function Naming Rules

- Function names are all lowercase, with underscores between words. Check [snake_case](https://en.wikipedia.org/wiki/Snake_case) for more details.
- Function names should be clear and readable. Use names that describe the purpose or intent of the function.

(c_vs_sql)=
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
  | ARRAY     | `ArrayRef`         |

*Note: ARRAY type is not supported in storage or as SQL query output column type*

#### 2.1.4 Parameters and Result

- SQL function parameters and C++ function parameters have the same position order.

- C++ function parameter types should match the SQL types. Check [2.1.3 C++ and SQL Data Type](c_vs_sql) for more details.

- SQL function return type:

  - If SQL function return BOOL or Numeric type (e.g., **BOOL**, **SMALLINT**, **INT**, **BIGINT**, **FLOAT**, **DOUBLE**), the C++ function should be designed to return corresponding C++  type（`bool`, `int16_t`, `int32_t`, `int64_t`, `float`, `double`).

    - ```c++
      // SQL: DOUBLE FUNC_DOUBLE(INT)
      double func_return_double(int); 
      ```

  - If SQL function return **STRING**, **TIMESTAMP**, **DATE**, **ArrayRef**, the C++ function result should be returned in parameter with the corresponding C++ pointer type (`codec::StringRef*`, `codec::Timestamp*`, `codec::Date*`).

    - ```c++
      // SQL: STRING FUNC_STR(INT)
      void func_output_str(int32_t, codec::StringRef*); 
      ```

  - If SQL function return type is ***Nullable***, we need one more `bool*`parameter to return a `is_null` flag.

    - ```c++
      // SQL: Nullable<DATE> FUNC_NULLABLE_DATE(BIGINT)
      void func_output_nullable_date(int64_t, codec::Date*, bool*); 
      ```

  - Notice that return types have greater impact on built-in function developing behaviours. We will cover the details in a later section [3.2 Scalar Function Development Classification](sfunc_category).

- Handling Nullable Parameters:
  - Generally, OpenMLDB adopts a uniform approach to handling NULL parameters for all built-in scalar functions. That is, if any input parameter is NULL, the function will directly return NULL.
  - However, for scalar functions or aggregate functions that require special handling of NULL parameters, you can configure the parameter as `Nullable<ArgType>`. In the C++ function, you will then use the corresponding C++ type of ArgType and `bool*` to express this parameter. For more details, refer to [3.2.4 Nullable SQL Function Parameters](arg_nullable).

#### 2.1.5 Memory Management

- Operator `new` operator or method `malloc` are forbidden in C++ built-in function implementation.
- In C++ built-in aggregate functions, it is permissible to use the `new` or `malloc` functions to allocate memory during initialization. However, it is crucial to ensure that the allocated space is released when the `output` generates the final result.
- Developers must call provided memory management APIs in order to archive space allocation for UDF output parameters:
  - `hybridse::udf::v1::AllocManagedStringBuf(size)` to allocate space. OpenMLDB `ByteMemoryPool` will assign continous space to the function and will release it when safe.
    - If allocated size < 0, allocation will fail. `AllocManagedStringBuf` return null pointer.
    - If allocated size exceed the MAX_ALLOC_SIZE which is 2048, the allocation will fail. `AllocManagedStringBuf` return null pointer.
  - `hybridse::udf::v1::AllocManagedArray(ArrayRef<T>*, uint64_t)`: allocate space for array types

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

#### 2.2.3 Register Function Interface

- Registration Scalar functions:
  - For scalar function with a single inout type: `RegisterExternal("register_func_name")`
  - For generic function that supports multiple types: `RegisterExternalTemplate<FTemplate>("register_func_name")`
- Registration for aggregate functions:
  - For aggregate function with a single inout type：`RegisterUdaf("register_func_name")`
  - For generic function that supports multiple types：`RegisterUdafTemplate<FTemplate>("register_func_name")`

The specific interface definitions will be elaborated in detail in the following sections.

#### 2.2.4 Documenting Function

`ExternalFuncRegistryHelper` provides API `doc(doc_string)`  to document a function. Documenting function is describing its use and functionality to the users. While it may be helpful in the development process, the main intended audience is the users.  So we expect the docstring to be **clear** and **legible**. 

Function docstrings should contain the following information:

- **@brief** command to add a summary of the function's purpose and behaviour. 
- **@param** command to document the parameters.
- **Examples** of the function's usage from SQL queries. Demo SQL should be placed in a `@code/@endcode` block.
- **@since** command to specify the production version when the function was added to OpenMLDB. The version can be obtained from the project's [CMakeList.txt](https://github.com/4paradigm/OpenMLDB/blob/main/CMakeLists.txt): ` ${OPENMLDB_VERSION_MAJOR}.${OPENMLDB_VERSION_MINOR}.${OPENMLDB_VERSION_BUG}`

```c++
RegisterExternal("function_name")
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

#### 2.2.5 Register Alias

Sometimes, we don't have to implement and register a function when it is an alias to another function that already exists in the default library. We can simply use api `RegisterAlias("alias_func", "original_func")` to link the current register function name with an existing registered name.

```c++
// substring() is registered into default library already 
RegisterAlias("substr", "substring");
```

## 2.3 Function Unit Test

Once a function is registered/developed, the developer should add some related unit tests to make sure everything is going well. 

#### 2.3.1 Add Unit Tests

Generally, developers can test scalar functions with [src/codegen/udf_ir_builder_test.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/codegen/udf_ir_builder_test.cc), and test aggregate functions by adding `TEST_F` cases to [src/codegen/udf_ir_builder_test.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/codegen/udf_ir_builder_test.cc). OpenMLDB provides `CheckUdf` so that the developer can perform function checking easily.

```c++
CheckUdf<return_type, arg_type,...>("function_name", expect_result, arg_value,...);
```

For each function signature, we at least have to:

- Add a unit test with a normal result
- If parameter is ***nullable***, add a unit test with NULL input to produce a normal result
- Add a unit test with a null result if the result is **nullable**

**Example**:
- Add unit test in [hybridse/src/codegen/udf_ir_builder_test.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/codegen/udf_ir_builder_test.cc):
  ```c++
  // month(timestamp) normal check
  TEST_F(UdfIRBuilderTest, month_timestamp_udf_test) {
      Timestamp time(1589958000000L);
      CheckUdf<int32_t, Timestamp>("month", 5, time);
  }
  
  // date(timestamp) normal check
  TEST_F(UdfIRBuilderTest, timestamp_to_date_test_0) {
      CheckUdf<Nullable<Date>, Nullable<Timestamp>>(
          "date", Date(2020, 05, 20), Timestamp(1589958000000L));
  }
  // date(timestamp) null check
  TEST_F(UdfIRBuilderTest, timestamp_to_date_test_null_0) {
      CheckUdf<Nullable<Date>, Nullable<Timestamp>>("date", nullptr, nullptr);
  }
  ```

- Add unit test in [hybridse/src/udf/udaf_test.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udaf_test.cc):
  ```c++
  // avg udaf test
  TEST_F(UdafTest, avg_test) {
      CheckUdf<double, ListRef<int16_t>>("avg", 2.5, MakeList<int16_t>({1, 2, 3, 4}));
  }
  ```

(compile_ut)=
#### 2.3.2 Compile and Test

- Compile `udf_ir_builder_test` and test
  ```bash
  # Compile udf_ir_builder_test, default output path is build/hybridse/src/codegen/udf_ir_builder_test
  make OPENMLDB_BUILD_TARGET=udf_ir_builder_test TESTING_ENABLE=ON
  
  # Run test, note that environment variable  SQL_CASE_BASE_DIR need to be specified as OpenMLDB project path
  SQL_CASE_BASE_DIR=${OPENMLDB_DIR} ./build/hybridse/src/codegen/udf_ir_builder_test
  ```
- Compile `udaf_test` and test
  ```bash
  # Compile udaf_test, default output path is build/hybridse/src/udf/udaf_test
  make OPENMLDB_BUILD_TARGET=udaf_test TESTING_ENABLE=ON
  
  # Run test, note that environment variable  SQL_CASE_BASE_DIR need to be specified as OpenMLDB project path
  SQL_CASE_BASE_DIR=${OPENMLDB_DIR} ./build/hybridse/src/udf/udaf_test
  ```

If testing is to be done through SDK or command line, `OpenMLDB` needs to be recompiled. For compilation, refer to [compile.md](../deploy/compile.md).

## 3. Scalar Function Development
### 3.1 Registration and Interface Configuration

#### 3.1.1 Registration of Scalar Function Supporting Single Data Type

The `DefaultUdfLibrary` provides the `RegisterExternal` interface to facilitate the registration of built-in scalar functions and initialize the registration name of the function. This method requires specifying a data type and only supports declared data types.


```c++
RegisterExternal("register_func_name")
  .args<arg_type, ...>(static_cast<return_type (*)(arg_type, ...)>(v1::func_ptr))
  .return_by_arg(bool_value)
  .returns<return_type>
```

The configuration of a function generally includes: function pointer configuration, parameter type configuration, and return value configuration.

- Configuring the C++ function pointer: `func_ptr`. It is important to use static_cast to convert the pointer to a function pointer, considering code readability and compile-time safety.
- Configuring parameter types: `args<arg_type,...>`.
- Configuring return value type: `returns<return_type>`. Typically, it is not necessary to explicitly specify the return type. However, if the function result is nullable, you need to explicitly configure the ***return type*** as ***returns<Nullable<return_type>>***.
- Configuring the return method: `return_by_arg()`.
  - When **return_by_arg(false)**, the result is directly returned through the `return` statement. OpenMLDB defaults to `return_by_arg(false)`.
  - When **return_by_arg(true)**, the result is returned through parameters:
    - If the return type is ***non-nullable***, the function result is returned through the last parameter.
    - If the return type is ***nullable***, the function result value is returned through the second-to-last parameter, and the ***null flag*** is returned through the last parameter. If the ***null flag*** is ***true***, the function result is ***null***; otherwise, the function result is retrieved from the second-to-last parameter.

The following code demonstrates an example of registering the built-in single-row function `substring`. You can find the code in [hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc).
```c++
// void sub_string(StringRef *str, int32_t from, StringRef *output);

RegisterExternal("substring")
        .args<StringRef, int32_t, int32_t>(
            static_cast<void (*)(StringRef*, int32_t, int32_t,
                                 StringRef*)>(udf::v1::sub_string))
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
```

#### 3.1.2 Registration of Built-In Functions Supporting Generic Templates
We also provide the `RegisterExternalTemplate` interface to support the registration of generic built-in single-row functions, allowing simultaneous support for multiple data types.

```c++
RegisterExternalTemplate<TemplateClass>("register_func_name")
  .args_in<arg_type, ...>()
  .return_by_arg(bool_value)
```

The configuration of a function generally includes: function template configuration, supported parameter types configuration, and return method configuration.

- Configuring the function template: `TemplateClass`.
- Configuring supported parameter types: `args_in<arg_type,...>`.
- Configuring the return method: `return_by_arg()`
  - When **return_by_arg(false)**, the result is directly returned through the `return` statement. OpenMLDB defaults to `return_by_arg(false)`.
  - When **return_by_arg(true)**, the result is returned through parameters.

The following code shows the code example of registering `abs` scalar function (code can be found at [hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc)).
```c++
RegisterExternalTemplate<v1::Abs>("abs")
    .doc(R"(
        @brief Return the absolute value of expr.

        Example:

        @code{.sql}

            SELECT ABS(-32);
            -- output 32

        @endcode

        @param expr

        @since 0.1.0)")
    .args_in<int64_t, double>();
```

Development of generic template built-in scalar functions is similar to that of single data type built-in scalar functions. In this document, we won't delve into detailed discussions on generic template functions. The remaining content in this chapter primarily focuses on the development of single data type built-in scalar functions.

(sfunc_category)=
## 3.2 Built-in Scalar Function Development Template

We classified built-in function into 3 types based on its return type:

- SQL functions return **BOOL** or Numeric types, e.g., **SMALLINT**, **INT**, **BIGINT**, **FLOAT**, **DOUBLE**
- SQL functions return **STRING**, **TIMESTAMP**, **DATE**, **ArrayRef**
- SQL functions return ***Nullable*** type

Return types have a greater impact on the built-in function's behaviour. We will cover the details of the three types of SQL functions in the following sections.

(return_bool)=

### 3.2.1 SQL Functions Return **BOOL** or Numeric Types

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

### 3.2.2 SQL Functions Return **STRING**, **TIMESTAMP** or **DATE**

If an SQL function returns **STRING**, **TIMESTAMP** or **DATE**, then the C++ function result should be returned in the parameter with the corresponding C++ pointer type (`codec::StringRef*`, `codec::Timestamp*`, `codec::Date*`).

Thus the C++ function can be declared and implemented as follows:

```c++
# hybridse/src/udf/udf.h
namespace hybridse {
  namespace udf {
    namespace v1 {
      void func(Arg1 arg1, Arg2 arg2, ..., Ret* result);
    }
  }
}

```

```c++
# hybridse/src/udf/udf.cc
namespace hybridse {
  namespace udf {
    namespace v1 {
      void func(Arg1 arg1, Arg2 arg2, ..., Ret* ret) {
        // ...
        // *ret = result value
      }
    }
  }
}
```

Configure and register the function into `DefaultUdfLibary` in[hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc). Note that if the function needs to return through parameter, `return_by_arg(true)` needs to be configured.

```c++
# hybridse/src/udf/default_udf_library.cc
RegisterExternal("my_func")
        .args<Arg1, Arg2, ...>(static_cast<R (*)(Arg1, Arg2, ..., Ret*)>(v1::func))
  			.return_by_arg(true)
        .doc(R"(
            documenting my_func
        )");
```

### 3.2.3 SQL Functions Return ***Nullable*** type

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

(arg_nullable)=

### 3.2.4 SQL Functions Handle Nullable Argument

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

## 3.3. SQL Functions Development Examples

### 3.3.1 SQL Functions Return **BOOL** or Numeric Types: `INT Month(TIMESTAMP)` Function

`INT Month(TIMESTAMP)` function returns the month for a given `timestamp`. Check [3.2.1 SQL functions return **BOOL** or Numeric types](return_bool) for more details. 

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

Add unit test `TEST_F` in [src/codegen/udf_ir_builder_test.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/codegen/udf_ir_builder_test.cc). Then [Compile and Test](compile_ut)。

```c++
// month(timestamp) normal check
TEST_F(UdfIRBuilderTest, month_timestamp_udf_test) {
    Timestamp time(1589958000000L);
    CheckUdf<int32_t, Timestamp>("month", 5, time);
}
```

Recompile `OpenMLDB` upon completing the development. Now, the `udf::v1:month` has been registered into the default library with the name `month`. As a result, we can call `month` in an SQL query while ignoring upper and lower cases.

```SQL
select MONTH(TIMESTAMP(1590115420000)) as m1, month(timestamp(1590115420000)) as m2;
 ---- ---- 
  m1   m2  
 ---- ---- 
  5    5  
 ---- ---- 
```

### 3.3.2 SQL Functions Return **STRING**, **TIMESTAMP** or **DATE** -  `STRING String(BOOL)`

The `STRING String(BOOL)` function accepts a **BOOL** type input and converts it to an output of type STRING. Check [3.2.2 SQL functions return **STRING**, **TIMESTAMP** or **DATE**](#322sql-functions-return-string-timestamp-or-date) for more details.

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

Add unit tests `TEST_F` in [src/codegen/udf_ir_builder_test.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/codegen/udf_ir_builder_test.cc). Then [Compile and Test](compile_ut).

```c++
// string(bool) normal check
TEST_F(UdfIRBuilderTest, bool_to_string_test) {
    Timestamp time(1589958000000L);
    CheckUdf<StringRef, bool>("string", "true", true);
  	CheckUdf<StringRef, bool>("string", "false", false);
}
```

Recompile `OpenMLDB` upon completing the development. Now, the `udf::v1:bool_to_string()` function has been registered into the default library with the name `string`. As a result, we can call `String()` in an SQL query while ignoring upper and lower cases.

```SQL
select STRING(true) as str_true, string(false) as str_false;
 ----------  ---------- 
  str_true   str_false  
 ----------  ---------- 
   true        true
 ----------  ---------- 
```


### 3.3.3 SQL Functions Return ***Nullable*** Type - `DATE Date(TIMESTAMP)`

`DATE Date(TIMESTAMP)()` function converts **TIMESTAMP** type to **DATE** type. Check [3.2.3 SQL functions return ***Nullable*** type](#323-sql-functions-return-nullable-type) and  [3.2.2 SQL functions return **STRING**, **TIMESTAMP** or **DATE**](#322-sql-functions-return-string-timestamp-or-date) for more details.

#### Step 1: Declare and Implement Built-In Functions

Due to the fact that the `date` type in OpenMLDB is a structured type, when designing functions, the result is not directly returned but is instead stored in the parameters for return. Additionally, considering that date conversions may encounter exceptions or failures, the return result is marked as ***nullable***. Therefore, an additional parameter, ***is_null***, is introduced to indicate whether the result is null or not.

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

The configuration of the function name and function parameters is similar to that of regular functions. However, there are additional considerations for configuring the return value type:

- Since the function result is stored in parameters for return, configure `return_by_arg(true)`.
- Since the function result may be null, configure `.returns<Nullable<Date>>`.

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

Add unit tests `TEST_F` in [src/codegen/udf_ir_builder_test.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/codegen/udf_ir_builder_test.cc). Then [Compile and Test](compile_ut).

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

Recompile `OpenMLDB` upon completing the development. Now, the `udf::v1:timestamp_to_date` has been registered into the default library with the name `date`. As a result, we can call `date()` in an SQL query while ignoring upper and lower cases.

```SQL
select date(timestamp(1590115420000)) as dt;
 ----------
     dt
 ------------
  2020-05-22 
 ------------
```

## 4. Aggregation Function Development

### 4.1. Registration and Configuration of Interface

#### 4.1.1 Registration of Aggregation Functions Supporting a Single Data Type

The `DefaultUdfLibrary` provides the `RegisterUdaf` interface to facilitate the registration of built-in aggregation functions and initialize the function's registration name. This method requires specifying a data type and only supports declared data types.


```c++
RegisterUdaf("register_func_name")
  .templates<OUT, ST, IN, ...>()
  .init("init_func_name", init_func_ptr, return_by_arg=false)
  .update("update_func_name", update_func_ptr, return_by_arg=false)
  .output("output_func_name", output_func_ptr, return_by_arg=false)
```


Unlike the registration of scalar functions, aggregation functions require the registration of three functions: `init`, `update`, and `output`, which correspond to the initialization of the aggregation function, the update of intermediate states, and the output of the final result. 
The configuration for these functions is as follows:

- Configure parameter types:
  - OUT: Output parameter type.
  - ST: Intermediate state type.
  - IN, ...: Input parameter types.
- Configure the `init` function pointer: `init_func_ptr`, with a function signature of `ST* Init()`.
- Configure the `update` function pointer: `update_func_ptr`,
  - If the input is non-nullable, the function signature is `ST* Update(ST* state, IN val1, ...)`.
  - If it is necessary to check whether the input is **Nullable**, this parameter can be configured as `Nullable`, and an additional `bool` parameter is added after the corresponding parameter in the function to store information about whether the parameter value is null.
    The function signature is: `ST* Update(ST* state, IN val1, bool val1_is_null, ...)`.
- Configure the output function pointer: `output_func_ptr`.
  When the function's return value may be null, an additional `bool*` parameter is required to store whether the result is null
  (refer to [3.2.3 SQL Functions Return ***Nullable*** type](#323-sql-functions-return-nullable-type).


The following code demonstrates an example of adding a new aggregation function `second`. The `second` function returns the non-null second element in the aggregated data. For the sake of demonstration, the example supports only the `int32_t` data type:
```c++
struct Second {
    static std::vector<int32_t>* Init() {
        auto list = new std::vector<int32_t>();
        return list;
    }

    static std::vector<int32_t>* Update(std::vector<int32_t>* state, int32_t val, bool is_null) {
        if (!is_null) {
            state->push_back(val);
        }
        return state;
    }

    static void Output(std::vector<int32_t>* state, int32_t* ret, bool* is_null) {
        if (state->size() > 1) {
            *ret = state->at(1);
            *is_null = false;
        } else {
            *is_null = true;
        }
        delete state;
    }
};

RegisterUdaf("second")
    .templates<Nullable<int32_t>, Opaque<std::vector<int32_t>>, Nullable<int32_t>>()
    .init("second_init", Second::Init)
    .update("second_update", Second::Update)
    .output("second_output", reinterpret_cast<void*>(Second::Output), true)
    .doc(R"(
        @brief Get the second non-null value of all values.

        @param value  Specify value column to aggregate on.

        Example:

        |value|
        |--|
        |1|
        |2|
        |3|
        |4|
        @code{.sql}
            SELECT second(value) OVER w;
            -- output 2
        @endcode
        @since 0.5.0
    )");
```

#### 4.1.2 Registration of Aggregation Functions Supporting Generics
We also provide the `RegisterUdafTemplate` interface for registering an aggregation function that supports generics.

```c++
RegisterUdafTemplate<TemplateClass>("register_func_name")
  .args_in<arg_type, ...>()
```

- Configure the aggregation function template: `TemplateClass`.
- Configure all supported parameter types: `args_in<arg_type, ...>`.

The following code demonstrates an example of registering the `distinct_count` aggregation function. You can find the code in [hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc).
```c++
RegisterUdafTemplate<DistinctCountDef>("distinct_count")
    .doc(R"(
            @brief Compute number of distinct values.

            @param value  Specify value column to aggregate on.

            Example:

            |value|
            |--|
            |0|
            |0|
            |2|
            |2|
            |4|
            @code{.sql}
                SELECT distinct_count(value) OVER w;
                -- output 3
            @endcode
            @since 0.1.0
        )")
    .args_in<bool, int16_t, int32_t, int64_t, float, double, Timestamp,
             Date, StringRef>();
```

## 5. Example Code Reference

### 5.1. Scalar Function Example Code Reference
For more scalar function example code, you can refer to: 
[hybridse/src/udf/udf.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.cc)

### 5.2. Aggregation Function Example Code Reference
For more aggregation function example code, you can refer to:
[hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc)



## 6. Documentation Management

Documentations for all built-in functions can be found in [Built-in Functions](../openmldb_sql/udfs_8h.md). It is a markdown file automatically generated from source, so please do not edit it directly.

- If you need to document the newly added functions, please refer to section [2.2.4 Documenting Function](#224-documenting-function) which explains that the documentation for built-in functions is managed in CPP source code. Subsequently, a series of steps will be taken to generate more readable documentation, which will appear in the `docs/*/openmldb_sql/` directory on the website.
- If you need to modify the documentation for an existing function, you can locate the corresponding documentation in the file `hybridse/src/udf/default_udf_library.cc` or `hybridse/src/udf/default_defs/*_def.cc` and make the necessary changes.

In the OpenMLDB project, a GitHub Workflow task is scheduled on a daily basis to regularly update the relevant documentation here. Therefore, modifications to the documentation for built-in functions only require changing the content in the corresponding source code locations as described above. The `docs` directory and the content on the website will be periodically updated accordingly. For details on the documentation generation process, you can check [udf_doxygen](https://github.com/4paradigm/OpenMLDB/tree/main/hybridse/tools/documentation/udf_doxygen).
