# 内置函数开发

## 1. 背景

OpenMLDB内置了上百个SQL函数，以供数据科学家作数据分析和特征抽取。目前，我们还提供了单行函数(Scalar Function)如`ABS`, `SIN`, `COS`, `DATE`, `YEAR`等支持单行数据处理。
同时，我们提供聚合类函数(Aggregate Function)如 `SUM`, `AVG`, `MAX`, `MIN`, `COUNT`来支持全表聚合和窗口聚合。

- 单行函数（Scalar Function）对若干数据执行计算，并返回单个值。单行函数按功能不同，又可分为：
  - 数学函数
  - 逻辑函数
  - 时间和日期函数
  - 字符串函数
  - 类型转换函数

- 聚合函数（Aggregate Function）对数据集（如一列数据）执行计算，并返回单个值。

本文是SQL内置函数的开发入门指南，旨在指引开发者快速掌握基础的内置函数开发方法。
首先会详细介绍单行函数开发步骤、分类以及示例，让开发者可以理解基本的自定义函数的开发以及注册模式。
之后会过渡到复杂的聚合函数开发细节。我们诚挚欢迎更多的开发者加入社区，帮助我们扩展和开发内置函数集。

## 2. SQL函数开发步骤

本章节主要介绍SQL内置函数的注册和配置的基本步骤。内置函数的开发包含以下几个步骤：

- 开发C++内置函数
- 注册和配置函数
- 函数单元测试

### 2.1.  开发C++内置函数

一般来说，开发者应需要为每一个SQL的内置函数开发一个对应的C++函数。用户通过SQL查询函数时，最终底层调用的就是这个C++函数。C++函数开发需要注意一下几点：

#### 2.1.1 开发位置

开发者需要在[hybridse/src/udf/udf.h](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.h) 中声明C++函数，并在[hybridse/src/udf/udf.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.cc) 中实现C++函数。
如果函数较为复杂，可以在[hybridse/src/udf/](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/) 下另建新的`.h`和`.cc`文件，来声明和定义函数。

函数一般声明和定义在namespace `hybridse::udf::v1`下。

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

#### 2.1.2 C++函数名规范

- C++内置函数名统一使用[snake_case](https://en.wikipedia.org/wiki/Snake_case) 风格
- 要求函数名能清晰表达函数功能

(c_vs_sql)=
#### 2.1.3 C++类型与SQL类型对应关系

内置C++函数的参数类型限定为：BOOL类型，数值类型，时间戳日期类型和字符串类型。C++类型SQL类型对应关系如下：

| SQL类型     | C/C++ 类型    |
|:----------|:------------|
| BOOL      | `bool`      |
| SMALLINT  | `int16_t`   |
| INT       | `int32_t`   |
| BIGINT    | `int64_t`   |
| FLOAT     | `float`     |
| DOUBLE    | `double`    |
| STRING    | `StringRef` |
| TIMESTAMP | `Timestamp` |
| DATE      | `Date`      |
| ARRAY     | `ArrayRef`  |

*注意 ARRAY 类型尚不支持在存储中作为表的列类型或者作为 SQL 查询的输出列格式*

#### 2.1.4 函数参数和返回值

- C++函数与SQL函数参数相对位置相同

- C++函数与SQL函数与参数类型类型一一对应。详情参见 [2.1.3 C++类型与SQL类型对应关系](c_vs_sql)

- 函数返回值要考虑以下几种情况：

  - 当SQL函数返回值为布尔或数值类型（如**BOOL**, **SMALLINT**, **INT**, **BIGINT**, **FLOAT**, **DOUBLE**）时，一般地，C++的函数返回值为与之对应的C++类型（`bool`, `int16_t`, `int32_t`, `int64_t`, `float`, `double`)。

    - ```c++
      // SQL: DOUBLE FUNC_DOUBLE(INT)
      double func_return_double(int); 
      ```

  - 当SQL函数返回值为**STRING**, **TIMESTAMP**, **DATE**或者**ArrayRef**时，要求C++函数通过参数来输出结果。这意味着，C++的**输入参数**后额外有一个指针类型的**输出参数**（`StringRef*`, `Timestamp*`或 `Date*`)用来存放和返回结果值

    - ```c++
      // SQL: STRING FUNC_STR(INT)
      void func_output_str(int32_t, StringRef*); 
      ```

  - 当SQL函数的返回值可能为 NULL (**Nullable**)时，额外要有一个`bool*`类型的**输出参数**来存放结果为空与否

    - ```c++
      // SQL: Nullable<DATE> FUNC_NULLABLE_DATE(BIGINT)
      void func_output_nullable_date(int64_t, Date*, bool*); 
      ```
    
  - 注意， SQL 函数返回值将对内置函数的实现和注册方式产生较大的影响，我们将在后续分别讨论，详情参见 [3.2. 单行函数开发分类](sfunc_category)。
  
- 参数Nullable的处理方式：

  - 一般地，OpenMLDB对所有内置的单行函数采取统一的NULL参数处理方式。即任意一个输入参数为NULL时，直接返回NULL。
  - 但需要对NULL参数做特殊处理的单行函数或者聚合函数，那么可以将参数配置为`Nullable<ArgType>`，然后在C++ function中将使用ArgType对应的C++类型和`bool*`来表达这个参数。详情参见[3.2.4 SQL函数参数是Nullable](arg_nullable)。

#### 2.1.5 内存管理

- C++内置单行函数中，不允许使用`new`操作符或者`malloc`函数开辟空间。 
- C++内置聚合函数，可以在初始化的时候使用`new`或者`malloc`函数开辟空间，但需要保证在`output`生成最终结果的时候，将空间释放。
- 涉及到为 UDF 输出参数分配内存的，必须使用OpenMLDB提供的内存管理接口:
  - `hybridse::udf::v1::AllocManagedStringBuf(size)`: 系统会从内存池`ByteMemoryPool`中分配指定大小的连续空间给该函数，并在安全的时候自动释放空间。
    - 若空间size大于2M字节，则分配失败，返回nullptr。
    - 若空间size < 0，则分配失败，返回nullptr。
  - `hybridse::udf::v1::AllocManagedArray(ArrayRef<T>*, uint64_t)`: 为 array 输出类型分配空间

**例子:**

`hybridse::udf::v1::bool_to_string()`负责将bool值转位字符串"true"或者"false"。需要`AllocManagedStringBuf`动态开辟相应空间存放字符串。

```c++
namespace hybridse {
  namespace udf {
    namespace v1 {
      void bool_to_string(bool v, StringRef *output) {
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

### 2.2. 注册和配置接口

#### 2.2.1 默认函数库

OpenMLDB的 `DefaultUdfLibrary` 负责存放和管理内置的全局SQL函数。默认函数库`DefaultUdfLibrary` 声明在文件[hybridse/src/udf/default_udf_library.h](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.h) 文件中，实现在[hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc) 文件中。

开发者先在[hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc) 中将C++函数注册到默认函数库 `DefaultUdfLibrary` 中，用户才能通过SQL语句访问这个函数。为了更好的管理函数注册，`DefaultUdfLibrary`提供几类`DefaultUdfLibrary::InitXXXX()` 接口来管理不同功能的函数配置注册:

- 数学函数：在`DefaultUdfLibrary::InitMathUdf()`中注册
- 逻辑函数：在`DefaultUdfLibrary::InitLogicalUdf()`中注册
- 时间和日期函数：在`DefaultUdfLibrary::InitTimeAndDateUdf()`中注册
- 字符串函数：在`DefaultUdfLibrary::InitStringUdf()`中注册
- 类型转换函数：在`DefaultUdfLibrary::InitTypeUdf()`中注册
- 聚合函数：在`DefaultUdfLibrary::InitUdaf()`中注册

#### 2.2.2 注册名

- 注册名就是外部用户可以访问的SQL函数名
- SQL函数注册名一般统一使用[snake_case](https://en.wikipedia.org/wiki/Snake_case)风格。
- SQL函数名**不要求**与对C++函数名**完全一致**。因为SQL函数名和C++函数是通过函数注册操作关联在一起
- 用户使用SQL函数时，是函数名是[大小写不敏感](https://en.wiktionary.org/wiki/case_insensitive) 的。例如，注册名为"aaa_bb"。用户在SQL语句中通过AAA_BB(), Aaa_Bb(), aAa_bb()均可调用"aaa_bb"关联的函数。

#### 2.2.3 注册函数接口
- 单行函数注册：
  - 支持单一输入类型：`RegisterExternal("register_func_name")`
  - 支持泛型：`RegisterExternalTemplate<FTemplate>("register_func_name")`
- 聚合函数注册：
  - 支持单一输入类型：`RegisterUdaf("register_func_name")`
  - 支持泛型：`RegisterUdafTemplate<FTemplate>("register_func_name")`

具体的接口定义会分别在下面章节中详细说明。

#### 2.2.4 配置函数文档

我们提供了`doc(doc_string)`API来配置文档。函数文档用来描述函数的功能和用法。它的主要受众是普通用户。因此，要求文档清晰易懂。一般需要包含一下几类信息：

- **@brief** ：简要描述函数的功能
- **@param**：函数参数描述
- **Examples**： 函数的使用示例。一般需要提供SQL的查询示例。SQL语句需要放置在 `@code/@endcode` 代码块中。
- **@since** ：指明引入该函数的OpenMLDB版本。OpenMLDB版本可以从项目的 [CMakeList.txt](https://github.com/4paradigm/OpenMLDB/blob/main/CMakeLists.txt) 获得:` ${OPENMLDB_VERSION_MAJOR}.${OPENMLDB_VERSION_MINOR}.${OPENMLDB_VERSION_BUG}`

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

#### 2.2.5 注册函数别名

一般情况下，需要开发C++函数，然后注册到`DefaultUdfLibrary`中去。但开发已有函数的别名函数时，则不需要遵循标准开发流程；仅需要使用`RegisterAlias("alias_func", "original_func")`API将别名关联到到已注册函数上即可。

```c++
// substring() is registered into default library already 
RegisterAlias("substr", "substring");
```

### 2.3. 单元函数测试

#### 2.3.1 添加函数单元测试

函数开发完成后，开发者需要添加相对应的测试用例以确保系统运行正确。
一般地，单行函数测试可以在[hybridse/src/codegen/udf_ir_builder_test.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/codegen/udf_ir_builder_test.cc) ,
聚合函数测试可以在[hybridse/src/udf/udaf_test.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udaf_test.cc)
中添加`TEST_F`单测。我们提供了CheckUdf函数以便开发者检验函数结果。

```c++
CheckUdf<return_type, arg_type,...>("function_name", expect_result, arg_value,...);
```

对每一个函数（每一种参数组合），我们至少需要实现:

- 添加一个单测，验证普通结果
- 如果参数是***nullable***，需要添加一个单测，输入有NULL的情况下，结果正确
- 如果结果是***nullable***，需要添加一个单测，验证结果为空

**示例:**

- 在[hybridse/src/codegen/udf_ir_builder_test.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/codegen/udf_ir_builder_test.cc)
添加单测：
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

- 在[hybridse/src/udf/udaf_test.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udaf_test.cc)
  添加单测：
  ```c++
  // avg udaf test
  TEST_F(UdafTest, avg_test) {
      CheckUdf<double, ListRef<int16_t>>("avg", 2.5, MakeList<int16_t>({1, 2, 3, 4}));
  }
  ```

(compile_ut)=

#### 2.3.2 编译和执行单测

- 编译`udf_ir_builder_test`并测试
  ```bash
  # 编译 udf_ir_builder_test, 默认输出路径为 build/hybridse/src/codegen/udf_ir_builder_test
  make OPENMLDB_BUILD_TARGET=udf_ir_builder_test TESTING_ENABLE=ON
  
  # 运行测试程序, 注意需要指定环境变量 SQL_CASE_BASE_DIR 到 OpenMLDB 项目到克隆路径
  SQL_CASE_BASE_DIR=${OPENMLDB_DIR} ./build/hybridse/src/codegen/udf_ir_builder_test
  ```
- 编译`udaf_test`测试
  ```bash
  # 编译 udaf_test, 默认输出路径为 build/hybridse/src/udf/udaf_test
  make OPENMLDB_BUILD_TARGET=udaf_test TESTING_ENABLE=ON
  
  # 运行测试程序, 注意需要指定环境变量 SQL_CASE_BASE_DIR 到 OpenMLDB 项目到克隆路径
  SQL_CASE_BASE_DIR=${OPENMLDB_DIR} ./build/hybridse/src/udf/udaf_test
  ```

如果需要通过SDK或者命令行的方式进行测试，需要重新编译`openmldb`。
了解更多编译信息参见 [compile.md](../deploy/compile.md)。

## 3. 单行函数开发
### 3.1. 注册和配置接口

#### 3.1.1 支持单一数据类型的单行函数注册

`DefaultUdfLibrary`提供`RegisterExternal` 接口来辅助内置单行函数的注册，并初始化函数的注册名。
该方法需要指定数据类型，并且仅支持声明的数据类型。

```c++
RegisterExternal("register_func_name")
  .args<arg_type, ...>(static_cast<return_type (*)(arg_type, ...)>(v1::func_ptr))
  .return_by_arg(bool_value)
  .returns<return_type>
```

函数的配置一般包括：函数指针配置、参数类型配置，返回值配置

- 配置C++函数指针: `func_ptr`。注意，考虑代码的可读性和编译安全，需要使用static_cast将指针转为函数指针。
- 配置参数类型：`args<arg_type,...>`
- 配置返回值类型：`returns<return_type>`。一般不需要显示地指明返回类型。但如果函数结果时nullable时，需要将***return type***显示地配置***returns<Nullable<return_type>>***
- 配置返回方式：`return_by_arg()`  .
  - 当 **return_by_arg(false)** 时，结果直接通过`return`返回。 OpenMLDB 默认配置  `return_by_arg(false) `
  - 当 **return_by_arg(true)** 时，结果通过参数返回
    - 若返回类型是***non-nullable***，函数结果将通过最后一个参数返回
    - 若返回类型是**nullable**，函数结果值将通过倒数第二个参数返回，而 ***null flag*** 将通过最后一个参数返回。如果***null flag***为***true***, 那么函数结果为***null***，否则函数结果从倒数第二个参数读取

下面代码展示了注册`substring` 内置单行函数的注册代码示例（代码可以在[hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc) 查看）：
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

#### 3.1.2 支持泛型模版的内置函数注册
我们还提供了`RegisterExternalTemplate` 接口来支持泛型的内置单行函数注册，可以同时支持多种数据类型

```c++
RegisterExternalTemplate<TemplateClass>("register_func_name")
  .args_in<arg_type, ...>()
  .return_by_arg(bool_value)
```

函数的配置一般包括：函数指针配置、参数类型配置，返回值配置

- 配置函数模板：`TemplateClass`
- 配置支持的参数类型：`args_in<arg_type,...>`
- 配置返回方式：`return_by_arg()`
  - 当 **return_by_arg(false)** 时，结果直接通过`return`返回。 OpenMLDB 默认配置  `return_by_arg(false) `
  - 当 **return_by_arg(true)** 时，结果通过参数返回

下面代码展示了注册`abs` 内置单行函数的代码示例（代码可以在[hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc) 查看）：
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
泛型模版的内置单行函数开发和单一数据类型的内置单行函数开发类似，在本文档中不详细展开讨论，
本章以下内容主要针对单一数据类型的内置单行函数开发。

(sfunc_category)=

### 3.2. 单行函数开发分类

我们按内置函数的返回类型将函数基本分为三种类型：

- SQL函数返回值为布尔或数值类型
- SQL函数返回值为**STRING**, **TIMESTAMP**,**DATE**, **ArrayRef**
- SQL函数的返回值可能为空

我们将详细介绍这几种类型的函数的开发和注册的基本流程。

(return_bool)=

#### 3.2.1 SQL函数返回值为布尔或数值类型

当SQL函数返回值为布尔或数值类型（如**BOOL**, **SMALLINT**, **INT**, **BIGINT**, **FLOAT**, **DOUBLE**）时，一般地，C++的函数返回值为与之对应的C++类型（`bool`, `int16_t`, `int32_t`, `int64_t`, `float`, `double`)。可以将函数设计为如下的样子：

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

同时，在[hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc) 中注册和配置函数：

```c++
# hybridse/src/udf/default_udf_library.cc
RegisterExternal("my_func")
        .args<Arg1, Arg2, ...>(static_cast<R (*)(Arg1, Arg2, ...)>(v1::func))
        .doc(R"(
            documenting my_func
        )");
```

#### 3.2.2 SQL函数返回值为STRING,TIMESTAMP或DATE

当SQL函数返回值为**STRING**, **TIMESTAMP**或**DATE**时，要求C++函数通过参数来输出结果。这意味着，普通参数后额外有一个指针类型的参数（`StringRef*`, `Timestamp*`或 `Date*`)用来存放和返回结果值。

具体地，我们可以将函数设计为如下的样子：

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

同时，在[hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc) 中注册和配置函数。请注意：由于函数结果通过参数输出，所以要配置`return_by_arg(true)`

```c++
# hybridse/src/udf/default_udf_library.cc
RegisterExternal("my_func")
        .args<Arg1, Arg2, ...>(static_cast<R (*)(Arg1, Arg2, ..., Ret*)>(v1::func))
        .return_by_arg(true)
        .doc(R"(
            documenting my_func
        )");
```

#### 3.2.3 SQL函数的返回值类型是Nullable

当SQL函数的返回值可能为空(**Nullable**)时，额外要有一个`bool*`类型的参数来存放结果为空与否。

具体地，我们可以将函数设计为如下的样子：

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

同时，在[hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc) 中注册和配置函数：

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

#### 3.2.4 SQL函数参数是Nullable

OpenMLDB对函数的NULL参数有默认的处理机制。即，任意一个参数为NULL时，函数返回NULL。但如果开发者想要在函数中获得参数是否为NULL并特别处理NULL参数时，我们需要把参数NULL的信息传递到函数中去。

一般地做法是将这个参数配置为`Nullable`，并且在C++函数的对应参数后面加一个`bool`参数才存放参数值是否为空的信息。

具体地，我们可以将函数设计为如下的样子：

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

同时，在[hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc) 中注册和配置函数：

```c++
# hybridse/src/udf/default_udf_library.cc
RegisterExternal("my_func")
        .args<Arg1, Nulable<Arg2>, ...>(static_cast<R (*)(Arg1, Arg2, bool, ...)>(v1::func))()
        .doc(R"(
            documenting my_func
        )");
```

### 3.3. SQL函数开发示例

#### 3.3.1 SQL函数返回值为布尔或数值类型 - `INT Month(TIMESTAMP)`函数

Month()函数接受一个**TIMESTAMP**的时间戳参数，返回一个**INT**整数值，它表示指定日期的月份。参考[3.2.1 SQL函数返回值为布尔或数值类型](return_bool)

##### **step 1: 实现待注册的内置函数**

在[hybridse/src/udf/udf.h](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.h) 声明`month()`函数：

```c++
# hybridse/src/udf/udf.h
namespace hybridse {
  namespace udf{
    namespace v1 {
      int32_t month(int64_t ts);
      int32_t month(Timestamp *ts);
    } // namespace v1
  } // namespace udf
} // namepsace hybridse
```

在[hybridse/src/udf/udf.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.cc) 中实现`month()`函数:

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
      int32_t month(Timestamp *ts) { return month(ts->ts_); }
    } // namespace v1
  } // namespace udf
} // namepsace hybridse
```

##### **step 2: 配置函数，并注册到默认函数库中**

在[hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc) 中注册和配置函数：

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

##### **step 3: 函数单元测试**

在[src/codegen/udf_ir_builder_test.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/codegen/udf_ir_builder_test.cc) 中添加`TEST_F`单测，并[编译和运行单元测试](compile_ut)。

```c++
// month(timestamp) normal check
TEST_F(UdfIRBuilderTest, month_timestamp_udf_test) {
    Timestamp time(1589958000000L);
    CheckUdf<int32_t, Timestamp>("month", 5, time);
}
```

SQL函数开发完成后，重新编译`openmldb`，可在SQL语句调用month函数（注意：函数名大小写不敏感）：

```SQL
select MONTH(TIMESTAMP(1590115420000)) as m1, month(timestamp(1590115420000)) as m2;
 ---- ---- 
  m1   m2  
 ---- ---- 
  5    5  
 ---- ---- 
```

#### 3.3.2 SQL函数返回值为STRING,TIMESTAMP或DATE - `STRING String(BOOL)`函数

`STRING String(BOOL)`函数接受一个**BOOL**参数，并将BOOL值转为**STRING**类型的值输出。详情参考[3.2.2 SQL函数返回值为**STRING**, **TIMESTAMP**或**DATE**](#3.2.2-SQL函数返回值为**STRING**, **TIMESTAMP**或**DATE**)

##### **step 1: 实现待注册的内置函数**

在[hybridse/src/udf/udf.h](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.h) 声明**bool_to_string()** 函数：

```c++
# hybridse/src/udf/udf.h
namespace hybridse {
  namespace udf{
    namespace v1 {
      void bool_to_string(bool v, StringRef *output);
    } // namespace v1
  } // namespace udf
} // namepsace hybridse
```

在[hybridse/src/udf/udf.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.cc) 中实现**bool_to_string()** 函数:

```c++
# hybridse/src/udf/udf.cc
namespace hybridse {
  namespace udf {
    namespace v1 {
        void bool_to_string(bool v, StringRef *output) {
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

请注意，我们使用`AllocManagedStringBuf`向OpenMLDB的内存池申请空间，而不使用`new`操作符或者`malloc`函数。

##### **step 2: 配置参数，返回值并注册函数**

`STRING String(BOOL)`是类型转换函数，建议在[hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc) 的`DefaultUdfLibrary::InitTypeUdf()`方法中注册和配置函数。

```c++
namespace hybridse {
	namespace udf {
    void DefaultUdfLibrary::InitTypeUdf() {
      // ...    
        RegisterExternal("string")
          .args<bool>(static_cast<void (*)(bool, StringRef*)>(
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

##### **step 3: 函数单元测试**

在[src/codegen/udf_ir_builder_test.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/codegen/udf_ir_builder_test.cc) 中添加`TEST_F`单测，并[编译和运行单元测试](compile_ut)。

```c++
// string(bool) normal check
TEST_F(UdfIRBuilderTest, bool_to_string_test) {
    Timestamp time(1589958000000L);
    CheckUdf<StringRef, bool>("string", "true", true);
  	CheckUdf<StringRef, bool>("string", "false", false);
}
```

SQL函数开发完成后，重新编译`openmldb`，可通过SQL调用String()函数（注：函数名大小写不敏感）：

```SQL
select STRING(true) as str_true, string(false) as str_false;
 ----------  ---------- 
  str_true   str_false  
 ----------  ---------- 
   true        true
 ----------  ---------- 
```

#### 3.3.3 SQL函数的返回值类型是Nullable - `DATE Date(TIMESTAMP)`函数

`DATE Date(TIMESTAMP)`函数接受一个`TIMESTAMP`参数，并将转成`DATE`类型输出。

参考[#3.2.3 SQL函数的返回值类型是Nullable ](#323-sql函数的返回值类型是nullable)
和[#3.2.2 SQL函数返回值为STRING,TIMESTAMP或DATE](#3.2.2-SQL函数返回值为STRING,TIMESTAMP或DATE)

##### **step 1: 实现待注册的内置函数**

由于OpenMLDB的`date`类型是一个结构体类型，设计函数是，不直接返回结果，而是将结果存放在参数中返回。同时，考虑到日期转换可能有异常或失败，返回结果是***nullable***的。因此，我们额外增加一个***is_null***参数保存结果是否为空。

在[hybridse/src/udf/udf.h](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.h) 声明`timestamp_to_date()`函数：

```c++
# hybridse/src/udf/udf.h
namespace hybridse {
  namespace udf{
    namespace v1 {
      void timestamp_to_date(Timestamp *timestamp,
            Date *ret /*result output*/, bool *is_null /*null flag*/);
    } // namespace v1
  } // namespace udf
} // namespace hybridse
```

在[hybridse/src/udf/udf.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.cc) 中实现`timestamp_to_date()`函数:

```c++
# hybridse/src/udf/udf.cc
namespace hybridse {
  namespace udf {
    namespace v1 {
        void timestamp_to_date(Timestamp *timestamp,
              Date *ret /*result output*/, bool *is_null /*null flag*/) {
          time_t time = (timestamp->ts_ + TZ_OFFSET) / 1000;
          struct tm t;
          if (nullptr == gmtime_r(&time, &t)) {
              *is_null = true;
              return;
          }
          *ret = Date(t.tm_year + 1900, t.tm_mon + 1, t.tm_mday);
          *is_null = false;
          return;
        }
    } // namespace v1
  } // namespace udf
} // namespace hybridse
```

##### **step 2: 配置参数，返回值并注册函数**

函数名和函数参数的配置和普通函数配置一样。但需要额外注意返回值类型的配置：

- 因为函数结果存放在参数中返回，所以配置`return_by_arg(true)`
- 因为函数结果可能为null所以配置`.returns<Nullable<Date>>`

因为`DATE Date(TIMESTAMP)`函数是时间和日期函数，所以建议在[hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc) 的`DefaultUdfLibrary::InitTimeAndDateUdf()`方法中注册和配置函数：

```c++
namespace hybridse {
  namespace udf {
    void DefaultUdfLibrary::InitTimeAndDateUdf() {
      // ...    
        RegisterExternal("date")
          .args<Timestamp>(
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

##### **step 3: 函数单元测试**

在[src/codegen/udf_ir_builder_test.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/codegen/udf_ir_builder_test.cc) 中添加`TEST_F`单测，并[编译和运行单元测试](compile_ut)。

```c++
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

SQL函数开发完成后，重新编译`openmldb`，可通过SQL调用Date()函数（注：函数名大小写不敏感）：

```sql
select date(timestamp(1590115420000)) as dt;
 ----------
     dt
 ------------
  2020-05-22 
 ------------
```

## 4. 聚合函数开发

### 4.1. 注册和配置接口

#### 4.1.1 支持单一数据类型的聚合函数注册

`DefaultUdfLibrary`提供`RegisterUdaf` 接口来辅助内置的聚合函数的注册，并初始化函数的注册名。
该方法需要指定数据类型，并且仅支持声明的数据类型。

```c++
RegisterUdaf("register_func_name")
  .templates<OUT, ST, IN, ...>()
  .init("init_func_name", init_func_ptr, return_by_arg=false)
  .update("update_func_name", update_func_ptr, return_by_arg=false)
  .output("output_func_name", output_func_ptr, return_by_arg=false)
```

和单行函数注册不同的是，聚合函数需要注册三个函数`init`, `update`, `output`，分别代码聚合函数初始化、
中间状态更新和最终结果输出。
函数的配置如下:

- 配置参数类型：
  - `OUT`：输出参数类型
  - `ST`：中间状态类型
  - `IN, ...`：输入参数类型
- 配置`init`函数指针: `init_func_ptr`，函数签名为`ST* Init()`
- 配置`update`函数指针: `update_func_ptr`，
  - 如果输入为非空，函数签名为`ST* Update(ST* state, IN val1, ...)`
  - 如果需要判断输入是否为**Nullable**，可以将这个参数配置为`Nullable`，并且在函数的对应参数后面加一个`bool`参数存放参数值是否为空的信息。
    函数签名为：`ST* Update(ST* state, IN val1, bool val1_is_null, ...)`
- 配置`output`函数指针: `output_func_ptr`。
  当函数的返回值可能为空时，额外要有一个`bool*`类型的参数来存放结果是否为空
  （可以参照[3.2.3 SQL函数的返回值类型是Nullable](#3.2.3-SQL函数的返回值类型是Nullable)）。


下面代码展示了新增`second` 聚合函数的代码示例，`second`功能为返回聚合数据中非空的第二个元素；为了方便展示，示例中`second`仅支持`int32_t`数据类型：
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

#### 4.1.2 支持泛型的聚合函数注册
我们还提供`RegisterUdafTemplate` 接口来注册一个支持泛型的聚合函数。

```c++
RegisterUdafTemplate<TemplateClass>("register_func_name")
  .args_in<arg_type, ...>()
```

- 配置聚合函数模板：`TemplateClass`
- 配置支持的所有参数类型：`args_in<arg_type, ...>`

下面代码展示了注册`distinct_count` 聚合函数的代码示例（代码可以在[hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc) 查看）：
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

## 5. 示例代码参考

### 5.1. 单行函数示例代码参考
更多单行函数示例代码可以参考：
[hybridse/src/udf/udf.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.cc)

### 5.2. 聚合函数示例代码参考
更多聚合函数示例代码可以参考：
[hybridse/src/udf/default_udf_library.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/default_udf_library.cc)



## 6. 文档管理

内置函数文档可在 [Built-in Functions](https://openmldb.ai/docs/zh/main/openmldb_sql/functions_and_operators/Files/udfs_8h.html) 查看，它是一个代码生成的 markdown 文件，注意请不要进行直接编辑。

- 如果需要对新增加的函数添加文档，请参照 2.2.4 配置函数文档 章节，说明了内置函数的文档是在 CPP 源代码中管理的。后续会通过一系列步骤生成如上网页中更加可读的文档， 即`docs/*/openmldb_sql/functions_and_operators/`目录下的内容。
- 如果需要修改一个已存在函数的文档，可以在文件 `hybridse/src/udf/default_udf_library.cc` 或者 `hybridse/src/udf/default_defs/*_def.cc` 下查找到对应函数的文档说明，进行修改。

OpenMLDB 项目中创建了一个定期天级别的 GitHub Workflow 任务来定期更新这里的相关文档。因此内置函数文档相关的改动只需按照上面的步骤修改对应源代码位置的内容即可，`docs` 目录和网站的内容会随之定期更新。具体的文档生成流程可以查看源代码路径下的 [udf_doxygen](https://github.com/4paradigm/OpenMLDB/tree/main/hybridse/tools/documentation/udf_doxygen)。

