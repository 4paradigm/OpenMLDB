# OpenMLDB SQL 内置函数开发指南

## 背景

​	OpenMLDB内置了上百个SQL函数，以供数据科学家作数据分析和特征抽取。目前，我们提供聚合类函数(Aggregate Function)如 `SUM`, `AVG`, `MAX`, `MIN`, `COUNT`来支持全表聚合和窗口聚合。同时，我们还提供了单行函数(Scalar Function)如`ABS`, `SIN`, `COS`, `DATE`, `YEAR`等支持单行数据处理。

- 聚合函数（Aggregate Function）对数据集（如一列数据）执行计算，并返回单个值。

- 单行函数（Scalar Function）对若干数据执行计算，并返回单个值。单行函数按功能不同，又可分为：
  - 数学函数
  - 逻辑函数
  - 时间和日期函数
  - 字符串函数
  - 类型转换函数

​	本文是SQL内置函数的开发入门指南，旨在指引开发者快速掌握基础的单行内置函数开发方法，暂不深入更为复杂的聚合函数开发细节。我们诚挚欢迎更多的开发者加入社区，帮助我们扩展和开发内置函数集。

## 开发和注册内置函数

本章节主要介绍内置函数的注册和配置的基本步骤和并提供若干示范。内置函数的开发包含以下两个步骤：

### 1.  开发C++内置函数

​		一般来说，开发者应需要为每一个SQL的内置函数开发一个对应的C++函数。用户通过SQL查询函数时，最终底层调用的就是这个C++函数。C++内置函数的开发需要注意一下几点：

- 代码位置：在[hybridse/src/udf/udf.h](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.h)中声明函数，并在[hybridse/src/udf/udf.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.cc)中实现函数

- 函数名：

  - SQL的函数名是大小写不敏感的。
  - **不要求**SQL函数名和C++函数名完全**一致**。函数注册后C++函数和SQL的函数会关联在一起。

- 数据类型：C++的数据类型和SQL类型的对应关系如下

  - | SQL类型   | C/C++ 类型         |
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

- 函数参数和返回值：

  - SQL参数与C++函数的参数相对位置相同

  - SQL函数返回值要考虑以下几种情况：

    - 当SQL函数返回值为布尔或数值类型（如**BOOL**, **SMALLINT**, **INT**, **BIGINT**, **FLOAT**, **DOUBLE**）时，一般地，C++的函数返回值为与之对应的C++类型（`bool`, `int16_t`, `int32_t`, `int64_t`, `float`, `double`)。

      - ```c++
        // SQL: DOUBLE FUNC_DOUBLE(INT)
        double func_return_double(int); 
        ```

    - 当SQL函数返回值为**STRING**, **TIMESTAMP**或**DATE**时，要求C++函数从参数返回结果。这意味着，普通参数后额外有一个指针类型的参数（`codec::StringRef*`, `codec::Timestamp*`或 `codec::Date*`)用来存放和返回结果值

      - ```c++
        // SQL: STRING FUNC_STR(INT)
        void func_output_str(int32_t, codec::StringRef*); 
        ```

    - 当SQL函数的返回值可能为空(**Nullable**)时，额外要有一个`bool*`类型的参数来存放结果为空与否

      - ```c++
        // SQL: Nullable<DATE> FUNC_NULLABLE_DATE(BIGINT)
        void func_output_nullable_date(int64_t, codec::Date*, bool*); 
        ```



### 2. 注册C++函数到默认函数库中

#### 默认函数库DefaultUdfLibrary的介绍

OpenMLDB的 `DefaultUdfLibrary` 负责存放和管理内置的全局SQL函数。开发者需要先将自己开发的的C++函数注册到默认函数库 `DefaultUdfLibrary` 中。而后，用户才能通过SQL语句访问这个函数。默认函数库`DefaultUdfLibrary` 声明在`DefaultUdfLibrary`的实现类里进行函数注册的操作。一般地，可按函数类型在相应的`DefaultUdfLibrary::InitXXXXUdf()` 函数里注册函数。

具体来说:

- 数学函数：在`void DefaultUdfLibrary::IniMathUdf()`中注册
- 逻辑函数：在`void DefaultUdfLibrary::InitLogicalUdf()`中注册
- 时间和日期函数：在`void DefaultUdfLibrary::InitTimeAndDateUdf()`中注册
- 字符串函数：在`void DefaultUdfLibrary::InitStringUdf()`中注册
- 类型转换函数：在`void DefaultUdfLibrary::InitTypeUdf()`中注册

#### 注册函数相关接口和配置介绍

`DefaultUdfLibrary`提供`RegisterExternal` 接口用以创建一个`ExternalFuncRegistryHelper`对象，并初始化函数的注册名。

```c++
ExternalFuncRegistryHelper helper = RegisterExternal("register_func");
// ... ignore function configuration details
```

注册成功后，用户在SQL语句中使用这个名字来调用函数。请注意，SQL中的函数大小写不敏感。

```SQL
SELECT register_func(col1) FROM t1;
```

`ExternalFuncRegistryHelper` is OpenMLDB的内置函数开发最重要的辅助类。它提供一套API来配置函数，并将配置好的函数注册到相应的函数库中。

```c++
ExternalFuncRegistryHelper helper = RegisterExternal(function_name);
helper
  .args<arg_type, ...>(built_in_fn_pointer)
  .return_by_arg(bool_value)
  .returns<return_type>
  .doc(documentation)
```

- `args<arg_type,...>`: 配置参数类型
- `built_in_fn_pointer`: C++函数指针
- `returns<return_type>`: 配置函数返回值类型。特别要注意的是，当函数结果时nullable时，需要将***return type***显示地配置***returns<Nullable<return_type>>***
-  `return_by_arg()`  : 配置结果是否通过参数返回
  - 当 **return_by_arg(false)**时 , 结果直接通过`return`返回. OpenMLDB 默认配置  `return_by_arg(false) ` 
  - 当 **return_by_arg(true)** 时,结果通过参数返回
    - 若返回类型是***non-nullable***, 函数结果将通过最后一个参数返回
    - 若返回类型是**nullable**, 函数结果值将通过倒数第二个参数返回，而 ***null flag*** 将通过最后一个参数返回。如果***null flag***为***true***, 那么函数结果为***null***，否则函数结果从倒数第二个参数读取
- `doc()`: 配置函数文档。文档配置请遵循[函数文档配置](#函数文档配置)的规范

### 场景1: 函数结果直接返回 `return_by_arg(false)`

一般来说，当SQL函数返回值为布尔或数值类型当函数的结果是一个数值(***bigint, int, smallint, float, double***) 或者***bool***值时，可以将结果直接return返回。可以将函数设计为如下的样子：

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

于此同时，注册的配置也如下：

```c++
RegisterExternal("my_func")
        .args<Arg1, Arg2, ...>(static_cast<R (*)(Arg1, Arg2, ...)>(v1::func))
  			.return_by_arg(false)
        .doc(R"(
            documenting my_func
        )");
```

由于`return_by_arg`默认就配置为`false`，`return_by_arg(false)`可以不显示配置，

```c++
RegisterExternal("my_func")
        .args<Arg1, Arg2, ...>(static_cast<R (*)(Arg1, Arg2, ...)>(v1::func))
        .doc(R"(
            documenting my_func
        )");
```

#### 例子：实现和注册`INT Month(TIMESTAMP)`函数

Month()函数接受一个**TIMESTAMP**的时间戳参数，返回一个**INT**整数值，它表示指定日期的月份。

**step 1: 实现待注册的内置函数**

在[hybridse/src/udf/udf.h](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.h)声明`month()`函数：

```c++
# hybridse/src/udf/udf.h
namespace udf{
  namespace v1 {
    int32_t month(int64_t ts);
    int32_t month(codec::Timestamp *ts);
  } // namespace v1
} // namespace udf
```

在[hybridse/src/udf/udf.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.cc)中实现`month()`函数:

```c++
# hybridse/src/udf/udf.cc
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
```

**step 2: 配置函数，并注册到默认函数库中**

使用`ExternalFuncRegistryHelper`工具类配置参数和并注册函数到默认库中:

```c++
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
```

注册后，我们可以在SQL语句中调用month函数，函数名大小写不敏感：

```SQL
select MONTH(TIMESTAMP(1590115420000)) as m1, month(timestamp(1590115420000)) as m2;
 ---- ---- 
  m1   m2  
 ---- ---- 
  5    5  
 ---- ---- 
```

### 场景2: 函数结果通过参数返回，结果不为null

当函数的结果是一个结构体时（如时间戳，日期，字符串），应该将结果存放在参数中返回。具体地，我们可以将函数设计为如下的样子：

```c++
# hybridse/src/udf/udf.h
namespace udf {
  namespace v1 {
    void func(Arg1 arg1, Arg2 arg2, ..., Ret* result);
  }
}

```

```c++
# hybridse/src/udf/udf.cc
namespace udf {
  namespace v1 {
    void func(Arg1 arg1, Arg2 arg2, ..., Ret* ret) {
      // ...
      // *ret = result value
    }
  }
}
```

于此同时，注册函数的配置如下：

```c++
RegisterExternal("my_func")
        .args<Arg1, Arg2, ...>(static_cast<R (*)(Arg1, Arg2, ...)>(v1::func))
  			.return_by_arg(true)
        .doc(R"(
            documenting my_func
        )");
```

#### Example：实现和注册`STRING String(BOOL)`函数

`STRING String(BOOL)`函数接受一个**BOOL**参数，并将BOOL值转为**STRING**类型的值输出。

**step 1: 实现待注册的内置函数**

由于OpenMLDB的`STRING`类型是一个结构体类型，对应的C++函数并不直接RETURN结果，而是将结果存放在`code::Date*`参数中返回。

在[hybridse/src/udf/udf.h](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.h)声明**bool_to_string()**)函数：

```c++
# hybridse/src/udf/udf.h
namespace udf{
  namespace v1 {
    void bool_to_string(bool v, hybridse::codec::StringRef *output);
  } // namespace v1
} // namespace udf
```

在[hybridse/src/udf/udf.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.cc)中实现**bool_to_string()**函数:

```c++
# hybridse/src/udf/udf.cc
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
```

**step 2: 配置参数，返回值并注册函数**

使用`ExternalFuncRegistryHelper`工具类配置参数和并注册函数到默认库中。需要额外注意返回值类型的配置：

- 因为函数结果存放在参数中返回，所以配置`return_by_arg(true)`

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

注册后，我们可以在SQL语句中调用string函数，函数名大小写不敏感：

```SQL
select STRING(true) as str_true, string(false) as str_false;
 ----------  ---------- 
  str_true   str_false  
 ----------  ---------- 
   true        true
 ----------  ---------- 
```

### 场景3: 函数结果可能为空，并且结果通过参数返回

当函数的结果是一个结构体时（如时间戳，日期，字符串），应该将结果存放在参数中返回。因结果是 ***nullable***，所以需额外保留一个***bool***参数来存放null标志位。

具体地，我们可以将函数设计为如下的样子：

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

于此同时，注册的配置也如下：

```c++
RegisterExternal("my_func")
        .args<Arg1, Arg2, ...>(static_cast<R (*)(Arg1, Arg2, ...)>(v1::func))
  			.return_by_arg(true)
  			.returns<Nullable<Ret>>()
        .doc(R"(
            documenting my_func
        )");
```

#### Example：实现和注册`DATE Date(TIMESTAMP)`函数

`DATE Date(TIMESTAMP)`函数接受一个`TIMESTAMP`参数，并将转成`DATE`类型输出。

**step 1: 实现待注册的内置函数**

由于OpenMLDB的`date`类型是一个结构体类型，设计函数是，不直接返回结果，而是将结果存放在参数中返回。同时，考虑到日期转换可能有异常或失败，返回结果是***nullable***的。因此，我们额外增加一个***is_null***参数保存结果是否为空。

在[hybridse/src/udf/udf.h](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.h)声明`timestamp_to_date()`函数：

```c++
# hybridse/src/udf/udf.h
namespace udf{
  namespace v1 {
    void timestamp_to_date(codec::Timestamp *timestamp,
      		codec::Date *ret /*result output*/, bool *is_null /*null flag*/);
  } // namespace v1
} // namespace udf
```

在[hybridse/src/udf/udf.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/udf/udf.cc)中实现`timestamp_to_date()`函数:

```c++
# hybridse/src/udf/udf.cc
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
```

**step 2: 配置参数，返回值并注册函数**

函数名和函数参数的配置和普通函数配置一样。但需要额外注意返回值类型的配置：

- 因为函数结果存放在参数中返回，所以配置`return_by_arg(true)`
- 因为函数结果可能为null,所以配置`.returns<Nullable<Date>>`

```c++
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
```

###  注册函数别名

一个函数可以有多个别名。如SUBSTR()函数和SUBSTRING()函数本质上是同一函数；CEILING()函数和CEIL()函数也是相同功能的函数。这种情况下，不需要对每一个函数实现一套C++函数并注册。只需要在`DefaultUdfLibrary`中为已经注册好的函数关联别名即可。

```c++
// substring() is registered into default library already 
RegisterAlias("substr", "substring");
// ceil() is registered into default library already 
RegisterAlias("ceiling", "ceil");
```

## 函数文档配置

`ExternalFuncRegistryHelper` 提供了`doc(doc_string)`API来配置文档。函数文档用来描述函数的功能和用法。它的主要受众是普通用户。所以，文档需要清晰易懂。

函数文档一般需要包含一下几类信息：

- **@brief** ：简要描述函数的功能
- **@param**：函数参数描述
- **Examples**： 函数的使用示例。一般需要提供SQL的查询示例。SQL语句需要放置在 `@code/@endcode` 代码块中。
- **@since** ：指明引入该函数的OpenMLDB版本。OpenMLDB版本可以从项目的 [CMakeList.txt](https://github.com/4paradigm/OpenMLDB/blob/main/CMakeLists.txt)获得:` ${OPENMLDB_VERSION_MAJOR}.${OPENMLDB_VERSION_MINOR}.${OPENMLDB_VERSION_BUG}`

**示例:**

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

## 函数测试

函数开发完成后，开发者还需要添加相对应的测试例以确保系统运行正确。

### `UdfIRBuilderTest`中添加单测（必要）

一般地，可以在[src/codegen/udf_ir_builder_test.cc](https://github.com/4paradigm/OpenMLDB/blob/main/hybridse/src/codegen/udf_ir_builder_test.cc)中添加`TEST_F`单测。我们提供了CheckUdf函数以便开发者检验函数结果。

```c++
CheckUdf<return_type, arg_type,...>("function_name", expect_result, arg_value,...);
```

对每一个函数（每一种参数组合），我们至少需要实现:

- 添加一个单测，验证普通结果
- 如果结果是***nullable***,需要添加一个单测，验证结果为空

**示例:**

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

编译和执行单测

```bash
cd ./hybridse
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DTESTING_ENABLE=ON
make udf_ir_builder_test -j4
SQL_CASE_BASE_DIR=${OPENMLDB_DIR} ./src/codegen/udf_ir_builder_test
```

### 可以添加集成测试Case（可选）

开发者可以在[cases/query/udf_query.yaml](https://github.com/4paradigm/OpenMLDB/blob/main/cases/query/udf_query.yaml)中添加yaml格式的case来测试新注册的函数：

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

编译和执行yaml case:

```bash
cd ./hybridse
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DTESTING_ENABLE=ON -DEXAMPLES_ENABLE=ON
make toydb_engine_test -j4
SQL_CASE_BASE_DIR=${OPENMLDB_DIR} ./examples/toydb/src/testing/toydb_engine_test --gtest_filter=EngineUdfQuery*
```



