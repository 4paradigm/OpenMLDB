# OpenMLDB SQL 内置函数开发指南

## 背景

​	OpenMLDB内置了上百个SQL函数，以供数据科学家作数据分析和特征抽取。目前，我们提供聚合类函数(Aggregate Function)如 `SUM`, `AVG`, `MAX`, `MIN`, `COUNT`来支持全表聚合和窗口聚合。同时，我们还提供了单行函数(Scalar Function)如`ABS`, `SIN`, `COS`, `DATE`, `YEAR`等支持单行数据处理。

​	本文是SQL内置函数的开发入门指南，旨在指引开发者快速掌握基础的内置函数开发方法。我们诚挚欢迎更多的开发者加入社区，帮助我们扩展和开发内置函数集。

## 函数分类

OpenMLDB将函数分类两大类：聚合类函数和单行处理函数:

- 聚合函数((Aggregate Function)对数据集（如一列数据）执行计算，并返回单个值。
- 单行函数（Scalar Function)对若干数据执行计算，并返回单个值。根据函数功能，又可分为
  - 数学函数
  - 逻辑函数
  - 日期函数
  - 字符串函数

本文将介绍单行函数的开发，暂不深入更为复杂的聚合函数开发细节。

## 开发和注册内置函数

`ExternalFuncRegistryHelper` is OpenMLDB的内置函数开发最重要的辅助类。它提供一套API来配置函数，并将配置好的函数注册到相应的函数库中。完成函数注册后，可以在SQL查询语句中调用函数。本章节主要介绍内置函数的注册和配置的基本步骤和并提供若干示范。

### ExternalFuncRegistryHelper API

OpenMLDB内置的函数都应该注册到默认函数库`DefaultUdfLibrary`(src/udf/default_udf_library.cc)中。`DefaultUdfLibrary`提供`RegisterExternal` 接口来创建一个函数注册助手。它负责配置函数的参数，配置返回值了行，并注册函数到默认函数库中。

```c++
RegisterExternal(function_name)
  .args<arg_type, ...>(built_in_fn_pointer)
  .return_by_arg(bool_value)
  .returns<return_type>
  .doc(documentation)
```

- `RegisterExternal(function_name)`: 可以创建一个`ExternalFuncRegistryHelper`对象，初始化函数的注册名。SQL使用这个名字来调用函数。
- `built_in_fn_pointer`: 被注册的C++函数指针
- `args<arg_type,...>`: 配置函数参数类型
-  `returns<return_type>`: 配置函数返回值类型
-  `return_by_arg()`  : 配置结果是否通过参数返回
  - 当 **return_by_arg(false)**时 , 结果直接通过`return`返回. OpenMLDB 默认配置  `return_by_arg(false) ` .
  - 当 **return_by_arg(true)** 时,结果通过参数返回。
    - 若返回类型是***non-nullable***, 函数结果将通过最后一个参数返回。
    - 若返回类型是**nullable**, 函数结果值将通过倒数第二个参数返回，而 ***null flag*** 将通过最后一个参数返回。如果***null flag***为***true***, 那么函数结果为***null***，否则函数结果从倒数第二个参数读取。
- `doc()`: 配置函数文档

### Register built-in function `return_by_arg(false)`

一般来说，当函数的结果是一个数值或者bool值时，我们配置`return_by_arg` as `false` 。那意味着，函数结果将直接return返回。

#### step 1: 实现待注册的内置函数

一般地，开发者可以在(`src/udf/udf.h` 和  `src/udf/udf.cc`)中实现内置函数。

**示例：**

我们在`udf.cc`中实现两个`month()`函数。month()函数返回一个整数值，它表示指定日期的月份。month()函数接受一个参数，该参数可以是`timestamp`的时间戳，也可以是`bigint`的毫秒数。

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

#### step 2: 配置函数，并注册到默认函数库中

使用`ExternalFuncRegistryHelper`工具类配置参数和注册函数到默认库中。

**示例：**

我们将前一节中实现的两个c++函数注册到默认库中。

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

注册后，我们可以在SQL语句中调用month函数：

```SQL
select month(timestamp(1590115420000)) as m1,  month(1590115420000) as m2;
 ---- ---- 
  m1   m2  
 ---- ---- 
  5    5   
 ---- ---- 
```

### Register built-in function  `return_by_arg(true)`

当函数的结果是一个结构体时（如时间戳，日期，字符串），应该将结果存放在参数中返回。若结果是 ***nullable**，则需额外保留一个bool参数来存放null标志。

#### step 1: 实现待注册的内置函数

**示例：**

我们在`udf.cc`中实现两个`timestamp_to_date()`函数。timestamp_to_date接受一个`timestamp`参数，并将`timestamp`转成`date`。由于OpenMLDB的`date`类型是一个结构体类型，设计函数是，不直接返回结果，而是将结果存放在参数中返回。与此同时，考虑到日期转换可能有异常或失败，转换失败是，应返回null。所以，我们额外增加一个null flag作为最后一个参数。

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

#### step 2: 配置参数，返回值并注册函数

**示例:**

函数名和函数参数的配置和普通函数配置一样。但需要额外注意返回值类型的配置：

- 因为函数结果存放在参数中返回，所以配置`return_by_arg(true)`
- 因为函数结果可能为null,所以配置`.returns<Nullable<Date>>`

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

## 函数文档配置

`ExternalFuncRegistryHelper` 提供了`doc(doc_string)`API来配置文档。函数文档用来描述函数的功能和用法。它的主要受众是普通用户。所以，文档需要清晰易懂。

函数文档一般需要包含一下几类信息：

- **@brief** ：简要描述函数的功能
- **@param**：函数参数描述
- **Examples**： 函数的使用示例。一般需要提供SQL的查询示例。SQL语句需要放置在 a `@code/@endcode` 代码块中。
- **@since** ：指明引入该函数的OpenMLDB版本

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

### `UdfIRBuilderTest`中添加单测

一般地，可以在`src/codegen/udf_ir_builder_test.cc`中添加`TEST_F`单测。我们提供了CheckUdf函数以便开发者检验函数结果。

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

