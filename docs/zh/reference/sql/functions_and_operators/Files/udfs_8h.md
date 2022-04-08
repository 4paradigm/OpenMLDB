## Built-in Functions

### function abs

```cpp
abs()
```

**Description**:

Return the absolute value of expr. 

**Parameters**: 

  * **expr** 


**Since**:
0.1.0


Example:



```cpp
SELECT ABS(-32);
-- output 32
```




**Supported Types**:

* [`bool`]
* [`number`] 

### function acos

```cpp
acos()
```

**Description**:

Return the arc cosine of expr. 

**Parameters**: 

  * **expr** 


**Since**:
0.1.0


Example:



```cpp
SELECT ACOS(1);
-- output 0
```




**Supported Types**:

* [`number`] 

### function add

```cpp
add()
```

**Description**:

Compute sum of two arguments. 

**Since**:
0.1.0


Example:



```cpp
select add(1, 2);
-- output 3
```




**Supported Types**:

* [`bool`, `bool`]
* [`bool`, `number`]
* [`bool`, `timestamp`]
* [`int16`, `timestamp`]
* [`int32`, `timestamp`]
* [`int64`, `timestamp`]
* [`number`, `bool`]
* [`number`, `number`]
* [`timestamp`, `bool`]
* [`timestamp`, `int16`]
* [`timestamp`, `int32`]
* [`timestamp`, `int64`]
* [`timestamp`, `timestamp`] 

### function asin

```cpp
asin()
```

**Description**:

Return the arc sine of expr. 

**Parameters**: 

  * **expr** 


**Since**:
0.1.0


Example:



```cpp
SELECT ASIN(0.0);
-- output 0.000000
```




**Supported Types**:

* [`number`] 

### function at

```cpp
at()
```

**Description**:

Returns the value of expression from the offset-th row of the ordered partition. 

**Parameters**: 

  * **offset** The number of rows forward from the current row from which to obtain the value.



Example:


| value    |
|  -------- |
| 0    |
| 1    |
| 2    |
| 3    |
| 4    |




```cpp
SELECT at(value, 3) OVER w;
-- output 3
```



**Supported Types**:

* [`list<bool>`, `int64`]
* [`list<date>`, `int64`]
* [`list<number>`, `int64`]
* [`list<string>`, `int64`]
* [`list<timestamp>`, `int64`] 

### function atan

```cpp
atan()
```

**Description**:

Return the arc tangent of expr If called with one parameter, this function returns the arc tangent of expr. If called with two parameters X and Y, this function returns the arc tangent of Y / X. 

**Parameters**: 

  * **X** 
  * **Y** 


**Since**:
0.1.0


Example:



```cpp
SELECT ATAN(-0.0);  
-- output -0.000000

SELECT ATAN(0, -0);
-- output 3.141593
```




**Supported Types**:

* [`bool`, `bool`]
* [`bool`, `number`]
* [`number`]
* [`number`, `bool`]
* [`number`, `number`] 

### function atan2

```cpp
atan2()
```

**Description**:

Return the arc tangent of Y / X.. 

**Parameters**: 

  * **X** 
  * **Y** 


**Since**:
0.1.0


Example:



```cpp
SELECT ATAN2(0, -0);
-- output 3.141593
```




**Supported Types**:

* [`bool`, `bool`]
* [`bool`, `number`]
* [`number`, `bool`]
* [`number`, `number`] 

### function avg

```cpp
avg()
```

**Description**:

Compute average of values. 

**Parameters**: 

  * **value** Specify value column to aggregate on.


**Since**:
0.1.0



Example:


| value    |
|  -------- |
| 0    |
| 1    |
| 2    |
| 3    |
| 4    |




```cpp
SELECT avg(value) OVER w;
-- output 2
```




**Supported Types**:

* [`list<number>`] 

### function avg_cate

```cpp
avg_cate()
```

**Description**:

Compute average of values grouped by category key and output string. Each group is represented as 'K:V' and separated by comma in outputs and are sorted by key in ascend order. 

**Parameters**: 

  * **value** Specify value column to aggregate on. 
  * **catagory** Specify catagory column to group by.



Example:


| value   | catagory    |
|  -------- | -------- |
| 0   | x    |
| 1   | y    |
| 2   | x    |
| 3   | y    |
| 4   | x    |




```cpp
SELECT avg_cate(value, catagory) OVER w;
-- output "x:2,y:2"
```



**Supported Types**:

* [`list<number>`, `list<date>`]
* [`list<number>`, `list<int16>`]
* [`list<number>`, `list<int32>`]
* [`list<number>`, `list<int64>`]
* [`list<number>`, `list<string>`]
* [`list<number>`, `list<timestamp>`] 

### function avg_cate_where

```cpp
avg_cate_where()
```

**Description**:

Compute average of values matching specified condition grouped by category key and output string. Each group is represented as 'K:V' and separated by comma in outputs and are sorted by key in ascend order. 

**Parameters**: 

  * **value** Specify value column to aggregate on. 
  * **condition** Specify condition column.
  * **catagory** Specify catagory column to group by. 



Example:


| value   | condition   | catagory    |
|  -------- | -------- | -------- |
| 0   | true   | x    |
| 1   | false   | y    |
| 2   | false   | x    |
| 3   | true   | y    |
| 4   | true   | x    |




```cpp
SELECT avg_cate_where(value, condition, catagory) OVER w;
-- output "x:2,y:3"
```



**Supported Types**:

* [`list<number>`, `list<bool>`, `list<date>`]
* [`list<number>`, `list<bool>`, `list<int16>`]
* [`list<number>`, `list<bool>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<string>`]
* [`list<number>`, `list<bool>`, `list<timestamp>`] 

### function avg_where

```cpp
avg_where()
```

**Description**:

Compute average of values match specified condition. 

**Parameters**: 

  * **value** Specify value column to aggregate on. 
  * **condition** Specify condition column.


**Since**:
0.1.0



Example:


| value    |
|  -------- |
| 0    |
| 1    |
| 2    |
| 3    |
| 4    |




```cpp
SELECT avg_where(value, value > 2) OVER w;
-- output 3.5
```




**Supported Types**:

* [`list<number>`, `list<bool>`] 

### function bool

```cpp
bool()
```

**Description**:

Cast string expression to bool. 

**Since**:
0.1.0


Example:



```cpp
select bool("true");
-- output true
```




**Supported Types**:

* [`string`] 

### function ceil

```cpp
ceil()
```

**Description**:

Return the smallest integer value not less than the expr. 

**Parameters**: 

  * **expr** 


**Since**:
0.1.0


Example:



```cpp
SELECT CEIL(1.23);
-- output 2
```




**Supported Types**:

* [`bool`]
* [`number`] 

### function ceiling

```cpp
ceiling()
```

**Description**:

Return the smallest integer value not less than the expr. 

**Parameters**: 

  * **expr** 


**Since**:
0.1.0


Example:



```cpp
SELECT CEIL(1.23);
-- output 2
```




**Supported Types**:

* [`bool`]
* [`number`] 

### function concat

```cpp
concat()
```

**Description**:

This function returns a string resulting from the joining of two or more string values in an end-to-end manner. (To add a separating value during joining, see concat_ws.) 

**Since**:
0.1.0


Example:



```cpp
select concat("1", 2, 3, 4, 5.6, 7.8, Timestamp(1590115420000L));
-- output "12345.67.82020-05-22 10:43:40"
```




**Supported Types**:

* [...] 

### function concat_ws

```cpp
concat_ws()
```

**Description**:

Returns a string resulting from the joining of two or more string value in an end-to-end manner. It separates those concatenated string values with the delimiter specified in the first function argument. 

**Since**:
0.1.0


Example:



```cpp
select concat("-", "1", 2, 3, 4, 5.6, 7.8, Timestamp(1590115420000L));
-- output "1-2-3-4-5.6-7.8-2020-05-22 10:43:40"
```




**Supported Types**:

* [`bool`, ...]
* [`date`, ...]
* [`number`, ...]
* [`string`, ...]
* [`timestamp`, ...] 

### function cos

```cpp
cos()
```

**Description**:

Return the cosine of expr. 

**Parameters**: 

  * **expr** It is a single argument in radians.


**Since**:
0.1.0


Example:



```cpp
SELECT COS(0);
-- output 1.000000
```





* The value returned by [cos()](/reference/sql/functions_and_operators/Files/udfs_8h.md#function-cos) is always in the range: -1 to 1.

**Supported Types**:

* [`number`] 

### function cot

```cpp
cot()
```

**Description**:

Return the cotangent of expr. 

**Parameters**: 

  * **expr** 


**Since**:
0.1.0


Example:



```cpp
SELECT COT(1);  
-- output 0.6420926159343306
```




**Supported Types**:

* [`number`] 

### function count

```cpp
count()
```

**Description**:

Compute number of values. 

**Parameters**: 

  * **value** Specify value column to aggregate on.


**Since**:
0.1.0



Example:


| value    |
|  -------- |
| 0    |
| 1    |
| 2    |
| 3    |
| 4    |




```cpp
SELECT count(value) OVER w;
-- output 5
```




**Supported Types**:

* [`list<bool>`]
* [`list<date>`]
* [`list<number>`]
* [`list<row>`]
* [`list<string>`]
* [`list<timestamp>`] 

### function count_cate

```cpp
count_cate()
```

**Description**:

Compute count of values grouped by category key and output string. Each group is represented as 'K:V' and separated by comma in outputs and are sorted by key in ascend order. 

**Parameters**: 

  * **value** Specify value column to aggregate on. 
  * **catagory** Specify catagory column to group by.



Example:


| value   | catagory    |
|  -------- | -------- |
| 0   | x    |
| 1   | y    |
| 2   | x    |
| 3   | y    |
| 4   | x    |




```cpp
SELECT count_cate(value, catagory) OVER w;
-- output "x:3,y:2"
```



**Supported Types**:

* [`list<number>`, `list<date>`]
* [`list<number>`, `list<int16>`]
* [`list<number>`, `list<int32>`]
* [`list<number>`, `list<int64>`]
* [`list<number>`, `list<string>`]
* [`list<number>`, `list<timestamp>`] 

### function count_cate_where

```cpp
count_cate_where()
```

**Description**:

Compute count of values matching specified condition grouped by category key and output string. Each group is represented as 'K:V' and separated by comma in outputs and are sorted by key in ascend order. 

**Parameters**: 

  * **value** Specify value column to aggregate on. 
  * **condition** Specify condition column.
  * **catagory** Specify catagory column to group by. 



Example:


| value   | condition   | catagory    |
|  -------- | -------- | -------- |
| 0   | true   | x    |
| 1   | false   | y    |
| 2   | false   | x    |
| 3   | true   | y    |
| 4   | true   | x    |




```cpp
SELECT count_cate_where(value, condition, catagory) OVER w;
-- output "x:2,y:1"
```



**Supported Types**:

* [`list<number>`, `list<bool>`, `list<date>`]
* [`list<number>`, `list<bool>`, `list<int16>`]
* [`list<number>`, `list<bool>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<string>`]
* [`list<number>`, `list<bool>`, `list<timestamp>`] 

### function count_where

```cpp
count_where()
```

**Description**:

Compute number of values match specified condition. 

**Parameters**: 

  * **value** Specify value column to aggregate on. 
  * **condition** Specify condition column.


**Since**:
0.1.0



Example:


| value    |
|  -------- |
| 0    |
| 1    |
| 2    |
| 3    |
| 4    |




```cpp
SELECT count_where(value, value > 2) OVER w;
-- output 2
```




**Supported Types**:

* [`list<date>`, `list<bool>`]
* [`list<number>`, `list<bool>`]
* [`list<string>`, `list<bool>`]
* [`list<timestamp>`, `list<bool>`] 

### function date

```cpp
date()
```

**Description**:

Cast timestamp or string expression to date. 

**Since**:
0.1.0


Example:



```cpp
select date(timestamp(1590115420000));
-- output 2020-05-22
select date("2020-05-22");
-- output 2020-05-22
```




**Supported Types**:

* [`string`]
* [`timestamp`] 

### function date_format

```cpp
date_format()
```

**Description**:

Formats the datetime value according to the format string. 

Example:



```cpp
select date_format(timestamp(1590115420000),"%Y-%m-%d %H:%M:%S");
--output "2020-05-22 10:43:40"
```



**Supported Types**:

* [`date`, `string`]
* [`timestamp`, `string`] 

### function day

```cpp
day()
```

**Description**:

Return the day of the month for a timestamp or date. 

**Since**:
0.1.0


Note: This function equals the `[day()](/reference/sql/functions_and_operators/Files/udfs_8h.md#function-day)` function.

Example: 

```cpp
select dayofmonth(timestamp(1590115420000));
-- output 22

select day(timestamp(1590115420000));
-- output 22
```




**Supported Types**:

* [`date`]
* [`int64`]
* [`timestamp`] 

### function dayofmonth

```cpp
dayofmonth()
```

**Description**:

Return the day of the month for a timestamp or date. 

**Since**:
0.1.0


Note: This function equals the `[day()](/reference/sql/functions_and_operators/Files/udfs_8h.md#function-day)` function.

Example: 

```cpp
select dayofmonth(timestamp(1590115420000));
-- output 22

select day(timestamp(1590115420000));
-- output 22
```




**Supported Types**:

* [`date`]
* [`int64`]
* [`timestamp`] 

### function dayofweek

```cpp
dayofweek()
```

**Description**:

Return the day of week for a timestamp or date. 

**Since**:
0.4.0


Note: This function equals the `[week()](/reference/sql/functions_and_operators/Files/udfs_8h.md#function-week)` function.

Example: 

```cpp
select dayofweek(timestamp(1590115420000));
-- output 6
```




**Supported Types**:

* [`date`]
* [`int64`]
* [`timestamp`] 

### function dayofyear

```cpp
dayofyear()
```

**Description**:

Return the day of year for a timestamp or date. Returns 0 given an invalid date. 

**Since**:
0.1.0


Example: 

```cpp
select dayofyear(timestamp(1590115420000));
-- output 143

select dayofyear(1590115420000);
-- output 143

select dayofyear(date("2020-05-22"));
-- output 143

select dayofyear(date("2020-05-32"));
-- output 0
```




**Supported Types**:

* [`date`]
* [`int64`]
* [`timestamp`] 

### function distinct_count

```cpp
distinct_count()
```

**Description**:

Compute number of distinct values. 

**Parameters**: 

  * **value** Specify value column to aggregate on.


**Since**:
0.1.0



Example:


| value    |
|  -------- |
| 0    |
| 0    |
| 2    |
| 2    |
| 4    |




```cpp
SELECT distinct_count(value) OVER w;
-- output 3
```




**Supported Types**:

* [`list<bool>`]
* [`list<date>`]
* [`list<number>`]
* [`list<string>`]
* [`list<timestamp>`] 

### function double

```cpp
double()
```

**Description**:

Cast string expression to double. 

**Since**:
0.1.0


Example:



```cpp
select double("1.23");
-- output 1.23
```




**Supported Types**:

* [`string`] 

### function exp

```cpp
exp()
```

**Description**:

Return the value of e (the base of natural logarithms) raised to the power of expr. 

**Parameters**: 

  * **expr** 


**Since**:
0.1.0




```cpp
SELECT EXP(0);  
-- output 1
```




**Supported Types**:

* [`number`] 

### function first_value

```cpp
first_value()
```

**Description**:

Returns the value of expr from the first row of the window frame. 



```
    @since 0.1.0
```

**Supported Types**: 

### function float

```cpp
float()
```

**Description**:

Cast string expression to float. 

**Since**:
0.1.0


Example:



```cpp
select float("1.23");
-- output 1.23
```




**Supported Types**:

* [`string`] 

### function floor

```cpp
floor()
```

**Description**:

Return the largest integer value not less than the expr. 

**Parameters**: 

  * **expr** 


**Since**:
0.1.0


Example:



```cpp
SELECT FLOOR(1.23);
-- output 1
```




**Supported Types**:

* [`bool`]
* [`number`] 

### function fz_join

```cpp
fz_join()
```

**Description**:

Used by feature zero, for each string value from specified column of window, join by delimeter. Null values are skipped. 

**Since**:
0.1.0


Example:



```cpp
select fz_join(fz_split("k1:v1,k2:v2", ","), " ");
--  "k1:v1 k2:v2"
```




**Supported Types**:

* [`list<string>`, `string`] 

### function fz_split

```cpp
fz_split()
```

**Description**:

Used by feature zero, split string to list by delimeter. Null values are skipped. 

**Since**:
0.1.0



**Supported Types**:

* [`string`, `string`] 

### function fz_split_by_key

```cpp
fz_split_by_key()
```

**Description**:

Used by feature zero, split string by delimeter and then split each segment as kv pair, then add each key to output list. Null and illegal segments are skipped. 

**Since**:
0.1.0



**Supported Types**:

* [`string`, `string`, `string`] 

### function fz_split_by_value

```cpp
fz_split_by_value()
```

**Description**:

Used by feature zero, split string by delimeter and then split each segment as kv pair, then add each value to output list. Null and illegal segments are skipped. 

**Since**:
0.1.0



**Supported Types**:

* [`string`, `string`, `string`] 

### function fz_top1_ratio

```cpp
fz_top1_ratio()
```

**Description**:

Compute the top1 key's ratio. 



```
    @since 0.1.0
```

**Supported Types**:

* [`list<date>`]
* [`list<number>`]
* [`list<string>`]
* [`list<timestamp>`] 

### function fz_topn_frequency

```cpp
fz_topn_frequency()
```

**Description**:

Return the topN keys sorted by their frequency. 



```
    @since 0.1.0
```

**Supported Types**:

* [`list<date>`, `list<int32>`]
* [`list<number>`, `list<int32>`]
* [`list<string>`, `list<int32>`]
* [`list<timestamp>`, `list<int32>`] 

### function fz_window_split

```cpp
fz_window_split()
```

**Description**:

Used by feature zero, for each string value from specified column of window, split by delimeter and add segment to output list. Null values are skipped. 

**Since**:
0.1.0



**Supported Types**:

* [`list<string>`, `list<string>`] 

### function fz_window_split_by_key

```cpp
fz_window_split_by_key()
```

**Description**:

Used by feature zero, for each string value from specified column of window, split by delimeter and then split each segment as kv pair, then add each key to output list. Null and illegal segments are skipped. 

**Since**:
0.1.0



**Supported Types**:

* [`list<string>`, `list<string>`, `list<string>`] 

### function fz_window_split_by_value

```cpp
fz_window_split_by_value()
```

**Description**:

Used by feature zero, for each string value from specified column of window, split by delimeter and then split each segment as kv pair, then add each value to output list. Null and illegal segments are skipped. 

**Since**:
0.1.0



**Supported Types**:

* [`list<string>`, `list<string>`, `list<string>`] 

### function hour

```cpp
hour()
```

**Description**:

Return the hour for a timestamp. 

**Since**:
0.1.0


Example: 

```cpp
select hour(timestamp(1590115420000));
-- output 10
```




**Supported Types**:

* [`int64`]
* [`timestamp`] 

### function identity

```cpp
identity()
```

**Description**:

Return value. 

**Since**:
0.1.0


Example: 

```cpp
select identity(1);
-- output 1
```




**Supported Types**:

* [`bool`]
* [`date`]
* [`number`]
* [`string`]
* [`timestamp`] 

### function if_null

```cpp
if_null()
```

**Description**:

If input is not null, return input value; else return default value. 

**Parameters**: 

  * **input** Input value 
  * **default** Default value if input is null


**Since**:
0.1.0


Example:



```cpp
SELECT if_null("hello", "default"), if_null(NULL, "default");
-- output ["hello", "default"]
```




**Supported Types**:

* [`bool`, `bool`]
* [`date`, `date`]
* [`double`, `double`]
* [`float`, `float`]
* [`int16`, `int16`]
* [`int32`, `int32`]
* [`int64`, `int64`]
* [`string`, `string`]
* [`timestamp`, `timestamp`] 

### function ifnull

```cpp
ifnull()
```

**Description**:

If input is not null, return input value; else return default value. 

**Parameters**: 

  * **input** Input value 
  * **default** Default value if input is null


**Since**:
0.1.0


Example:



```cpp
SELECT ifnull("hello", "default"), ifnull(NULL, "default");
-- output ["hello", "default"]
```




**Supported Types**:

* [`bool`, `bool`]
* [`date`, `date`]
* [`double`, `double`]
* [`float`, `float`]
* [`int16`, `int16`]
* [`int32`, `int32`]
* [`int64`, `int64`]
* [`string`, `string`]
* [`timestamp`, `timestamp`] 

### function ilike_match

```cpp
ilike_match()
```

**Description**:

pattern match same as ILIKE predicate 

**Parameters**: 

  * **target** string to match
  * **pattern** the glob match pattern
  * **escape** escape character


**Since**:
0.4.0


Rules:

1. Special characters:
    * underscore(_): exact one character
    * precent(%): zero or more characters.
2. Escape character:
    * backslash() is the default escape character
    * length of <escape character> must <= 1
    * if <escape character> is empty, escape feautre is disabled
3. case insensitive
4. backslash: sql string literal use backslash() for escape sequences, write '\' as backslash itself
5. if one or more of target, pattern and escape are null values, then the result is null
Example: 

```cpp
select ilike_match('Mike', 'mi_e', '\\')
-- output: true

select ilike_match('Mike', 'mi\\_e', '\\')
-- output: false

select ilike_match('Mi_e', 'mi\\_e', '\\')
-- output: true

select ilike_match('Mi\\ke', 'mi\\_e', '')
-- output: true

select ilike_match('Mi\\ke', 'mi\\_e', string(null))
-- output: null
```




**Supported Types**:

* [`string`, `string`]
* [`string`, `string`, `string`] 

### function inc

```cpp
inc()
```

**Description**:

Return expression + 1. 

**Since**:
0.1.0


Example: 

```cpp
select inc(1);
-- output 2
```




**Supported Types**:

* [`number`] 

### function int16

```cpp
int16()
```

**Description**:

Cast string expression to int16. 

**Since**:
0.1.0


Example:



```cpp
select int16("123");
-- output 123
```




**Supported Types**:

* [`string`] 

### function int32

```cpp
int32()
```

**Description**:

Cast string expression to int32. 

**Since**:
0.1.0


Example:



```cpp
select int32("12345");
-- output 12345
```




**Supported Types**:

* [`string`] 

### function int64

```cpp
int64()
```

**Description**:

Cast string expression to int64. 

**Since**:
0.1.0


Example:



```cpp
select int64("1590115420000");
-- output 1590115420000
```




**Supported Types**:

* [`string`] 

### function is_null

```cpp
is_null()
```

**Description**:

Check if input value is null, return bool. 

**Parameters**: 

  * **input** Input value


**Since**:
0.1.0



**Supported Types**:

* [`bool`]
* [`date`]
* [`number`]
* [`string`]
* [`timestamp`] 

### function isnull

```cpp
isnull()
```

**Description**:

Check if input value is null, return bool. 

**Parameters**: 

  * **input** Input value


**Since**:
0.1.0



**Supported Types**:

* [`bool`]
* [`date`]
* [`number`]
* [`string`]
* [`timestamp`] 

### function lag

```cpp
lag()
```

**Description**:

Returns the value of expression from the offset-th row of the ordered partition. 

**Parameters**: 

  * **offset** The number of rows forward from the current row from which to obtain the value.



Example:


| value    |
|  -------- |
| 0    |
| 1    |
| 2    |
| 3    |
| 4    |




```cpp
SELECT lag(value, 3) OVER w;
-- output 3
```



**Supported Types**:

* [`list<bool>`, `int64`]
* [`list<date>`, `int64`]
* [`list<number>`, `int64`]
* [`list<string>`, `int64`]
* [`list<timestamp>`, `int64`] 

### function like_match

```cpp
like_match()
```

**Description**:

pattern match same as LIKE predicate 

**Parameters**: 

  * **target** string to match
  * **pattern** the glob match pattern
  * **escape** escape character


**Since**:
0.4.0


Rules:

1. Special characters:
    * underscore(_): exact one character
    * precent(%): zero or more characters.
2. Escape character:
    * backslash() is the default escape character
    * length of <escape character> must <= 1
    * if <escape character> is empty, escape feature is disabled
3. case sensitive
4. backslash: sql string literal use backslash() for escape sequences, write '\' as backslash itself
5. if one or more of target, pattern and escape are null values, then the result is null
Example: 

```cpp
select like_match('Mike', 'Mi_e', '\\')
-- output: true

select like_match('Mike', 'Mi\\_e', '\\')
-- output: false

select like_match('Mi_e', 'Mi\\_e', '\\')
-- output: true

select like_match('Mi\\ke', 'Mi\\_e', '')
-- output: true

select like_match('Mi\\ke', 'Mi\\_e', string(null))
-- output: null
```




**Supported Types**:

* [`string`, `string`]
* [`string`, `string`, `string`] 

### function ln

```cpp
ln()
```

**Description**:

Return the natural logarithm of expr. 

**Parameters**: 

  * **expr** 


**Since**:
0.1.0


Example:



```cpp
SELECT LN(1);  
-- output 0.000000
```




**Supported Types**:

* [`bool`]
* [`number`] 

### function log

```cpp
log()
```

**Description**:

log(base, expr) If called with one parameter, this function returns the natural logarithm of expr. If called with two parameters, this function returns the logarithm of expr to the base. 

**Parameters**: 

  * **base** 
  * **expr** 


**Since**:
0.1.0


Example:



```cpp
SELECT LOG(1);  
-- output 0.000000

SELECT LOG(10,100);
-- output 2
```




**Supported Types**:

* [`bool`]
* [`bool`, `bool`]
* [`bool`, `date`]
* [`bool`, `number`]
* [`bool`, `string`]
* [`bool`, `timestamp`]
* [`number`]
* [`number`, `bool`]
* [`number`, `date`]
* [`number`, `number`]
* [`number`, `string`]
* [`number`, `timestamp`] 

### function log10

```cpp
log10()
```

**Description**:

Return the base-10 logarithm of expr. 

**Parameters**: 

  * **expr** 


**Since**:
0.1.0


Example:



```cpp
SELECT LOG10(100);  
-- output 2
```




**Supported Types**:

* [`bool`]
* [`number`] 

### function log2

```cpp
log2()
```

**Description**:

Return the base-2 logarithm of expr. 

**Parameters**: 

  * **expr** 


**Since**:
0.1.0


Example:



```cpp
SELECT LOG2(65536);  
-- output 16
```




**Supported Types**:

* [`bool`]
* [`number`] 

### function make_tuple

```cpp
make_tuple()
```

**Description**:


**Supported Types**:

* [...] 

### function max

```cpp
max()
```

**Description**:

Compute maximum of values. 

**Parameters**: 

  * **value** Specify value column to aggregate on.


**Since**:
0.1.0



Example:


| value    |
|  -------- |
| 0    |
| 1    |
| 2    |
| 3    |
| 4    |




```cpp
SELECT max(value) OVER w;
-- output 4
```




**Supported Types**:

* [`list<date>`]
* [`list<number>`]
* [`list<string>`]
* [`list<timestamp>`] 

### function max_cate

```cpp
max_cate()
```

**Description**:

Compute maximum of values grouped by category key and output string. Each group is represented as 'K:V' and separated by comma in outputs and are sorted by key in ascend order. 

**Parameters**: 

  * **value** Specify value column to aggregate on. 
  * **catagory** Specify catagory column to group by.



Example:


| value   | catagory    |
|  -------- | -------- |
| 0   | x    |
| 1   | y    |
| 2   | x    |
| 3   | y    |
| 4   | x    |




```cpp
SELECT max_cate(value, catagory) OVER w;
-- output "x:4,y:3"
```



**Supported Types**:

* [`list<number>`, `list<date>`]
* [`list<number>`, `list<int16>`]
* [`list<number>`, `list<int32>`]
* [`list<number>`, `list<int64>`]
* [`list<number>`, `list<string>`]
* [`list<number>`, `list<timestamp>`] 

### function max_cate_where

```cpp
max_cate_where()
```

**Description**:

Compute maximum of values matching specified condition grouped by category key and output string. Each group is represented as 'K:V' and separated by comma in outputs and are sorted by key in ascend order. 

**Parameters**: 
 
  * **value** Specify value column to aggregate on. 
  * **condition** Specify condition column.
  * **catagory** Specify catagory column to group by.


Example:


| value   | condition   | catagory    |
|  -------- | -------- | -------- |
| 0   | true   | x    |
| 1   | false   | y    |
| 2   | false   | x    |
| 3   | true   | y    |
| 4   | true   | x    |




```cpp
SELECT max_cate_where(value, condition, catagory) OVER w;
-- output "x:4,y:3"
```



**Supported Types**:

* [`list<number>`, `list<bool>`, `list<date>`]
* [`list<number>`, `list<bool>`, `list<int16>`]
* [`list<number>`, `list<bool>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<string>`]
* [`list<number>`, `list<bool>`, `list<timestamp>`] 

### function max_where

```cpp
max_where()
```

**Description**:

Compute maximum of values match specified condition. 

**Parameters**: 

  * **value** Specify value column to aggregate on. 
  * **condition** Specify condition column.


**Since**:
0.1.0



Example:


| value    |
|  -------- |
| 0    |
| 1    |
| 2    |
| 3    |
| 4    |




```cpp
SELECT max_where(value, value <= 2) OVER w;
-- output 2
```




**Supported Types**:

* [`list<number>`, `list<bool>`] 

### function maximum

```cpp
maximum()
```

**Description**:

Compute maximum of two arguments. 

**Since**:
0.1.0



**Supported Types**:

* [`bool`, `bool`]
* [`date`, `date`]
* [`double`, `double`]
* [`float`, `float`]
* [`int16`, `int16`]
* [`int32`, `int32`]
* [`int64`, `int64`]
* [`string`, `string`]
* [`timestamp`, `timestamp`] 

### function min

```cpp
min()
```

**Description**:

Compute minimum of values. 

**Parameters**: 

  * **value** Specify value column to aggregate on.


**Since**:
0.1.0



Example:


| value    |
|  -------- |
| 0    |
| 1    |
| 2    |
| 3    |
| 4    |




```cpp
SELECT min(value) OVER w;
-- output 0
```




**Supported Types**:

* [`list<date>`]
* [`list<number>`]
* [`list<string>`]
* [`list<timestamp>`] 

### function min_cate

```cpp
min_cate()
```

**Description**:

Compute minimum of values grouped by category key and output string. Each group is represented as 'K:V' and separated by comma in outputs and are sorted by key in ascend order. 

**Parameters**: 

  * **value** Specify value column to aggregate on. 
  * **catagory** Specify catagory column to group by.



Example:


| value   | catagory    |
|  -------- | -------- |
| 0   | x    |
| 1   | y    |
| 2   | x    |
| 3   | y    |
| 4   | x    |




```cpp
SELECT min_cate(value, catagory) OVER w;
-- output "x:0,y:1"
```



**Supported Types**:

* [`list<number>`, `list<date>`]
* [`list<number>`, `list<int16>`]
* [`list<number>`, `list<int32>`]
* [`list<number>`, `list<int64>`]
* [`list<number>`, `list<string>`]
* [`list<number>`, `list<timestamp>`] 

### function min_cate_where

```cpp
min_cate_where()
```

**Description**:

Compute minimum of values matching specified condition grouped by category key and output string. Each group is represented as 'K:V' and separated by comma in outputs and are sorted by key in ascend order. 

**Parameters**: 

  * **value** Specify value column to aggregate on. 
  * **condition** Specify condition column.
  * **catagory** Specify catagory column to group by. 


Example:


| value   | condition   | catagory    |
|  -------- | -------- | -------- |
| 0   | true   | x    |
| 1   | false   | y    |
| 2   | false   | x    |
| 1   | true   | y    |
| 4   | true   | x    |
| 3   | true   | y    |




```cpp
SELECT min_cate_where(value, condition, catagory) OVER w;
-- output "x:0,y:1"
```



**Supported Types**:

* [`list<number>`, `list<bool>`, `list<date>`]
* [`list<number>`, `list<bool>`, `list<int16>`]
* [`list<number>`, `list<bool>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<string>`]
* [`list<number>`, `list<bool>`, `list<timestamp>`] 

### function min_where

```cpp
min_where()
```

**Description**:

Compute minimum of values match specified condition. 

**Parameters**: 

  * **value** Specify value column to aggregate on. 
  * **condition** Specify condition column.


**Since**:
0.1.0



Example:


| value    |
|  -------- |
| 0    |
| 1    |
| 2    |
| 3    |
| 4    |




```cpp
SELECT min_where(value, value > 2) OVER w;
-- output 3
```




**Supported Types**:

* [`list<number>`, `list<bool>`] 

### function minimum

```cpp
minimum()
```

**Description**:

Compute minimum of two arguments. 

**Since**:
0.1.0



**Supported Types**:

* [`bool`, `bool`]
* [`date`, `date`]
* [`double`, `double`]
* [`float`, `float`]
* [`int16`, `int16`]
* [`int32`, `int32`]
* [`int64`, `int64`]
* [`string`, `string`]
* [`timestamp`, `timestamp`] 

### function minute

```cpp
minute()
```

**Description**:

Return the minute for a timestamp. 

**Since**:
0.1.0


Example: 

```cpp
select minute(timestamp(1590115420000));
-- output 43
```




**Supported Types**:

* [`int64`]
* [`timestamp`] 

### function month

```cpp
month()
```

**Description**:

Return the month part of a timestamp or date. 

**Since**:
0.1.0


Example: 

```cpp
select month(timestamp(1590115420000));
-- output 5
```




**Supported Types**:

* [`date`]
* [`int64`]
* [`timestamp`] 

### function nvl

```cpp
nvl()
```

**Description**:

If input is not null, return input value; else return default value. 

**Parameters**: 

  * **input** Input value 
  * **default** Default value if input is null


**Since**:
0.1.0


Example:



```cpp
SELECT if_null("hello", "default"), if_null(NULL, "default");
-- output ["hello", "default"]
```




**Supported Types**:

* [`bool`, `bool`]
* [`date`, `date`]
* [`double`, `double`]
* [`float`, `float`]
* [`int16`, `int16`]
* [`int32`, `int32`]
* [`int64`, `int64`]
* [`string`, `string`]
* [`timestamp`, `timestamp`] 

### function nvl2

```cpp
nvl2()
```

**Description**:

nvl2(expr1, expr2, expr3) - Returns expr2 if expr1 is not null, or expr3 otherwise. 

**Parameters**: 

  * **expr1** Condition expression 
  * **expr2** Return value if expr1 is not null 
  * **expr3** Return value if expr1 is null


**Since**:
0.2.3


Example:



```cpp
SELECT nvl2(NULL, 2, 1);
-- output 1
```




**Supported Types**:

* [`bool`, `bool`, `bool`]
* [`bool`, `date`, `date`]
* [`bool`, `double`, `double`]
* [`bool`, `float`, `float`]
* [`bool`, `int16`, `int16`]
* [`bool`, `int32`, `int32`]
* [`bool`, `int64`, `int64`]
* [`bool`, `string`, `string`]
* [`bool`, `timestamp`, `timestamp`]
* [`date`, `bool`, `bool`]
* [`date`, `date`, `date`]
* [`date`, `double`, `double`]
* [`date`, `float`, `float`]
* [`date`, `int16`, `int16`]
* [`date`, `int32`, `int32`]
* [`date`, `int64`, `int64`]
* [`date`, `string`, `string`]
* [`date`, `timestamp`, `timestamp`]
* [`number`, `bool`, `bool`]
* [`number`, `date`, `date`]
* [`number`, `double`, `double`]
* [`number`, `float`, `float`]
* [`number`, `int16`, `int16`]
* [`number`, `int32`, `int32`]
* [`number`, `int64`, `int64`]
* [`number`, `string`, `string`]
* [`number`, `timestamp`, `timestamp`]
* [`string`, `bool`, `bool`]
* [`string`, `date`, `date`]
* [`string`, `double`, `double`]
* [`string`, `float`, `float`]
* [`string`, `int16`, `int16`]
* [`string`, `int32`, `int32`]
* [`string`, `int64`, `int64`]
* [`string`, `string`, `string`]
* [`string`, `timestamp`, `timestamp`]
* [`timestamp`, `bool`, `bool`]
* [`timestamp`, `date`, `date`]
* [`timestamp`, `double`, `double`]
* [`timestamp`, `float`, `float`]
* [`timestamp`, `int16`, `int16`]
* [`timestamp`, `int32`, `int32`]
* [`timestamp`, `int64`, `int64`]
* [`timestamp`, `string`, `string`]
* [`timestamp`, `timestamp`, `timestamp`] 

### function pow

```cpp
pow()
```

**Description**:

Return the value of expr1 to the power of expr2. 

**Parameters**: 

  * **expr1** 
  * **expr2** 


**Since**:
0.1.0


Example:



```cpp
SELECT POW(2, 10);
-- output 1024.000000
```




**Supported Types**:

* [`bool`, `bool`]
* [`bool`, `number`]
* [`number`, `bool`]
* [`number`, `number`] 

### function power

```cpp
power()
```

**Description**:

Return the value of expr1 to the power of expr2. 

**Parameters**: 

  * **expr1** 
  * **expr2** 


**Since**:
0.1.0


Example:



```cpp
SELECT POW(2, 10);
-- output 1024.000000
```




**Supported Types**:

* [`bool`, `bool`]
* [`bool`, `number`]
* [`number`, `bool`]
* [`number`, `number`] 

### function round

```cpp
round()
```

**Description**:

Return the nearest integer value to expr (in floating-point format), rounding halfway cases away from zero, regardless of the current rounding mode. 

**Parameters**: 

  * **expr** 


**Since**:
0.1.0


Example:



```cpp
SELECT ROUND(1.23);
-- output 1
```




**Supported Types**:

* [`bool`]
* [`number`] 

### function second

```cpp
second()
```

**Description**:

Return the second for a timestamp. 

**Since**:
0.1.0


Example: 

```cpp
select second(timestamp(1590115420000));
-- output 40
```




**Supported Types**:

* [`int64`]
* [`timestamp`] 

### function sin

```cpp
sin()
```

**Description**:

Return the sine of expr. 

**Parameters**: 

  * **expr** It is a single argument in radians.


**Since**:
0.1.0


Example:



```cpp
SELECT SIN(0);
-- output 0.000000
```





* The value returned by [sin()](/reference/sql/functions_and_operators/Files/udfs_8h.md#function-sin) is always in the range: -1 to 1.

**Supported Types**:

* [`number`] 

### function sqrt

```cpp
sqrt()
```

**Description**:

Return square root of expr. 

**Parameters**: 

  * **expr** It is a single argument in radians.


**Since**:
0.1.0


Example:



```cpp
SELECT SQRT(100);
-- output 10.000000
```




**Supported Types**:

* [`number`] 

### function strcmp

```cpp
strcmp()
```

**Description**:

Returns 0 if the strings are the same, -1 if the first argument is smaller than the second according to the current sort order, and 1 otherwise. 

**Since**:
0.1.0


Example:



```cpp
select strcmp("text", "text1");
-- output -1
select strcmp("text1", "text");
-- output 1
select strcmp("text", "text");
-- output 0
```




**Supported Types**:

* [`string`, `string`] 

### function string

```cpp
string()
```

**Description**:

Return string converted from numeric expression. 

**Since**:
0.1.0


Example:



```cpp
select string(123);
-- output "123"

select string(1.23);
-- output "1.23"
```




**Supported Types**:

* [`bool`]
* [`date`]
* [`number`]
* [`timestamp`] 

### function substr

```cpp
substr()
```

**Description**:

Return a substring from string `str` starting at position `pos`. 

**Parameters**: 

  * **str** 
  * **pos** define the begining of the substring.


**Since**:
0.1.0


Note: This function equals the `[substr()](/reference/sql/functions_and_operators/Files/udfs_8h.md#function-substr)` function.

Example:



```cpp
select substr("hello world", 2);
-- output "llo world"

select substring("hello world", 2);
-- output "llo world"
```





* If `pos` is positive, the begining of the substring is `pos` charactors from the start of string.
* If `pos` is negative, the beginning of the substring is `pos` characters from the end of the string, rather than the beginning.

**Supported Types**:

* [`string`, `int32`]
* [`string`, `int32`, `int32`] 

### function substring

```cpp
substring()
```

**Description**:

Return a substring from string `str` starting at position `pos`. 

**Parameters**: 

  * **str** 
  * **pos** define the begining of the substring.


**Since**:
0.1.0


Note: This function equals the `[substr()](/reference/sql/functions_and_operators/Files/udfs_8h.md#function-substr)` function.

Example:



```cpp
select substr("hello world", 2);
-- output "llo world"

select substring("hello world", 2);
-- output "llo world"
```





* If `pos` is positive, the begining of the substring is `pos` charactors from the start of string.
* If `pos` is negative, the beginning of the substring is `pos` characters from the end of the string, rather than the beginning.

**Supported Types**:

* [`string`, `int32`]
* [`string`, `int32`, `int32`] 

### function sum

```cpp
sum()
```

**Description**:

Compute sum of values. 

**Parameters**: 

  * **value** Specify value column to aggregate on.



Example:


| value    |
|  -------- |
| 0    |
| 1    |
| 2    |
| 3    |
| 4    |




```cpp
SELECT sum(value) OVER w;
-- output 10
```



**Supported Types**:

* [`list<number>`]
* [`list<timestamp>`] 

### function sum_cate

```cpp
sum_cate()
```

**Description**:

Compute sum of values grouped by category key and output string. Each group is represented as 'K:V' and separated by comma in outputs and are sorted by key in ascend order. 

**Parameters**: 

  * **value** Specify value column to aggregate on. 
  * **catagory** Specify catagory column to group by.



Example:


| value   | catagory    |
|  -------- | -------- |
| 0   | x    |
| 1   | y    |
| 2   | x    |
| 3   | y    |
| 4   | x    |




```cpp
SELECT sum_cate(value, catagory) OVER w;
-- output "x:6,y:4"
```



**Supported Types**:

* [`list<number>`, `list<date>`]
* [`list<number>`, `list<int16>`]
* [`list<number>`, `list<int32>`]
* [`list<number>`, `list<int64>`]
* [`list<number>`, `list<string>`]
* [`list<number>`, `list<timestamp>`] 

### function sum_cate_where

```cpp
sum_cate_where()
```

**Description**:

Compute sum of values matching specified condition grouped by category key and output string. Each group is represented as 'K:V' and separated by comma in outputs and are sorted by key in ascend order. 

**Parameters**: 

  * **value** Specify value column to aggregate on. 
  * **condition** Specify condition column.
  * **catagory** Specify catagory column to group by. 


Example:


| value   | condition   | catagory    |
|  -------- | -------- | -------- |
| 0   | true   | x    |
| 1   | false   | y    |
| 2   | false   | x    |
| 3   | true   | y    |
| 4   | true   | x    |




```cpp
SELECT sum_cate_where(value, condition, catagory) OVER w;
-- output "x:4,y:3"
```



**Supported Types**:

* [`list<number>`, `list<bool>`, `list<date>`]
* [`list<number>`, `list<bool>`, `list<int16>`]
* [`list<number>`, `list<bool>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<string>`]
* [`list<number>`, `list<bool>`, `list<timestamp>`] 

### function sum_where

```cpp
sum_where()
```

**Description**:

Compute sum of values match specified condition. 

**Parameters**: 

  * **value** Specify value column to aggregate on. 
  * **condition** Specify condition column.


**Since**:
0.1.0



Example:


| value    |
|  -------- |
| 0    |
| 1    |
| 2    |
| 3    |
| 4    |




```cpp
SELECT sum_where(value, value > 2) OVER w;
-- output 7
```




**Supported Types**:

* [`list<number>`, `list<bool>`] 

### function tan

```cpp
tan()
```

**Description**:

Return the tangent of expr. 

**Parameters**: 

  * **expr** It is a single argument in radians.


**Since**:
0.1.0


Example:



```cpp
SELECT TAN(0);
-- output 0.000000
```




**Supported Types**:

* [`number`] 

### function timestamp

```cpp
timestamp()
```

**Description**:

Cast int64, date or string expression to timestamp. 

**Since**:
0.1.0


Supported string style:

* yyyy-mm-dd
* yyyymmdd
* yyyy-mm-dd hh:mm:ss
Example:


```{tip}
We can use `string()` to make timestamp type values more readable.
```
```cpp
select string(timestamp(1590115420000));
-- output 2020-05-22 10:43:40

select string(timestamp(date("2020-05-22")));
-- output 2020-05-22 00:00:00

select string(timestamp("2020-05-22 10:43:40"));
-- output 2020-05-22 10:43:40
```




**Supported Types**:

* [`date`]
* [`string`] 

### function top

```cpp
top()
```

**Description**:

Compute top k of values and output string separated by comma. The outputs are sorted in desc order. 

**Parameters**: 

  * **value** Specify value column to aggregate on. 
  * **k** Fetch top n keys.


**Since**:
0.1.0



Example:


| value    |
|  -------- |
| 0    |
| 1    |
| 2    |
| 3    |
| 4    |




```cpp
SELECT top(value, 3) OVER w;
-- output "2,3,4"
```




**Supported Types**:

* [`list<date>`, `list<int32>`]
* [`list<date>`, `list<int64>`]
* [`list<number>`, `list<int32>`]
* [`list<number>`, `list<int64>`]
* [`list<string>`, `list<int32>`]
* [`list<string>`, `list<int64>`]
* [`list<timestamp>`, `list<int32>`]
* [`list<timestamp>`, `list<int64>`] 

### function top_n_key_avg_cate_where

```cpp
top_n_key_avg_cate_where()
```

**Description**:

Compute average of values matching specified condition grouped by category key. Output string for top N keys in descend order. Each group is represented as 'K:V' and separated by comma. 

**Parameters**: 
 
  * **value** Specify value column to aggregate on. 
  * **condition** Specify condition column. 
  * **catagory** Specify catagory column to group by.
  * **n** Fetch top n keys.



Example:


| value   | condition   | catagory    |
|  -------- | -------- | -------- |
| 0   | true   | x    |
| 1   | false   | y    |
| 2   | false   | x    |
| 3   | true   | y    |
| 4   | true   | x    |
| 5   | true   | z    |
| 6   | false   | z    |




```cpp
    SELECT top_n_key_avg_cate_where(value, condition, catagory, 2)
OVER w;
    -- output "z:5,y:3"
```



**Supported Types**:

* [`list<number>`, `list<bool>`, `list<date>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<date>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<int16>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<int16>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<int32>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<int32>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<int64>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<int64>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<string>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<string>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<timestamp>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<timestamp>`, `list<int64>`] 

### function top_n_key_count_cate_where

```cpp
top_n_key_count_cate_where()
```

**Description**:

Compute count of values matching specified condition grouped by category key. Output string for top N keys in descend order. Each group is represented as 'K:V' and separated by comma. 

**Parameters**: 

  * **value** Specify value column to aggregate on. 
  * **condition** Specify condition column. 
  * **catagory** Specify catagory column to group by. 
  * **n** Fetch top n keys.



Example:


| value   | condition   | catagory    |
|  -------- | -------- | -------- |
| 0   | true   | x    |
| 1   | true   | y    |
| 2   | false   | x    |
| 3   | true   | y    |
| 4   | false   | x    |
| 5   | true   | z    |
| 6   | true   | z    |




```cpp
    SELECT top_n_key_count_cate_where(value, condition, catagory, 2)
OVER w;
    -- output "z:2,y:2"
```



**Supported Types**:

* [`list<number>`, `list<bool>`, `list<date>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<date>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<int16>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<int16>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<int32>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<int32>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<int64>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<int64>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<string>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<string>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<timestamp>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<timestamp>`, `list<int64>`] 

### function top_n_key_max_cate_where

```cpp
top_n_key_max_cate_where()
```

**Description**:

Compute maximum of values matching specified condition grouped by category key. Output string for top N keys in descend order. Each group is represented as 'K:V' and separated by comma. 

**Parameters**: 

  * **value** Specify value column to aggregate on. 
  * **condition** Specify condition column. 
  * **catagory** Specify catagory column to group by. 
  * **n** Fetch top n keys.



Example:


| value   | condition   | catagory    |
|  -------- | -------- | -------- |
| 0   | true   | x    |
| 1   | false   | y    |
| 2   | false   | x    |
| 3   | true   | y    |
| 4   | true   | x    |
| 5   | true   | z    |
| 6   | false   | z    |




```cpp
    SELECT top_n_key_max_cate_where(value, condition, catagory, 2)
OVER w;
    -- output "z:5,y:3"
```



**Supported Types**:

* [`list<number>`, `list<bool>`, `list<date>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<date>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<int16>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<int16>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<int32>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<int32>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<int64>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<int64>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<string>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<string>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<timestamp>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<timestamp>`, `list<int64>`] 

### function top_n_key_min_cate_where

```cpp
top_n_key_min_cate_where()
```

**Description**:

Compute minimum of values matching specified condition grouped by category key. Output string for top N keys in descend order. Each group is represented as 'K:V' and separated by comma. 

**Parameters**: 

  * **value** Specify value column to aggregate on. 
  * **condition** Specify condition column.
  * **catagory** Specify catagory column to group by. 
  * **n** Fetch top n keys.



Example:


| value   | condition   | catagory    |
|  -------- | -------- | -------- |
| 0   | true   | x    |
| 1   | true   | y    |
| 2   | false   | x    |
| 3   | true   | y    |
| 4   | false   | x    |
| 5   | true   | z    |
| 6   | true   | z    |




```cpp
    SELECT top_n_key_min_cate_where(value, condition, catagory, 2)
OVER w;
    -- output "z:5,y:1"
```



**Supported Types**:

* [`list<number>`, `list<bool>`, `list<date>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<date>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<int16>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<int16>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<int32>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<int32>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<int64>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<int64>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<string>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<string>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<timestamp>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<timestamp>`, `list<int64>`] 

### function top_n_key_sum_cate_where

```cpp
top_n_key_sum_cate_where()
```

**Description**:

Compute sum of values matching specified condition grouped by category key. Output string for top N keys in descend order. Each group is represented as 'K:V' and separated by comma. 

**Parameters**: 

  * **value** Specify value column to aggregate on. 
  * **condition** Specify condition column. 
  * **catagory** Specify catagory column to group by. 
  * **n** Fetch top n keys.



Example:


| value   | condition   | catagory    |
|  -------- | -------- | -------- |
| 0   | true   | x    |
| 1   | true   | y    |
| 2   | false   | x    |
| 3   | true   | y    |
| 4   | false   | x    |
| 5   | true   | z    |
| 6   | true   | z    |




```cpp
    SELECT top_n_key_sum_cate_where(value, condition, catagory, 2)
OVER w;
    -- output "z:11,y:4"
```



**Supported Types**:

* [`list<number>`, `list<bool>`, `list<date>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<date>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<int16>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<int16>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<int32>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<int32>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<int64>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<int64>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<string>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<string>`, `list<int64>`]
* [`list<number>`, `list<bool>`, `list<timestamp>`, `list<int32>`]
* [`list<number>`, `list<bool>`, `list<timestamp>`, `list<int64>`] 

### function truncate

```cpp
truncate()
```

**Description**:

Return the nearest integer that is not greater in magnitude than the expr. 

**Parameters**: 

  * **expr** 


**Since**:
0.1.0


Example:



```cpp
SELECT TRUNCATE(1.23);
-- output 1.0
```




**Supported Types**:

* [`bool`]
* [`number`] 

### function ucase

```cpp
ucase()
```

**Description**:

Convert all the characters to uppercase. Note that characters values > 127 are simply returned. 

**Since**:
0.4.0


Example:



```cpp
SELECT UCASE('Sql') as str1;
--output "SQL"
```




**Supported Types**:

* [`string`] 

### function upper

```cpp
upper()
```

**Description**:

Convert all the characters to uppercase. Note that characters values > 127 are simply returned. 

**Since**:
0.4.0


Example:



```cpp
SELECT UCASE('Sql') as str1;
--output "SQL"
```




**Supported Types**:

* [`string`] 

### function week

```cpp
week()
```

**Description**:

Return the week of year for a timestamp or date. 

**Since**:
0.1.0


Example: 

```cpp
select weekofyear(timestamp(1590115420000));
-- output 21
select week(timestamp(1590115420000));
-- output 21
```




**Supported Types**:

* [`date`]
* [`int64`]
* [`timestamp`] 

### function weekofyear

```cpp
weekofyear()
```

**Description**:

Return the week of year for a timestamp or date. 

**Since**:
0.1.0


Example: 

```cpp
select weekofyear(timestamp(1590115420000));
-- output 21
select week(timestamp(1590115420000));
-- output 21
```




**Supported Types**:

* [`date`]
* [`int64`]
* [`timestamp`] 

### function year

```cpp
year()
```

**Description**:

Return the year part of a timestamp or date. 

**Since**:
0.1.0


Example: 

```cpp
select year(timestamp(1590115420000));
-- output 2020
```




**Supported Types**:

* [`date`]
* [`int64`]
* [`timestamp`] 




