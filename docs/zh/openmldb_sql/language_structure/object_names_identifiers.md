# SQL 对象名

本文介绍 OpenMLDB SQL 语句中的对象名。

对象名用于命名OpenMLDB中所有的对象，包括 database、table、column、index、function、deployment 等等。在 SQL 语句中，可以通过标识符 (identifier) 来引用这些对象。

## 标识符

标识符可以被反引号包裹，即 `SELECT * FROM t` 也可以写成 `SELECT * FROM `t``。但如果标识符中存在至少一个特殊符号，或者它是一个保留关键字，那就必须使用反引号包裹来引用它所代表的模式对象。

```sql
CREATE TABLE `JOIN` (`select` INT);
```

不允许标识符中包含`字符。

```sql
CREATE TABLE a`b (a int);
```

```
Syntax error: Unclosed identifier literal [at 1:15]
CREATE TABLE a`b (a int);
```



## 限定标识符



OpenMLDB中的对象名可以是限定的或者非限定的标识符。

### 限定的对象名：

限定的对象名至少包含一个限定词来描述特定的上下文环境。

- 限定词和对象名使用用`.`隔开。如`db_name.tbl_name`。
- 当包含多个限定词时，限定词与限定词使用`.`符号隔开。如`db_name.tbl_name.col_name`。
- `.` 的左右两端可以出现空格，`tbl_name.col_name` 等于 `tbl_name . col_name`。

例如，已知系统中有两个数据库，分别为`db0`和`db1`。`db1`中有表t1。当我们使用`USE db0`将系统的默认数据库设置为`db0`。当使用`db1.t1`这个限定标识符来表示一张表时，表`t1`所在的上下文是在数据库`db1`。

```sql
USE db0;
SELECT * FROM db1.t1;
```

### 非限定的对象名：

非限定的对象名不包含任何限定词的修饰，只包含对象标识符本身

如果在给定上下文下，直接使用标识符不会引起混淆，那么就可以非限定的使用标识符来表示对象。例如，我们使用`USE db1`将系统的默认数据库设置为`db1`。此时，可以直接使用非限定的对象名`t1`来代表默认数据库`db0`中的表`t1`

```
USE db1;
SELECT * from t1;
```

