# 日期与时间类型

OpenMLDB支持日期类型`DATE`和时间戳`TIMESTAMP`。

每个时间类型有一个有效值范围和一个NULL值，当指定不合法不能表示的值时使用NULL值。

| 类型      | 大小 (bytes) | 范围                                                         | 格式            | 用途                     |
| :-------- | :----------- | :----------------------------------------------------------- | :-------------- | :----------------------- |
| DATE      | 4            | 1900-01-01 ~                                                 | YYYY-MM-DD      | 日期值                   |
| TIMESTAMP | 8            | ~ INT64_MAX | 在线: int64, 离线`LOAD DATA`: int64 或 'yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]' | 混合日期和时间值，时间戳 |

## 时区处理

当时间字符串转换为`TIMESTAMP`时，OpenMLDB 会将转换为UTC时区的时间戳。当 `TIMESTAMP` 转换为时间字符串时，OpenMLDB 将存储的 `TIMESTAMP` 转换为当前时区的时间字符串。注意：目前OpenMLDB的仅支持一种时区为UTC/GMT+08:00时区。我们将尽快支持TimeZone的配置。

