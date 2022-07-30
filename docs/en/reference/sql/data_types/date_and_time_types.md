# Date and Time Type

OpenMLDB supports date type `DATE` and timestamp `TIMESTAMP`.

Each time type has a valid range of values ​​and a NULL value. The NULL value is used when specifying an invalid value that cannot be represented.

| Type      | Size (bytes) |   Scope                                                         | Format            | Use                     |
| :-------- | :----------- | :----------------------------------------------------------- | :-------------- | :----------------------- |
| DATE      | 4            | 1900-01-01 ~                                                 | YYYY-MM-DD      | Date Value                   |
| TIMESTAMP | 8            | From ‘1970-01-01 00:00:01.000000’ UTC to ‘2038-01-19 03:14:07.999999’(2038-01-19 11:14:07.999999 GMT) | online: int64, offline: int64 or 'yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]' | Mixed Date and Time Value, Timestamp |

## Time Zone Handling

When a time string is converted to `TIMESTAMP`, OpenMLDB will convert it to a timestamp in the UTC time zone. However, when `TIMESTAMP` is converted to a time string, OpenMLDB converts the stored `TIMESTAMP` to a time string in the current time zone. Note: Currently OpenMLDB only supports one time zone which is UTC/GMT+08:00 time zone. We will support the configuration of TimeZone as soon as possible.
