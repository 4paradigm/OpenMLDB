# ALTER

OpenMLDB 支持通过 ALERT 命令来修改表属性。

## Syntax

```
ALTER TABLE TableName (AlterAction) 'offline_path' FilePath

TableName ::=
    Identifier ('.' Identifier)?

AlterAction:
    'add' | 'drop'

FilePath 
				::= URI

URI
				::= 'file://FilePathPattern'
				|'hdfs://FilePathPattern'
				|'hive://[db.]table'
				|'FilePathPattern'

FilePathPattern
				::= string_literal                              
```

**说明**

- `ALTER` 目前仅支持修改离线表的 symbolic paths 配置。

## Examples

```SQL
-- Add one offline path
ALTER TABLE t1 ADD offline_path 'hdfs://foo/bar';

-- Drop one offline path
ALTER TABLE t1 DROP offline_path 'hdfs://foo/bar';

-- Add one offline path and drop anthor offline path
ALTER TABLE t1 ADD offline_path 'hdfs://foo/bar', DROP offline_path 'hdfs://foo/bar2';
```