# ALTER

OpenMLDB supports ALERT command to update table's attributes.

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

**Description**

- `ALTER` only supports updateing offline table's symbolic paths currently.

## Examples

```SQL
-- Add one offline path
ALTER TABLE t1 ADD offline_path 'hdfs://foo/bar';

-- Drop one offline path
ALTER TABLE t1 DROP offline_path 'hdfs://foo/bar';

-- Add one offline path and drop anthor offline path
ALTER TABLE t1 ADD offline_path 'hdfs://foo/bar', DROP offline_path 'hdfs://foo/bar2';
```