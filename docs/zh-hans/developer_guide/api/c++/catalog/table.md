# Table



## Schema

```c++
typedef ::google::protobuf::RepeatedPtrField<::fesql::type::ColumnDef> Schema;
```

schema是[`ColumnDef`](#ColumnDef)的集合



## ColumnDef

```protobuf
message ColumnDef {
    optional string name = 1;
    optional Type type = 2;
    optional uint32 offset = 3;
    optional bool is_not_null = 4;
    optional bool is_constant = 5 [default = false];
}
```





