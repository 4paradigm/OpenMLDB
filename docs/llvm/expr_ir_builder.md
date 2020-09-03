

### Binary expression 

### UnaryExpression

### Cast Expression

| src\|dist | bool   | smallint | int    | float  | int64  | double | timestamp | date   | string |
| :-------- | :----- | :------- | :----- | :----- | :----- | :----- | :-------- | :----- | :----- |
| bool      | Safe   | Safe     | Safe   | Safe   | Safe   | Safe   | UnSafe    | X      | Safe   |
| smallint  | UnSafe | Safe     | Safe   | Safe   | Safe   | Safe   | UnSafe    | X      | Safe   |
| int       | UnSafe | UnSafe   | Safe   | Safe   | Safe   | Safe   | UnSafe    | X      | Safe   |
| float     | UnSafe | UnSafe   | UnSafe | Safe   | Safe   | Safe   | UnSafe    | X      | Safe   |
| bigint    | UnSafe | UnSafe   | UnSafe | UnSafe | Safe   | UnSafe | UnSafe    | X      | Safe   |
| double    | UnSafe | UnSafe   | UnSafe | UnSafe | UnSafe | Safe   | UnSafe    | X      | Safe   |
| timestamp | UnSafe | UnSafe   | UnSafe | UnSafe | Safe   | UnSafe | Safe      | UnSafe | Safe   |
| date      | X      | X        | X      | X      | X      | X      | UnSafe    | Safe   | Safe   |
| string    | UnSafe | UnSafe   | UnSafe | UnSafe | UnSafe | UnSafe | UnSafe    | UnSafe | Safe   |



1. uncastable 场景: resolved/codegen/报错
2. occur error when casting：NULL. e.g: `cast("2020-05" as date)` 