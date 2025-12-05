# Composite data type

## MAP type

Represents values comprising a set of key-value pairs.

**Syntax**

```sql
MAP < KEY, VALUE >
```

**Example**

```sql
-- construct map value with map builtin function, each parameter is 
-- key1, value1, key2, value2, ... respectively
select map (1, "12", 2, "100")
-- {1: "12", 2: "100"}

-- access map value with [] operator
select map (1, "12", 2, "100")[2]
-- "100"
```

**Limitations**

1. Generally not recommended to store a map value with too much key-value pairs, since it's a row-based storage model.
2. Map data type can not used as the key or ts column of table index, queries can not be optimized based on specific key value inside a map column neither.
3. Query a key-value in a map takes `O(n)` complexity at most.
4. Currently, it is not allowed to output a map type value from a SQL query, however you can access information about the map value using map-related expressions. For example, you may use `[]` operator over a `map<int, int>` type to extract value of specific key.

