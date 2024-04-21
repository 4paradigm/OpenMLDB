# 复合数据类型

## MAP 类型

包含 key-value pair 的 map 类型。

**Syntax**

```sql
MAP < KEY, VALUE >
```

**Example**

```sql
-- 使用 map function 构造 map 数据类型，参数 1-N 表示 key1, value1, key2, value2, ...
select map (1, "12", 2, "100")
-- {1: "12", 2: "100"}

-- 用 [] operator 抽取特定 key 的 value
select map (1, "12", 2, "100")[2]
-- "100"
```

**限制**

1. 由于采用行存储形式，不建议表的 MAP 类型存储 key-value pair 特别多的情况，否则可能导致性能问题。
2. map 数据类型不支持作为索引的 key 或 ts 列，无法对 map 列特定 key 做查询优化。 
3. map key-value 查询最多消耗 `O(n)` 复杂度
