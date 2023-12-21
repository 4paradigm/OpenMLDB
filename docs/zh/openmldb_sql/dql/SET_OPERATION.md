# SET Operation



SET 操作将两个或者更多相同输出 schema 的 query 语句按特定的规则整合成一个结果输出. 共有三种 Set 操作符: `UNION`, `INTERSECT`, `EXCEPT`, OpenMLDB 目前仅支持 `UNION`.



## Syntax

```yacc
set_operation:
  query set_operator query

set_operator:
  UNION { ALL | DISTINCT }
```

注意

- `ALL/DISTINCT` 是必填, 不支持单独 UNION 的写法
- 同一层 Set Operation 里, 多个 UNION 操作必须都是 UNION ALL 或者 UNION DISTINCT.  可以利用子查询将不同 DISTINCT 属性的 query 串联起来
- 这里的 UNION 语法不同于 [WINDOW clause](./WINDOW_CLAUSE.md) 中的 WINDOW UNION 语法 

## 边界说明

| SELECT语句元素 | 离线模式  | 在线预览模式 | 在线请求模式 | 说明 |
| :------------- | --------- | ------------ | ------------ | :--- |
| SET Operation  | **``✓``** | **``✓``**    | **``✓``**    |      |

- 在线模式下只支持 UNION ALL, 离线模式支持 UNION ALL 和 UNION DISTINCT



## Example

```sql
SELECT * FROM t1
UNION ALL
SELECT * FROM t2
UNION ALL
SELECT * FROM t3

-- differnt DISTINCT field
SELECT * FROM t1
UNION ALL
(
    SELECT * FROM t2
    UNION DISTINCT
    SELECT * FROM t3
)
```

