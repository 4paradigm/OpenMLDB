# 列溯源机制与列名解析

## 列溯源关系

- 值溯源
    - 对于表A和它的上游表B, A.c1溯源于B.c2当且仅当
        - 对表A的任意一行ra, 存在表B的行rb满足 ra.c1 == rb.c2
        - 且对于所有A和B间的溯源列(c1', c2'), 都满足 ra.c1' == rb.c2'
    - 值溯源是严格的溯源关系
    - 例子:
       - 简单列筛选
       - left join的左表列相对左表
       - left join的右表列相对右表不是值溯源
       - union不是值溯源

- 名字溯源
    - 对于表A和它的上游表B, A.c1溯源于B.c2当且仅当
        - 对表A的任意一行ra, #可能# 存在表B的行rb满足 ra.c1 == rb.c2
        - 且对于所有A和B间的溯源列(c1', c2'), 都满足 ra.c1' == rb.c2
    - 名字溯源是放宽的溯源关系

- 列标志符 `column_id`
    - 用于标示列的值溯源，不同表相同column id的列有值溯源关系

     
## `SchemaSource`对象
- 记录一段schema的列溯源信息, 每个物理节点输出列由多段SchemaSource组成，合并在一起就是整体的输出schema
    ```c++
    class SchemaSource {
        const codec::Schema* schema_;
        std::string source_name_ = "";
        std::vector<size_t> column_ids_;
        std::vector<int> source_child_idxs_;
        std::vector<int> source_child_column_ids_;
    }
    ```
    对于每一列的column id可能有几种情况：
    - `child_idx == -1` 该列为当前物理节点新产生的列
    - `child_idx >= 0 && column_id == child_column_id` 从第`child_idx`个输入节点而来的值溯源列
    - `child_idx >= 0 && column_id == child_column_id` 从第`child_idx`个输入节点而来的非值溯源列

- 理论上可以从节点的SchemaSource做任何列解析工作

- `SchemasContext`对象
    - 带有名字的节点输出schema source的容器
    - 每个物理计划节点持有一个SchemasContext对象，所有schema相关信息都保存在这里
    - 构造和查询接口
    

## 列名解析
- 对于 `relation_name` . `column_name`：列名需要相对特定物理节点的SchemasContext解析
    - `relation_name`为空
        - 不考虑列溯源信息，只在当前schema中查找列名，要求候选column id唯一
    
    - `relation_name`
        - 递归搜索子节点，直到找到名字匹配的子SchemasContext
        - 在子context解析`column_name`
        - 在根context验证是否存在列可以溯源到找到的结果，并且无歧义
        - 根据溯源信息，同时可以找到该列的最终产生源头
          - 根据当前case，"溯源"条件只要求名字可溯源
          