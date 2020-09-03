# 计划边界

## 性能敏感模式边界

FeSQL在性能/资源敏感的环境（如rtidb）下对查询语句有严格对要求。否则会产生内存、性能等诸多不安全问题。

### 性能敏感模式

根据使用场景对资源、性能对要求把应用场景分为：性能敏感模式和性能不敏感模式。

| 场景          | 模式   |
| ------------- | ------ |
| Rtidb请求模式 | 敏感   |
| Rtidb批模式   | 敏感   |
| FeSpark       | 不敏感 |
| FeFlink       | 不敏感 |

### 性能敏感模式下的边界规则
建立FeSQL的性能敏感模式下的边界规则

| 边界         | 边界描述                                              |
| ------------ | ----------------------------------------------------- |
| WINDOW边界   | window的partition by keys至少要有一个命中索引         |
|              | window的order by key必须为样本索引的ts列              |
|              | window union的partition keys 至少有一个命中副表的索引 |
|              | window union的order by key必须为副表索引的ts列        |
| LASTJOIN边界 | join至少有一个条件命中副表的索引                      |
|              | last join的order列为ts列                              |
| GROUP边界    | group by的keys至少有一个命中索引                      |

1. 

