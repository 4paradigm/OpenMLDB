# DDLParser
我们想要从create procedure语句中解析indexes，解析的源头可以是sql本身，也可以是sql compile后的logical/physical plan。
解析indexes的目的是提高查询效率，也就是想要直接访问合适的index减少数据的读取范围，如果index不合适，我们可能需要通过较多的数据读取才能获得想要的数据。假设我们设置了合适的index，指向这类合适的index的阶段是logical to physical plan的阶段，即transform apply passes这一优化阶段。所以我们选择physical plan作为解析源头。

可以理解为，没有indexes指导时，physical plan得到一个结果，它不会有indexes相关的优化。我们将“无indexes优化的physical plan”作为解析源头，仿照passes对其进行优化尝试，passes尝试去找的index，就是我们想要加上的index。待我们将解析得到的indexes加入table，再一次执行sql compile，那么就能利用上这些indexes。
## Create procedure化简
`create procedure begin ... end;`语句sql编译时，内部实现如下：
```
return TransformPhysicalPlan(sp_plan->GetInnerPlanNodeList(),
 output);
）
```
也就说，实际的physical plan是begin ... end中的这部分，即inner plan。我们在调试测试等阶段可以直接使用inner plan为输入，它和create procedure得到的physical plan是一样的。所以下文提到的所有例子都仅仅只有inner plan，即各个select语句。
## Transform Apply Passes

具体来讲，transform阶段是先得到了physical plan，再通过apply passes得到optimized physical plan。

apply passes共6种，如下：

```
AddPass(PhysicalPlanPassType::kPassColumnProjectsOptimized);
AddPass(PhysicalPlanPassType::kPassFilterOptimized);
AddPass(PhysicalPlanPassType::kPassLeftJoinOptimized);
AddPass(PhysicalPlanPassType::kPassGroupAndSortOptimized);
AddPass(PhysicalPlanPassType::kPassLimitOptimized);
AddPass(PhysicalPlanPassType::kPassClusterOptimized);
```

接下来进行逐个分析。由于本文档专注于index的解析，所以不会过多描述各个优化策略。

### kPassColumnProjectsOptimized

 执行SimpleProjectOptimized，主要目的是“Find consecutive simple projects”。与index无关。

### kPassFilterOptimized

执行ConditionOptimized，对join/request join node和filter node的condition做优化。这个优化并不会使用到index，仅凭借key来优化。

### kPassLeftJoinOptimized

执行LeftJoinOptimized，对left/last join node都可以进行优化，可能会将作为producer0的join node进行位置调整。但还是不会使用到index，仅用key来判断是否优化。

### kPassGroupAndSortOptimized

执行GroupAndSortOptimized，这个优化使用到了index。这个优化策略的细节较多，不多赘述，它们的最底层都是KeysOptimized。KeysOptimized简单来说，它做的事情就是，将有“合适index”的table data provider设置为该index的partition data provider。

### kPassLimitOptimized

不会使用index。

### kPassClusterOptimized

优化join的left input，不会使用index。

## Extract Indexes

从上面passes的简要分析中，我们可以看出，如果想要解析出indexes，我们就模拟GroupAndSortOptimized的逻辑，当它想要match best index时，我们根据当时的信息生成出index。那么当table存在这样的index时，GroupAndSortOptimized就能利用indexes优化计划。

### GroupAndSortOptimizedParser

我们将GroupAndSortOptimized的index解析逻辑，封装为GroupAndSortOptimizedParser。

GroupAndSortOptimizedParser不可避免地和GroupAndSortOptimized的代码结构很像，但考虑到GroupAndSortOptimized依赖的东西较多，要在DDLParser内部单独运行就需要很多改动，而且让一个TransformUpPysicalPass的派生类做index提取，容易让人困惑。所以我们还是让GroupAndSortOptimizedParser独立，便于迭代。

整个GroupAndSortOptimized优化逻辑可以理解为，对某个node，比如window node，我们选择进行某种具体的optimized策略，会进行find best index，返回bool表示成功与否。

那么，我们可以将GroupAndSortOptimizedParser逻辑设计为，对某个node，我们先提取ttl info，然后进行optimizedparse，其中可以find best index的地方就新建这个index。
