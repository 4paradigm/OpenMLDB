## 粗粒度窗口迭代分析

- 前序pass：	语法化和类型解析
      
- 表达式的窗口迭代rank
   - 在同一个窗口下的聚合计算，可能需要多次迭代完成，例如`sum(c1 * log(sum(c2)))`的计算代码可能如下
      ```python
      c2_sum = 0
      for row in window:  # 第一次迭代(rank=1)
          c2_sum = c2_sum + row.c2
    
      c2_log = log(c2_sum)  # 第一次迭代后的非迭代计算(rank=1)

      final_sum = 0
      for row in window:  # 第二次迭代(rank=2)
         final_sum = final_sum + row.c1 * c2_log
      return final_sum
      ```
      
   - 表达式的`rank`记录该表达式可以在第几次窗口迭代后计算完成，另外可区分`is_iter`属性，即是否表达式的计算结果不需要被物化
   
   - 哪些表达式可以不被物化？
       - 窗口本身
       - map, filter

- 计算规则
   - 普通表达式:
       
       ```
       rank(expr(c1, c2, ..., cn)) = 
           max(rank(c1), rank(c2), ... rank(cn))
       ```
   
   - Lambada函数调用
       ```
       rank(call(lambada, c1, c2, ..., cn)) =
            rank(unificate(lambada, c1, c2, ..., cn))
       ```
   
   - UDAF函数调用
       ```
       rank(call(udaf, c1, c2, ..., cn)) =
           max(udaf_rank, arg_rank(c1), ..., arg_rank(cn))
           
       arg_rank(c) =
           - rank(c)为0  =>  0
           - c在窗口迭代中不需要物化  =>  rank(c)
           - UDAF所有实参都不是窗口迭代  =>  rank(c)
           - _  =>  rank(c) + 1
           
       udaf_rank = 
           - update函数为lambda  =>
               - rank(body)为0  =>  0
               - UDAF所有实参都不是窗口迭代  =>  rank(body)
               - _  =>   rank(body) + 1
           - _  =>  0
       ```

- 应用场景

    - UDAF合并优化：多个udaf合并的条件是rank相同
    
    - 增量计算：增量计算按rank拆分成组，组内的聚合操作可以在一起增量计算