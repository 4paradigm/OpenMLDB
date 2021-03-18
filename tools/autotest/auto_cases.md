## 随机化测试脚本

### 基础使用
```
# 构建hybridse成功后，从hybridse core导出udf信息
./build/src/export_udf_info --output_file=./udf_defs.yaml
 
# 运行随机udf测试脚本
# 目前仅支持测试单窗口内的udf计算
python tools/autotest/auto_cases.py  \
    --bin_path=build  \
    --udf_path=udf_defs.yaml  \
    --expr_num=2  \
    --expr_depth=3  \
    --workers=4 
     
# CTRL^C停止任务，查看./logs文件夹
# 对每个失败case，子目录下包含日志以及yaml测试用例文件
  
# 更多配置参考
python tools/autotest/auto_cases.py --help
```

### Features提纲

- 多进程测试
    - 使用multiprocessing开多个子进程生成和运行随机测试用例
    - `--workers=并行度`

- 随机化配置
    - 支持用例中包括数据、窗口、函数等等多种实体的随机化配置
      ```
      --rows_preceding=2,3,4,100,1000
      --rows_preceding=[100, 1000)
      ```
      
- 生成模式
    - upwards (目前不支持)
        - query按自底向上方向生成
        - 先采样输入表的schema，此后输入表的列保持不变
        - 根据已经生成的子query（含输入表），构造父query
        - query内仅采样输入列类型支持的表达式计算
    
    - downwards
        - query按自顶向下方向生成
        - 先生成root query，以及root query对输入表的列需求
        - query内表达式构造可以任意采样
        - 父query对输入列的需求构成采样子query的约束
    
- 配置测试数据分布 (目前不支持)
    - 支持各个列类型配置特殊的数据分布
    - 支持query敏感的数据分布
        
        - 针对特定的window配置（rows bewween 3 preceding), 生成有效的时间列分布或者生成有危险性的分布；例如让窗口内的某些列大概率为null；
        - 针对特定join配置生成有效的数据分布，例如保证join之后的结果对于大部分pk非空 or join之后窗口依然有足够的大小

- 配置压测模式 (目前仅支持跑engine_test)
    - 针对随机case执行真正带压力的测试，测量系统内存等指标
    
- 自动化错误上报
    - 同gitlab / jira等流程结合
