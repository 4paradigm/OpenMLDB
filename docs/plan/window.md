# Window

### WindowFrameType
1. kFrameRows - 以条数来定义窗口区间
2. kFrameRowsRange - 以时间偏移来定义窗口区间，每次只滑入一条数据进入窗口，是FESQL特有
3. kFrameRange - 以时间偏移来定义窗口区间，每次滑动1毫秒的数据集进入窗口，是SQL标准的时间窗口区间，
    - 目前FESQL并不支持改窗口。因为它无法充分确保离线在线窗口一致性。
4. kFrameRowsMergeRowsRange - kFrameRows 和 kFrameRowsRange合并后形成的复合窗口。
    - SQL语法上并不支持直接显性定义kFrameRowsMergeRowsRange窗口, 是FESQL内部窗口合并优化后，产生的复合窗口，对用户不感知。
    
### Window Specification
