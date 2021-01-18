select
    reqId,
    int(sum(`col_131`) over flattenRequest_col_680_eventTime_0_10)  as flattenRequest_col_131_window_avg_829,
    int(avg(`col_161`) over flattenRequest_col_373_eventTime_0s_5529601s) as flattenRequest_col_161_window_avg_830,
    count(`col_74`) over flattenRequest_col_612_eventTime_0s_5529601s as flattenRequest_col_74_window_avg_831
from
    `flattenRequest`
    window flattenRequest_col_680_eventTime_0_10 as (partition by `col_680` order by `eventTime` rows_range between 10s preceding and 0s preceding),
    flattenRequest_col_373_eventTime_0s_5529601s as (partition by `col_680` order by `eventTime` rows between 10 preceding and 0 preceding),
    flattenRequest_col_612_eventTime_0s_5529601s as (partition by `col_680` order by `eventTime` rows_range between 10s preceding and 0s preceding);