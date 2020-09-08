select 
    trip_duration,
    passenger_count,
    sum(pickup_latitude) over w as vendor_sum_pl,
    max(pickup_latitude) over w as vendor_max_pl,
    min(pickup_latitude) over w as vendor_min_pl,
    avg(pickup_latitude) over w as vendor_avg_pl,
    sum(dropoff_latitude) over w as vendor_sum_pl2,
    max(dropoff_latitude) over w as vendor_max_pl2,
    min(dropoff_latitude) over w as vendor_min_pl2,
    avg(dropoff_latitude) over w as vendor_avg_pl2,
    sum(trip_duration) over w as vendor_sum_pl3,
    max(trip_duration) over w as vendor_max_pl3,
    min(trip_duration) over w as vendor_min_pl3,
    avg(trip_duration) over w as vendor_avg_pl3
from t1
window w as (partition by vendor_id order by pickup_datetime ROWS BETWEEN 10000 PRECEDING AND CURRENT ROW)
