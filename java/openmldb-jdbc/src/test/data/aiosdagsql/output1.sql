WITH main_instance as (
WITH t1 as (
select t2.id,t2.instance from t1 last join t2 on t1.id = t2.id
),
t2 as (
select t3.* from t1 last join t3 on t1.id = t3.id
)
select t1.*,t2.age,t2.job,t2.marital from t1 last join t2 on t1.id = t2.id
)
select main_instance.instance from main_table last join main_instance on main_table.id = main_instance.id