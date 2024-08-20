USE demo_db;
SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) CONFIG (execute_mode = 'request', values = ("aaa", 11, 22, 1.2, 1.3, 1635247427000, "2021-05-20"));
