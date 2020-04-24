#! /bin/sh
#

../../build/src/vm/csv_db --db_dir=./db_dir --db=db1 --query="select  min(col3) over w as col3_min, sum(col3) over w as col3_sum from table1 WINDOW w as (PARTITION BY col1 ORDER BY col3 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);"  2>/dev/null

