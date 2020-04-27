#! /bin/sh
#

../../build/src/vm/csv_db --db_dir=./db_dir --db=db1 --query="select col1, col2, col3  from table1;"  2>/dev/null

