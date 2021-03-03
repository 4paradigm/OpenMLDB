# run_window_query.sh
# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#! /bin/sh
#

../../build/src/vm/csv_db --db_dir=./db_dir --db=db1 --query="select  min(col3) over w as col3_min, sum(col3) over w as col3_sum from table1 WINDOW w as (PARTITION BY col1 ORDER BY col3 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);"  2>/dev/null

