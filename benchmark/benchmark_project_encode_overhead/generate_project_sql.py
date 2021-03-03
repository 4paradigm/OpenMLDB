#!/usr/bin/env python
# -*- coding: utf-8 -*-

# generate_project_sql.py
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

import os

def main():

    FEATURE_NUMBER = 1000
    OUTPUT_SQL_PATH = "compare_project.sql"

    sql_content = "select\n"
    for i in range(FEATURE_NUMBER):
        sql_line = "trip_duration + {} as c{},\n".format(i, i)
        sql_content += sql_line

    sql_content = sql_content[:-2] + "\nfrom t1"

    f = open(OUTPUT_SQL_PATH, "w")
    f.write(sql_content)
    f.close()


if __name__ == "__main__":
    main()
