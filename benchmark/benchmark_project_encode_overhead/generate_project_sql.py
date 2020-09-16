#!/usr/bin/env python

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
