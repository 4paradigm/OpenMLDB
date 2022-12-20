#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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

"""Module of insert data to table"""
import sqlalchemy as db

import datetime

ddl = """
create table if not exists t1(
id string,
vendor_id int,
pickup_datetime timestamp,
dropoff_datetime timestamp,
passenger_count int,
pickup_longitude double,
pickup_latitude double,
dropoff_longitude double,
dropoff_latitude double,
store_and_fwd_flag string,
trip_duration int,
index(key=vendor_id, ts=pickup_datetime),
index(key=passenger_count, ts=pickup_datetime)
);
"""
engine = db.create_engine('openmldb:///?zk=127.0.0.1:2181&zkPath=/openmldb') # no-db connection, to create the db
connection = engine.connect()
connection.execute('create database if not exists db_test;')

# use db, or create a new db connection: engine = db.create_engine('openmldb:///db_test?zk=127.0.0.1:2181&zkPath=/openmldb')
connection.execute('use db_test')
connection.execute(ddl)

def insert_row(line):
    row = line.split(',')
    row[2] = f"{int(datetime.datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S').timestamp() * 1000)}l"
    row[3] = f"{int(datetime.datetime.strptime(row[3], '%Y-%m-%d %H:%M:%S').timestamp() * 1000)}l"
    insert = f"insert into t1 values('{row[0]}', {row[1]}, {row[2]}, {row[3]}, {row[4]}, {row[5]}, " \
             f"{row[6]}, {row[7]}, {row[8]}, '{row[9]}', {row[10]});"
    connection.execute(insert)


with open('data/taxi_tour_table_train_simple.csv', 'r', encoding='utf-8') as fd:
    idx = 0
    for csv_line in fd:
        if idx == 0:
            idx = idx + 1
            continue
        insert_row(csv_line.replace('\n', ''))
        idx = idx + 1
