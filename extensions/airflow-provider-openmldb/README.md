<h1 align="center">
  Airflow OpenMLDB Provider
</h1>

<br/>

# Overview

Airflow OpenMLDB Provider supports connecting to OpenMLDB. Specifically, connect to the OpenMLDB API Server.

Operators:
- OpenMLDBLoadDataOperator
- OpenMLDBSelectIntoOperator
- OpenMLDBDeployOperator
- OpenMLDBSQLOperator: the underlying implementation of operators above. Support all sql.

Only operators and a hook, no sensors.

# Build

To build openmldb provider, follow the steps below:

1. Clone the repo.
2. `cd` into provider directory.
3. Run `python3 -m pip install build`.
4. Run `python3 -m build` to build the wheel.
5. Find the .whl file in `/dist/*.whl`.

# How to use

Write the dag, using openmldb operators, ref [simple openmldb operator dag example](https://github.com/4paradigm/OpenMLDB/blob/main/extensions/airflow-provider-openmldb/openmldb_provider/example_dags/example_openmldb.py).

Create the connection in airflow, the name is `openmldb_conn_id` you set. 

Trigger the dag.

## Test Way

Add connection:
```
airflow connections add openmldb_conn_id --conn-uri http://127.0.0.1:9080
airflow connections list --conn-id openmldb_conn_id
```
Dag test:
```
 airflow dags test example_openmldb_complex 2022-08-25
```
