# Standalone Version Usage Process

## Preparation

This article provides a guide on developing and deploying OpenMLDB CLI. To begin, you should download the sample data and initiate the OpenMLDB CLI. It is recommended to utilize a prepared Docker image for a convenient experience.

Prerequisites:

- Docker (minimum version: 18.03)

### Pull Mirror

Execute the following command to fetch the OpenMLDB image and initiate the Docker container:

```bash
docker run -it 4pdosc/openmldb:0.8.3 bash
```

Upon successful container launch, all subsequent commands in this tutorial will assume execution within the container.

If you require external access to the OpenMLDB server within the container, please consult [CLI/SDK-Container onebox](https://chat.openai.com/reference/ip_tips.md#clisdk-containeronebox).

### Download Sample Data

Execute the following command to download the sample data necessary for the subsequent procedures:

```bash
curl https://openmldb.ai/demo/data.csv --output /work/taxi-trip/data/data.csv
```

### Start Server and Client

- Start the standalone OpenMLDB server

```SQL
./init.sh standalone
```

- Start the standalone OpenMLDB CLI client

```SQL
cd taxi-trip
/work/openmldb/bin/openmldb --host 127.0.0.1 --port 6527
```

The following figure shows the screen after the correct execution of the commands in the Docker and the successful start of the OpenMLDB CLI:

![image-20220111142406534](C:\Users\65972\Documents\GitHub\fix_docs\OpenMLDB\docs\zh\tutorial\images\cli.png)

## Usage Process

The following image displays the screen after the correct execution of commands within Docker, signifying the successful launch of the OpenMLDB CLI:

The workflow for the standalone version of OpenMLDB typically comprises five stages:

1. Database and table establishment.
2. Data preparation.
3. Offline feature computation.
4. Deployment of SQL solutions.
5. Real-time online feature computation.

Unless otherwise specified, the following commands are executed by default within the cluster version of the OpenMLDB CLI.

### 1. Create the Database and Table

```sql
CREATE DATABASE demo_db;
USE demo_db;
CREATE TABLE demo_table1(c1 string, c2 int, c3 bigint, c4 float, c5 double, c6 timestamp, c7 date);
```

### 2. Data Preparation

Import the sample data previously downloaded as training data for both offline and online feature computations.

It's important to note that in the standalone version, table data is not segregated for offline and online usage. Thus, the same table is employed for both offline and online feature computations. Alternatively, you have the option to manually import different data for offline and online use, resulting in the creation of two separate tables. However, for simplicity, this tutorial employs the same data for both offline and online computations in the standalone version.

Execute the subsequent command to import the data:

```sql
LOAD DATA INFILE 'data/data.csv' INTO TABLE demo_table1;
```

Preview data:

```sql
SELECT * FROM demo_table1 LIMIT 10;

 ----- ---- ---- ---------- ----------- --------------- ------------

  c1    c2   c3   c4         c5          c6              c7

 ----- ---- ---- ---------- ----------- --------------- ------------

  aaa   12   22   2.200000   12.300000   1636097390000   2021-08-19

  aaa   11   22   1.200000   11.300000   1636097290000   2021-07-20

  dd    18   22   8.200000   18.300000   1636097990000   2021-06-20

  aa    13   22   3.200000   13.300000   1636097490000   2021-05-20

  cc    17   22   7.200000   17.300000   1636097890000   2021-05-26

  ff    20   22   9.200000   19.300000   1636098000000   2021-01-10

  bb    16   22   6.200000   16.300000   1636097790000   2021-05-20

  bb    15   22   5.200000   15.300000   1636097690000   2021-03-21

  bb    14   22   4.200000   14.300000   1636097590000   2021-09-23

  ee    19   22   9.200000   19.300000   1636097000000   2021-01-10

 ----- ---- ---- ---------- ----------- --------------- ------------
```

### 3. Offline Feature Computation

Execute SQL commands to extract features and store the resulting features in a file for subsequent model training.

```sql
SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) INTO OUTFILE '/tmp/feature.csv';
```

### 4. The Launch of SQL Solution

Deploy the SQL solution developed for offline feature computation online. It's crucial to ensure that the deployed SQL solution aligns with the corresponding offline feature computation SQL solution.

```sql
DEPLOY demo_data_service SELECT c1, c2, sum(c3) OVER w1 AS w1_c3_sum FROM demo_table1 WINDOW w1 AS (PARTITION BY demo_table1.c1 ORDER BY demo_table1.c6 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);
```

Once the deployment is complete, you can access the deployed SQL solutions using the `SHOW DEPLOYMENTS` command.

```sql
SHOW DEPLOYMENTS;

 --------- -------------------

  DB        Deployment

 --------- -------------------

  demo_db   demo_data_service

 --------- -------------------

1 row in set
```

```{note}
In this standalone version of the tutorial, the same dataset is utilized for both offline and online feature computations. If a user prefers to work with a different dataset, they must import the new dataset prior to deployment. During deployment, the tables associated with the new dataset should be used.
```

### 5. Exit CLI

```sql
quit;
```

At this stage, all development and deployment tasks using OpenMLDB CLI have been completed, and we have returned to the command line of the operating system.

### 6. Real-Time Feature Computation

Real-time online services can be provided through the following web APIs:

```bash
http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service

​        ___________/      ____/              _____________/

​              |               |                        |

​      APIServer Address  Database Name          Deployment Name
```

Now, the real-time system is ready to accept input data in JSON format. Here's an example: you can place a row of data into the designated `input` field.

```bash
curl http://127.0.0.1:8080/dbs/demo_db/deployments/demo_data_service -X POST -d'{"input": [["aaa", 11, 22, 1.2, 1.3, 1635247427000, "2021-05-20"]]}'
```

The expected return results for this query are as follows (the computed features will be stored in the `data` field):

```json
{"code":0,"msg":"ok","data":{"data":[["aaa",11,22]]}}
```

Explanation:

- The API server can process requests and supports batch requests. Arrays are supported via the `input` field, with each line of input processed individually. For detailed parameter formats, please refer to the [REST API documentation](https://chat.openai.com/quickstart/sdk/rest_api.md).
- To understand the results of real-time feature computation requests, please refer to the [Explanation of Real-Time Feature Computation Results](https://chat.openai.com/quickstart/openmldb_quickstart.md#explanationofrealtimefeaturecomputationresults).