# Compile OpenMLDB

## Compile on Linux
1. Download source code
    ```bash
    git clone git@github.com:4paradigm/OpenMLDB.git
    cd OpenMLDB
    ```
2. Download docker image
    ```bash
    docker pull ghcr.io/4paradigm/hybridsql:0.4.0
    ```
3. Start docker and map local dir in docker
    ```bash
    docker run -v `pwd`:/OpenMLDB -it ghcr.io/4paradigm/hybridsql:0.4.0 bash
    ```
4. DownLoad dependencies and init env(init only once)
    ```bash
    cd /OpenMLDB
    bash steps/init_env.sh  
    ``` 
5. Compile OpenMLDB
    ```bash
    mkdir build && cd build
    cmake ..
    make -j5 openmldb
    ```

## Compile OpenMLDB Spark Distribution

Download the pre-built OpenMLDB Spark distribution.

```bash
wget https://github.com/4paradigm/spark/releases/download/v3.0.0-openmldb0.2.3/spark-3.0.0-bin-openmldbspark.tgz
```

Or download the source code and compile from scratch.

```bash
git clone https://github.com/4paradigm/spark.git

cd ./spark/

./dev/make-distribution.sh --name openmldbspark --pip --tgz -Phadoop-2.7 -Pyarn -Pallinone
```

Then we can run PySpark or SparkSQL just like standard Spark distribution.

```bash
tar xzvf ./spark-3.0.0-bin-openmldbspark.tgz

cd spark-3.0.0-bin-openmldbspark/

export SPARK_HOME=`pwd`
```