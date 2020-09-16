#!/usr/bin/env python

from pyspark.sql import SparkSession
import os


def main():
    spark = SparkSession.builder.appName("sparksql_app").getOrCreate()

    parquet_path1 = "file:///tmp/test1/"
    parquet_path2 = "file:///tmp/pyspark_output/"
    current_path = os.getcwd()
    parquet_path1 = "file://{}/../data/taxi_tour_all/".format(current_path)
    parquet_path2 = "file://{}/../data/taxi_tour_all/".format(current_path)

    parquet1 = spark.read.parquet(parquet_path1)
    parquet2 = spark.read.parquet(parquet_path2)

    except1 = parquet1.subtract(parquet2).count()
    except2 = parquet2.subtract(parquet1).count()

    if except1 == except2: 
        print("DataFrames are equal")
    else:
        print("DataFrames are not equal, except1: {}, except2: {}".format(except1, except2))
        
    spark.stop()

if __name__ == "__main__":
    main()
