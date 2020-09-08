#!/usr/bin/env python

from pyspark.sql import SparkSession
import os


def main():
    spark = SparkSession.builder.appName("sparksql_app").getOrCreate()

    current_path = os.getcwd()
    parquet_file_path = "file://{}/taxi_tour_all/".format(current_path)

    train = spark.read.parquet(parquet_file_path)
    train.createOrReplaceTempView("t1")

    with open("taxi_hour_window_single_window.sql", "r") as f:
        sparksqlText = f.read()

    spark_df = spark.sql(sparksqlText)

    output_path = "file:///tmp/pyspark_output/"
    spark_df.write.mode('overwrite').parquet(output_path)

    spark.stop()


if __name__ == "__main__":
    main()
