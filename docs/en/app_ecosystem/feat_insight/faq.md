# Frequently Asked Questions

## What are the differences between FeatInsight and mainstream Feature Stores?

Mainstream Feature Stores, such as Feast, Tecton, Feathr, provide feature management and computation capabilities, with online storage mainly using pre-aggregated Key-value stores like Redis. FeatInsight provides real-time feature computation capabilities, and feature extraction solutions can be directly deployed with a single click without the need to re-deploy and synchronize online data. The main feature comparisons are as follows.

| Feature Store System	    | Feast            	| Tecton	        | Feathr	        | FeatInsight |
| --------------------------| ------------------ | ----------------- | ----------------- | ----------------- |
| Data Source Support	    | Multiple data sources	| Multiple data sources	| Multiple data sources	| Multiple data sources | 
| Scalability       	    | High	                | High	| Medium to High	| High | 
| Real-time Feature Service	| Supported	| Supported	| Supported	| Supported | 
| Batch Feature Service  	| Supported	| Supported	| Supported	| Supported | 
| Feature Transformation	| Basic transformations supported	| Complex transformations and SQL supported	| Complex transformations supported	| Complex transformations and SQL supported | 
| Data Storage	            | Multiple storage options supported	| Mainly supports cloud storage	| Multiple storage options supported	| Built-in high-performance time-series database, supports multiple storage options | 
| Community and Support	    | Open-source community	| Commercial support	| Open-source community	| Open-source community | 
| Real-time Feature Computation	| Not supported	| Not supported	| Not supported	| Supported | 

## Is it necessary to have OpenMLDB for deploying FeatInsight?

Yes, it is necessary because FeatInsight's metadata storage and feature computation rely on the OpenMLDB cluster. Therefore, deploying FeatInsight requires deployment of the OpenMLDB cluster. You can also use the [All-in-One Docker image](./install/docker.md) that integrates both for one-click deployment.

After using FeatInsight, users can develop and deploy features without relying on OpenMLDB CLI or SDK. All feature engineering needs can be completed through the web interface.

## How can I implement MLOps workflows using FeatInsight?

With FeatInsight, you can create databases and tables in the frontend, then submit the import tasks for online and offline data. Use OpenMLDB SQL syntax for data exploration and feature creation. You can then export offline features and deploy online features with just one click. There is no need for any additional development work to transition from offline to online in the MLOps process. For detailed steps, refer to the [Quickstart](./quickstart.md).

## How does FeatInsight support ecosystem integration?

FeatInsight relies on the OpenMLDB ecosystem and supports integration with other components in the OpenMLDB ecosystem.

For example, integration with data integration components in the OpenMLDB ecosystem supports  [Kafka](../../integration/online_datasources/kafka_connector_demo.md)、[Pulsar](../../integration/online_datasources/pulsar_connector_demo.md)、[RocketMQ](../../integration/online_datasources/rocketmq_connector.md)、[Hive](../../integration/offline_data_sources/hive.md)、[Amazon S3](../../integration/offline_data_sources/s3.md). For scheduling systems, it supports [Airflow](../../integration/deploy_integration/airflow_provider_demo.md)、[DolphinScheduler](../../integration/deploy_integration/dolphinscheduler_task_demo.md)、[Byzer](../../integration/deploy_integration/OpenMLDB_Byzer_taxi.md), etc. It also provides a certain degree of support for Spark Connector supporting HDFS, Iceberg, and cloud-related technologies like Kubernetes, Alibaba Cloud MaxCompute, etc.

## What is the business value and technical complexity of FeatInsight?

Compared to simple Feature Stores using HDFS for storing offline data and Redis for storing online data, FeatInsight's value lies in using the online-offline consistent feature extraction language of OpenMLDB SQL. For feature development scientists, they only need to write SQL logic to define features. In offline scenarios, this SQL will be translated into a distributed Spark application for execution. In online scenarios, the same SQL will be translated into query statements for an online time-series database for execution, achieving consistency between online and offline feature computations.

Currently, the SQL compiler, online storage engine, and offline computing engine are all implemented based on programming languages such as C++ and Scala. For scientists without a technical background, using SQL language to define the feature development process can reduce learning costs and improve development efficiency. All the code is open-source and available, with the OpenMLDB project at https://github.com/4paradigm/openmldb and the FeatInsight project at https://github.com/4paradigm/FeatInsight.