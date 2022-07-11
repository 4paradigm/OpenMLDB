# Online Module Architecture

## 1. Overview

The main modules of OpenMLDB's online architecture include Apache ZooKeeper, nameserver, and tablets (further including SQL engine and storage engine). The following figure shows the relationship between these modules. Among them, tablets are the core modules of the entire OpenMLDB storage and computing, and also modules that consume more resources; ZooKeeper and nameserver are mainly used for auxiliary functions, such as metadata management and high availability. The function of each module will be described in detail below.
![image-20220316160612968](images/architecture.png)


## 2. Apache ZooKeeper
OpenMLDB relies on ZooKeeper for service discovery and metadata storage and management functions. There will be interaction between ZooKeeper and OpenMLDB SDK, tablets, namesever for distributing and updating metadata.

## 3. Nameserver
Nameserver is mainly used for tablet management and failover. When a tablet node goes down, the nameserver triggers a series of tasks to perform a failover, and when the node recovers, it reloads the data into the node. In order to ensure the high availability of the nameserver itself, the nameserver will deploy multiple instances during deployment at the same time. The namesaver will then use the primary/secondary node deployment mode, with only be one primary node at the same time. Multiple nameservers implement the preemption of the primary node through ZooKeeper. Therefore, if the current primary node unexpectedly goes offline, the secondary node will use ZooKeeper to elect a node to become the primary node again.

## 4. Tablets
Tablet is used by OpenMLDB to execute SQL and data storage, and it is also the core of the entire OpenMLDB function implementation and the bottleneck of resource consumption. From a functional point of view, Tablet includes two modules: SQL engine and storage engine. Tablet is also the smallest configurable granularity of OpenMLDB deployment resources. A tablet cannot be split across multiple physical nodes; however, there can be multiple tablets on a single physical node.

### 4.1 SQL Engine
The SQL engine is responsible for executing SQL query calculations. The execution process after the SQL engine receives the request of the SQL query is shown in the following figure:
![img](images/sql_engine.png)
The SQL engine parses SQL into an AST syntax tree through [ZetaSQL](https://github.com/4paradigm/zetasql). Because we have added special SQL syntax for feature engineering extensions such as `LAST JOIN`, `WINDOW UNION`, etc., the open source ZetaSQL has been optimized. After a series of compilation transformation, optimization, and LLVM-based codegen as shown in the figure above, the execution plan is finally generated. Based on the execution plan, the SQL engine obtains the storage layer data through the catalog to perform the final SQL execution operation. In the distributed version, a distributed execution plan is generated, and the execution task is sent to other tablet nodes for execution. Currently, the SQL engine of OpenMLDB adopts the push mode, which distributes tasks to the nodes where the data is located for execution instead of pulling the data back. This has the benefit of reducing data transfers.

### 4.2 Stoage Engine
The Storage engine is responsible for the storage of OpenMLDB data and supports the corresponding high-availability-related functions.

#### Data Distribution
OpenMLDB Cluster Edition is a distributed database. The data of a table will be partitioned, and multiple copies will be created, and finally distributed in different nodes. Two important concepts are explained here: replicas and partitioning.

- Replication: In order to ensure high availability and improve the efficiency of distributed queries, the data table will be stored in multiple copies, and these copies are called replicas.

- Partition: When a table (or specifically a copy) is stored, it will be further divided into multiple partitions for distributed computing. The number of partitions can be specified when the table is created, but once created, the number of partitions cannot be dynamically modified. A partition is the smallest unit of master-slave synchronization and expansion and contraction of a storage engine. A partition can be flexibly migrated between different tablets. At the same time, different partitions of a table can be calculated in parallel, improving the performance of distributed computing. OpenMLDB will automatically try to balance the number of partitions on each tablet to improve the overall performance of the system. Multiple partitions of a table may be distributed on different tablets. The roles of partitions are divided into primary partitions (leader) and secondary partition (followers). When a calculation request is obtained, the request will be sent to the primary partition corresponding to the data for calculation; the secondary partition is used to ensure high availability.

The figure below shows the storage layout of a data table on three tablets based on four partitions in the case of two replicas. In actual use, if the load of one or several tablets is too high, data migration can be performed based on partitioning to improve the load balance and overall throughput of the system.

![image-20220317150559595](images/table_partition.png)

#### Data Persistence and Master-Slave Synchronization
The online data of the current version of OpenMLDB is all stored in memory. In order to achieve high availability, the data will be persisted to the hard disk in the form of binlog and snapshot.

![image-20220317152718586](images/binlog_snapshot.png)

As shown in the figure above, the server will write memory and binlog at the same time after receiving the write request from the SDK. Binlog is used for master-slave synchronization. After the data is written to binlog, a background thread will asynchronously read the data from binlog and synchronize it to the slave node. After receiving the synchronization request from the node, the memory and binlog are also written. Snapshot can be regarded as a mirror of memory data, but for performance reasons, snapshot is not dumped from memory, but generated by merging binlog and the previous snapshot. Expired data will be deleted during the merging process. OpenMLDB will record the master-slave synchronization and the offset merged into the snapshot. If all the data in a binlog file is synchronized to the slave node and merged into the snapshot, the binlog file will be deleted by the background thread.


:::{note}
In the upcoming v0.5.0 version, OpenMLDB will also support disk-based storage engines, and its persistence mechanism will be different from the description in this article.
:::
