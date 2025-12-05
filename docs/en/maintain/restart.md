# Cluster Start Stop

The OpenMLDB cluster currently supports two deployment methods, [One-Click Deployment and Manual Deployment](https://chat.openai.com/deploy/install_deploy.md). In this document, we describe the startup and shutdown steps for each of these deployment methods separately. Please be cautious not to mix the startup and shutdown methods between the two deployment approaches, as it may lead to unpredictable issues.

```{important}
If this is the initial deployment and startup of the cluster, please refer to the comprehensive [Installation and Deployment Documentation](../deploy/install_deploy.md) for additional configurations and considerations. This document is intended for clusters that have already been deployed and may require startup, shutdown, or restart operations due to various reasons such as routine maintenance, upgrades, configuration updates, etc.
```


## One Click Deployment Cluster

### Start Cluster

Execute in the directory where the cluster is deployed, ensuring that the content of the conf/hosts file in that directory matches the deployment information of the actual components

```
sbin/start-all.sh
```

### Stop Cluster

Execute in the directory where the cluster is deployed, ensuring that the content of the conf/hosts file in that directory matches the deployment information of the actual components

```
sbin/stop-all.sh
```

## Manually Deployed Cluster

### Start Cluster

**1. Start TabletServer**

Execute the following command in the deployment directory of each deployed TabletServer node

```
bash bin/start.sh start tablet
```

After startup, there should be a `success` prompt, as shown below.

```
Starting tablet ...
Start tablet success
```

**2. Start NameServer**

Execute the following command in the deployment directory of each NameServer node deployed

```
bash bin/start.sh start nameserver
```

After startup, there should be a `success` prompt, as shown below.

```
Starting nameserver ...
Start nameserver success
```

**3. Start TaskManager**

If the TaskManager has not been deployed, you can skip this step  
Execute the following command in the deployment directory of each deployed TaskManager node

```
bash bin/start.sh start taskmanager
```

**4. Start APIServer**

If the APIServer has not been deployed, you can skip this step  
Execute the following command in the deployment directory of each deployed APIServer node

```
bash bin/start.sh start apiserver
```

After startup, there should be a `success` prompt, as shown below.

```
Starting apiserver ...
Start apiserver success
```

**5. Check if the Service is Started**

Start the SQL client, replacing the values of `zk_cluster` and `zk_root_path` with the corresponding values from the configuration file.

```bash
./bin/openmldb --zk_cluster=172.27.2.52:12200 --zk_root_path=/openmldb --role=sql_client
```

Then execute the following command

```
show components;
```

The results should be similar to the following table, including all cluster components (except for APIServer).

```
------------------- ------------ --------------------- -------- ---------
  Endpoint            Role         Connect_time          Status   Ns_role
 ------------------- ------------ --------------------- -------- ---------
  172.24.4.39:10821   tablet       2023-09-01 11:36:58   online   NULL
  172.24.4.40:10821   tablet       2023-09-01 11:36:57   online   NULL
  172.24.4.56:10821   tablet       2023-09-01 11:36:58   online   NULL
  172.24.4.40:7520    nameserver   2023-09-01 11:36:59   online   master
 ------------------- ------------ --------------------- -------- ---------

4 rows in set
```

**6. Start auto_failover**

Start the NS client, replacing the values of `zk_cluster` and `zk_root_path` with the corresponding values from the configuration file.

```
./bin/openmldb --zk_cluster=172.27.2.52:12200 --zk_root_path=/openmldb --role=ns_client
```

Execute the following command:

```
confset auto_failover true 
```

**7. Data Recovery**

Use the one-click data recovery feature in the OpenMLDB operations tool to restore data. For instructions on using the operations tool, refer to [OpenMLDB Operations Tool](https://chat.openai.com/c/openmldb_ops.md).

Execute the following command in any OpenMLDB deployment directory, replacing the values of `zk_cluster` and `zk_root_path` with their corresponding values from the configuration file. This command only needs to be executed once, and it should not be interrupted during the process.

```
python tools/openmldb_ops.py --openmldb_bin_path=./bin/openmldb --zk_cluster=172.27.2.52:12200 --zk_root_path=/openmldb --cmd=recoverdata
```

### Stop Cluster

**1. Close auto_failover**

Start the `ns_client`, replacing the values of `zk_cluster` and `zk_root_path` with their corresponding values from the configuration file.

```
./bin/openmldb --zk_cluster=172.27.2.52:12200 --zk_root_path=/openmldb --role=ns_client
```

Execute the following command:

```
confset auto_failover false 
```

**2. Stop TabletServer**

Execute the following command in the deployment directory of each deployed TabletServer node

```
bash bin/start.sh stop tablet
```

**3. Stop NameServer**

Execute the following command in the deployment directory of each NameServer node deployed

```
bash bin/start.sh stop nameserver
```

**4. Stop TaskManager**

If the TaskManager has not been deployed, you can skip this step  
Execute the following command in the deployment directory of each deployed TaskManager node

```
bash bin/start.sh stop taskmanager
```

**5. Stop APIServer**

If APIServer has not been deployed, you can skip this step  
Execute the following command in the deployment directory of each deployed TaskManager node

```
bash bin/start.sh stop apiserver
```

## Cluster Restart 

Cluster restart can occur in two scenarios: one for version upgrades or updating configuration files, and another for routine restarts without the need for upgrades or configuration changes (such as regular maintenance).

- Version upgrade or updating configuration files: Follow the specific steps outlined in the relevant operation documentation for operations like [version upgrade](https://chat.openai.com/c/upgrade.md) or [updating configuration files](https://chat.openai.com/c/update_conf.md). It's crucial to note that for tablet restarts, perform sequential operations. Avoid upgrading or configuring multiple tablets simultaneously. Wait for one tablet to complete the upgrade/configuration, confirm the restart status, and then proceed to the next tablet. Refer to the "Restart Result Confirmation" section in the version upgrade and update configuration file documents for detailed procedures.
- Regular restart: If there's no need for a version upgrade or updating the configuration file, follow the steps outlined in the "Stop Cluster" and "Start Cluster" sections mentioned above.