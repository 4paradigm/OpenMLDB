# 离线引擎使用 Kubernetes 后端（可选）

## 介绍

OpenMLDB 的离线引擎也支持集成 Kubernetes 服务，用户可以配置使用 Kubernetes 集群来调度运行大数据离线任务，使用 HDFS 等分布式存储服务来管理离线数据。

## 部署 Kubernetes

参考 [Kubernetes 官方文档](https://kubernetes.io/docs/home/)部署 Kubernetes 单机版或集群版。

参考 [spark-operator 官方文档](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator)部署管理 Spark 任务的 Operator 。下面是使用 helm 部署到 default 命令空间的命令，根据需要修改命令空间以及权限信息。

```
helm install my-release spark-operator/spark-operator --namespace default --create-namespace --set webhook.enable=true

kubectl create serviceaccount spark --namespace default

kubectl create clusterrolebinding binding --clusterrole=edit --serviceaccount=default:spark
```

部署成功后，可以使用 spark-operator 提供的代码示例测试 Spark 任务是否可以正常提交。

## HDFS 支持

如果需要配置 Kubernetes 任务读写 HDFS 数据，需要提前准备 Hadoop 配置文件并且创建 ConfigMap 。根据需要可修改 ConfigMap 名称和文件路径，创建命令示例如下。

```
kubectl create configmap hadoop-config --from-file=/tmp/hadoop/etc/
```

## TaskManager 配置 Kubernetes

TaskManager 配置文件中可以指定 Kubernetes 相关配置，相关配置项如下。

| Config | Type | Note |
| ------ | ---- | ---- |
| spark.master | String | 可支持"kuberenetes"或"k8s" |
| offline.data.prefix | String | 建议使用 HDFS 路径 |
| k8s.hadoop.configmap | String | 默认为"hadoop-config" |
| k8s.mount.local.path | String | 默认为"/tmp" |

如果使用 Kubernetes 模式，用户的计算任务会运行在集群上，因此建议配置离线存储路径为 HDFS 路径，否则可能导致任务读写数据失败。配置示例如下。

```
offline.data.prefix=hdfs:///foo/bar/
```

## 任务提交和管理

配置 TaskManager 和 Kubernetes 后，可在命令行提交离线任务，用法与 Local 或 Yarn 模式一样，不仅可以在 SQL 命令行客户端中使用，也可以通过各种语言的 SDK 中使用。

例如提交数据导入任务。

```
LOAD DATA INFILE 'hdfs:///hosts' INTO TABLE db1.t1 OPTIONS(delimiter = ',', mode='overwrite');
```

检查 Hadoop ConfigMap 内容。

```
kubectl get configmap hdfs-config -o yaml
```

查看 Spark 任务以及 Pod 内容和日志。

```
kubectl get SparkApplication

kubectl get pods
```
