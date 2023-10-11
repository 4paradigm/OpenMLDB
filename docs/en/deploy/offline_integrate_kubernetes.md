# [Alpha] Offline engine using Kubernetes backend (optional)

## Introduction

The OpenMLDB offline engine offers support for the integration of Kubernetes services. Users can configure Kubernetes clusters to schedule and execute offline tasks and utilize distributed storage services like HDFS to handle offline data management.

## Deploy Kubernetes

- To deploy Kubernetes in either stand-alone or cluster: please refer to [Kubernetes official documentation](https://kubernetes.io/docs/home/)

- Deploy Kubernetes operator for Apache Spark: please refer to the [spark-on-k8s-operator official documentation](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator). Below is an example of `Helm` deploying to the `default` command space, which can modify command space and permission information as needed.

```
helm install my-release spark-operator/spark-operator --namespace default --create-namespace --set webhook.enable=true

kubectl create serviceaccount spark --namespace default

kubectl create clusterrolebinding binding --clusterrole=edit --serviceaccount=default:spark
```

Once you have successfully deployed the spark operator, you can utilize the provided code examples to test the submission of Spark tasks and verify if they execute properly.

## HDFS Support

If you want to configure Kubernetes tasks to access HDFS data, you will need to prepare a Hadoop configuration file and create a ConfigMap beforehand. An example command is shown below, you can modify the ConfigMap name and file path accordingly.

```
kubectl create configmap hadoop-config --from-file=/tmp/hadoop/etc/
```

## Configure Kubernetes with TaskManager

You can specify Kubernetes configurations in the TaskManager configuration file. The relevant configurations are outlined below.

| Config | Type | Note |
| ------ | ---- | ---- |
| spark.master | String | Supports "kuberenetes" or "k8s" |
| offline.data.prefix | String | Recommended to use HDFS path |
| k8s.hadoop.configmap | String | Default setting as "hadoop-config" |
| k8s.mount.local.path | String | Default setting as "/tmp" |

When in Kubernetes mode, it is recommended to configure the offline storage path as HDFS path, to ensure smooth execution in the cluster. Failure to do so may result in failure to read/write data. An example of the configuration is provided below.

```
offline.data.prefix=hdfs:///foo/bar/
```

## Submission and management of the task

Once you have configured TaskManager and Kubernetes, you can submit offline tasks through command line. The process is similar to using Local or Yarn mode. It can be used in SQL CLI and SDKs of various languages.

Here is an example of submitting a load data task.

```
LOAD DATA INFILE 'hdfs:///hosts' INTO TABLE db1.t1 OPTIONS(delimiter = ',', mode='overwrite');
```

Checking of Hadoop ConfigMap content.

```
kubectl get configmap hdfs-config -o yaml
```

Checking of Spark task and Pod content and log.

```
kubectl get SparkApplication

kubectl get pods
```
