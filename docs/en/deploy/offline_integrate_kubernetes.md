# [Alpha] Offline engine using Kubernetes backend (optional)

## Introduction

The OpenMLDB offline engine also offers support for integrating Kubernetes services. Users can configure Kubernetes clusters to schedule and execute big data offline tasks and utilize distributed storage services like HDFS to handle offline data management.

## Deploy Kubernetes

- To deploy Kubernetes in either stand-alone or cluster version: Please refer to [Kubernetes official documentation](https://kubernetes.io/docs/home/)

- Deploy Management of the Operator in Spark: Please refer to the [spark-on-k8s-operator official documentation](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator). Below is an example of `Helm` deploying to the `default` command space, which can modify command space and permission information as needed.

```
helm install my-release spark-operator/spark-operator --namespace default --create-namespace --set webhook.enable=true

kubectl create serviceaccount spark --namespace default

kubectl create clusterrolebinding binding --clusterrole=edit --serviceaccount=default:spark
```

Once you have successfully deployed the spark operator, you can utilize the provided code examples to test the submission of Spark tasks and verify if they execute properly.

## Support HDFS

If you require configuring Kubernetes tasks to access HDFS data for reading and writing purposes, you will need to prepare a Hadoop configuration document and create a ConfigMap beforehand. Feel free to modify the ConfigMap name and file path according to your requirements. An example of the create command is provided below.

```
kubectl create configmap hadoop-config --from-file=/tmp/hadoop/etc/
```

## The configuration of TaskManager Kubernetes

In the TaskManager configuration file documentation, you have the option to specify Kubernetes-related configurations. The relevant configurations are outlined below.

| Config | Type | Note |
| ------ | ---- | ---- |
| spark.master | String | Supports "kuberenetes" or "k8s" |
| offline.data.prefix | String | Suggest to use the path of HDFS |
| k8s.hadoop.configmap | String | Default setting as "hadoop-config" |
| k8s.mount.local.path | String | Default setting as "/tmp" |

When using Kubernetes mode, it is advised to configure the offline storage path as an HDFS path. This ensures that user computing tasks run smoothly on the cluster version. Failure to do so may result in issues with reading and writing data for the tasks. An example of the configuration is provided below.

```
offline.data.prefix=hdfs:///foo/bar/
```

## Submission and management of the task

Once you have configured TaskManager and Kubernetes, you can submit offline tasks through the command line. The process is similar to using Local or Yarn mode. This functionality is available in SQL command line clients as well as through SDKs in different programming languages.

Here is an example of inserting a task after submitting the data.

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
