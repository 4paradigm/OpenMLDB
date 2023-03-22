/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.taskmanager.k8s

import com._4paradigm.openmldb.taskmanager.JobInfoManager
import com._4paradigm.openmldb.taskmanager.config.TaskManagerConfig
import com._4paradigm.openmldb.taskmanager.dao.JobInfo
import com._4paradigm.openmldb.taskmanager.k8s.K8sJobManager.getDrvierPodName
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext
import io.fabric8.kubernetes.client.{Config, DefaultKubernetesClient, Watcher, WatcherException}
import org.slf4j.LoggerFactory

import java.util.Calendar
import scala.collection.mutable


object K8sJobManager {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def getK8sJobName(jobId: Int): String = {
    s"openmldb-job-$jobId"
  }

  def getDrvierPodName(jobId: Int): String = {
    s"${getK8sJobName(jobId)}-driver"
  }

  def submitSparkJob(jobType: String, mainClass: String,
    args: List[String] = List(),
    localSqlFile: String = "",
    sparkConf: Map[String, String] = Map(),
    defaultDb: String = "",
    blocking: Boolean = false): JobInfo = {

    val jobInfo = JobInfoManager.createJobInfo(jobType, args, sparkConf)

    val jobName = getK8sJobName(jobInfo.getId)
    jobInfo.setApplicationId(jobName)

    val finalSparkConf: mutable.Map[String, String] = mutable.Map(sparkConf.toSeq: _*)

    val defaultSparkConfs = TaskManagerConfig.SPARK_DEFAULT_CONF.split(";")
    defaultSparkConfs.map(sparkConf => {
      if (sparkConf.nonEmpty) {
        val kvList = sparkConf.split("=")
        val key = kvList(0)
        val value = kvList.drop(1).mkString("=")
        finalSparkConf.put(key, value)
      }
    })

    if (TaskManagerConfig.SPARK_EVENTLOG_DIR.nonEmpty) {
      finalSparkConf.put("spark.eventLog.enabled", "true")
      finalSparkConf.put("spark.eventLog.dir", TaskManagerConfig.SPARK_EVENTLOG_DIR)
    }

    // Set ZooKeeper config for openmldb-batch jobs
    if (TaskManagerConfig.ZK_CLUSTER.nonEmpty && TaskManagerConfig.ZK_ROOT_PATH.nonEmpty) {
      finalSparkConf.put("spark.openmldb.zk.cluster", TaskManagerConfig.ZK_CLUSTER)
      finalSparkConf.put("spark.openmldb.zk.root.path", TaskManagerConfig.ZK_ROOT_PATH)
    }

    if (defaultDb.nonEmpty) {
      finalSparkConf.put("spark.openmldb.default.db", defaultDb)
    }

    if(TaskManagerConfig.ENABLE_HIVE_SUPPORT) {
      finalSparkConf.put("spark.sql.catalogImplementation", "hive")
    }

    val manager = new K8sJobManager()

    // TODO: Support hdfs later
    val mountLocalPath = if (TaskManagerConfig.OFFLINE_DATA_PREFIX.startsWith("file://")) {
      TaskManagerConfig.OFFLINE_DATA_PREFIX.drop(7)
    } else {
      logger.warn("offline data prefix should start with file:// for K8S jobs, mount /tmp instead")
      "/tmp"
    }

    val jobConfig = K8sJobConfig(
      jobName = jobName,
      mainClass = mainClass,
      mainJarFile = "local:///opt/spark/jars/openmldb-batchjob-0.7.2-SNAPSHOT.jar",
      arguments = args,
      sparkConf = finalSparkConf.toMap,
      mountLocalPath = mountLocalPath
    )
    manager.submitJob(jobConfig)

    // Update K8S job status
    manager.waitAndWatch(jobInfo: JobInfo)

    jobInfo
  }
}

class K8sJobManager(val namespace:String = "default",
                    val dockerImage: String = "registry.cn-shenzhen.aliyuncs.com/tobegit3hub/openmldb-spark") {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // TODO: Configure and create a Kubernetes client from TaskManagerConfig
  val k8sConfig = Config.autoConfigure(null)
  val client =   new DefaultKubernetesClient(k8sConfig)

  def listAllPods(): Unit = {
    // List Pods in the specified namespace
    val pods = client.pods().inNamespace(namespace).list().getItems

    // Print the Pod names
    println(s"Pods in namespace '$namespace':")
    for (pod <- pods.toArray(new Array[Pod](pods.size()))) {
      println(s" - ${pod.getMetadata.getName}")
    }
  }

  def submitJob(jobConfig: K8sJobConfig): Unit = {
    // Define the SparkApplication YAML
    val sparkApplicationYaml =
      s"""
        |apiVersion: "sparkoperator.k8s.io/v1beta2"
        |kind: SparkApplication
        |metadata:
        |  name: ${jobConfig.jobName}
        |  namespace: ${namespace}
        |spec:
        |  type: Scala
        |  mode: cluster
        |  image: ${dockerImage}
        |  imagePullPolicy: Always
        |  mainClass: ${jobConfig.mainClass}
        |  mainApplicationFile: ${jobConfig.mainJarFile}
        |  arguments: ${K8sYamlUtil.generateArgumentsString(jobConfig.arguments)}
        |  sparkConf: ${K8sYamlUtil.generateSparkConfString(jobConfig.sparkConf)}
        |  sparkVersion: "3.1.1"
        |  restartPolicy:
        |    type: Never
        |  volumes:
        |    - name: host-local
        |      hostPath:
        |        path: ${jobConfig.mountLocalPath}
        |        type: Directory
        |    - name: hadoop-config
        |      configMap:
        |        name: hadoop-config
        |  driver:
        |    cores: ${jobConfig.driverCores}
        |    memory: "${jobConfig.driverMemory}"
        |    labels:
        |      version: 3.1.1
        |    serviceAccount: spark
        |    volumeMounts:
        |      - name: host-local
        |        mountPath: ${jobConfig.mountLocalPath}
        |      - name: hadoop-config
        |        mountPath: /etc/hadoop/conf
        |    env:
        |      - name: HADOOP_CONF_DIR
        |        value: /etc/hadoop/conf
        |  executor:
        |    cores: ${jobConfig.executorCores}
        |    instances: ${jobConfig.executorNum}
        |    memory: "${jobConfig.executorMemory}"
        |    labels:
        |      version: 3.1.1
        |    volumeMounts:
        |      - name: host-local
        |        mountPath: ${jobConfig.mountLocalPath}
        |      - name: hadoop-config
        |        mountPath: /etc/hadoop/conf
        |    env:
        |      - name: HADOOP_CONF_DIR
        |        value: /etc/hadoop/conf
      """.stripMargin

    // Create a CustomResourceDefinitionContext for the SparkApplication
    val crdContext = new CustomResourceDefinitionContext.Builder()
      .withGroup("sparkoperator.k8s.io")
      .withPlural("sparkapplications")
      .withScope("Namespaced")
      .withVersion("v1beta2")
      .withKind("SparkApplication")
      .build()

    // Create the SparkApplication resource
    val createdResource = client.customResource(crdContext).create(namespace, sparkApplicationYaml)

    // Print the created SparkApplication
    logger.info(s"SparkApplication created: $createdResource")
  }

  def close(): Unit = {
    // Close the Kubernetes client
    client.close()
  }

  def waitAndWatch(jobInfo: JobInfo, timeout: Long = 5000): Unit = {
    val startTime = System.currentTimeMillis()

    val podName = getDrvierPodName(jobInfo.getId)
    var pod = client.pods().inNamespace(namespace).withName(podName).get()

    while (pod == null) {
      // Sleep to wait pod to be created
      if (System.currentTimeMillis() - startTime >= timeout) {
        close()
        throw new Exception(s"Pod $podName not found when timeout")
      } else {
        logger.info(s"Sleep 1 second and wait for pod $podName to be created")
        Thread.sleep(1000)
        pod = client.pods().inNamespace(namespace).withName(podName).get()
      }
    }

    watchPodStatus(jobInfo)
  }

  /**
   * Watch the status of the pod.
   * Notice that we should not close the client when updating the status of the job.
   *
   * @param jobInfo
   */
  def watchPodStatus(jobInfo: JobInfo): Unit = {

    val podName = getDrvierPodName(jobInfo.getId)
    val pod = client.pods().inNamespace(namespace).withName(podName).get()

    if (pod == null) {
      //close()
      throw new Exception(s"Pod $podName not found")
    }

    client.pods().inNamespace(namespace).withName(podName).watch(new Watcher[Pod] {
      override def eventReceived(action: Watcher.Action, resource: Pod): Unit = {
        // handle pod status change event
        if (resource.getStatus.getPhase.equals("Succeeded")) {
          jobInfo.setState("finished")
          client.close()
        } else if (resource.getStatus.getPhase.equals("Failed")) {
          jobInfo.setState("failed")
          client.close()
        } else if (resource.getStatus.getPhase.equals("Pending")) {
          jobInfo.setState("pending")
        } else if (resource.getStatus.getPhase.equals("Running")) {
          jobInfo.setState("running")
        } else {
          logger.warn(s"Pod ${resource.getMetadata.getName} status changed to ${resource.getStatus.getPhase} " +
            s"but not update job state")
        }

        logger.info("Job(id=%d) state change to %s".format(jobInfo.getId, jobInfo.getState))

        if (jobInfo.isFinished) {
          // Set end time
          val endTime = new java.sql.Timestamp(Calendar.getInstance.getTime().getTime())
          jobInfo.setEndTime(endTime)

          // TODO: Get error message to set

          jobInfo.sync()
        }

      }

       def onClose(e: WatcherException): Unit = {
         // handle watch close event
         if (e != null) {
           println("Pod watch closed with error: " + e.getMessage)
         } else {
           println("Pod watch closed normally")
         }
      }

    })

  }

}