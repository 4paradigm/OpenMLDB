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

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext
import io.fabric8.kubernetes.client.{Config, DefaultKubernetesClient}
import org.slf4j.LoggerFactory


class K8sJobManager(val namespace:String = "default",
                    val dockerImage: String = "registry.cn-shenzhen.aliyuncs.com/tobegit3hub/openmldb-spark") {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // Configure and create a Kubernetes client
  val k8sConfig = Config.autoConfigure(null)
  val client = new DefaultKubernetesClient(k8sConfig)

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
        |    - name: "host-local"
        |      hostPath:
        |        path: ${jobConfig.mountLocalPath}
        |        type: Directory
        |  driver:
        |    cores: ${jobConfig.driverCores}
        |    memory: "${jobConfig.driverMemory}"
        |    labels:
        |      version: 3.1.1
        |    serviceAccount: spark
        |    volumeMounts:
        |      - name: "host-local"
        |        mountPath: ${jobConfig.mountLocalPath}
        |  executor:
        |    cores: ${jobConfig.executorCores}
        |    instances: ${jobConfig.executorNum}
        |    memory: "${jobConfig.executorMemory}"
        |    labels:
        |      version: 3.1.1
        |    volumeMounts:
        |      - name: "host-local"
        |        mountPath: ${jobConfig.mountLocalPath}
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
    logger.info(s"SparkApplication created: ${createdResource}")
  }

  def close(): Unit = {
    // Close the Kubernetes client
    client.close()
  }

}