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

import org.scalatest.{FunSuite, Ignore}

@Ignore
class TestK8sJobManager extends FunSuite {

  test("Test K8sJobManager constructor and close") {
    val manager = new K8sJobManager()
    manager.close()
  }

  test("Test listAllPods") {
    val manager = new K8sJobManager()

    manager.listAllPods()

    manager.close()
  }

  test("Test submitJob") {
    val manager = new K8sJobManager()

    val jobConfig = OpenmldbOfflineJobConfig(
      jobName = "job",
      mainClass = "com._4paradigm.openmldb.batch.tools.RunOpenmldbSql",
      mainJarFile = "local:///opt/spark/jars/openmldb-batch-0.7.2.jar",
      arguments = List("SELECT * from db1.t1"),
      sparkConf = Map("spark.openmldb.zk.cluster" -> "127.0.0.1:2181", "spark.openmldb.zk.root.path" -> "/openmldb"),
      mountLocalPath = "/tmp/openmldb_offline_data/"
    )
    manager.submitJob(jobConfig)

    manager.close()
  }

}