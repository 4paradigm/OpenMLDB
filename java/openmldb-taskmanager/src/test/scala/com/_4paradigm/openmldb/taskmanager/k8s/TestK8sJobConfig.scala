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

import org.scalatest.FunSuite

class TestK8sJobConfig extends FunSuite {

  test("Test OpenmldbOfflineJobConfig") {
    val jobName = "jobName"
    val mainClass = "com._4paradigm.openmldb.batch.tools.RunOpenmldbSql"
    val mainJarFile = "local:///opt/spark/jars/openmldb-batch-0.7.2.jar"

    val jobConfig = K8sJobConfig(
      jobName = jobName,
      mainClass = mainClass,
      mainJarFile = mainJarFile
    )

    assert(jobConfig.jobName.equals(jobName))
    assert(jobConfig.mainClass.equals(mainClass))
    assert(jobConfig.mainJarFile.equals(mainJarFile))
    assert(jobConfig.arguments.isEmpty)
    assert(jobConfig.sparkConf.isEmpty)
    assert(jobConfig.driverCores > 0)
    assert(jobConfig.executorNum > 0)
    assert(jobConfig.executorCores > 0)
  }

}