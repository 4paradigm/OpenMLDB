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

package com._4paradigm.openmldb.taskmanager.spark

import com._4paradigm.openmldb.taskmanager.{DummySparkApp, JobInfoManager}
import org.apache.spark.launcher.SparkLauncher
import org.scalatest.{FunSuite, Ignore}

// TODO: need to run with SPARK_HOME
@Ignore
class TestSparkJobManager extends FunSuite {

  test("Test submitSparkJob") {

    JobInfoManager.createJobSystemTable()

    val mainClass = classOf[DummySparkApp].getName
    //val mainClass = "com._4paradigm.openmldb.batchjob.SparkVersionApp"

    val jobType = "DummySparkApp"
    val sparkConf = Map(SparkLauncher.DRIVER_EXTRA_CLASSPATH -> System.getProperty("java.class.path"))

    SparkJobManager.submitSparkJob(mainClass, jobType, null, sparkConf)

    JobInfoManager.getAllJobs().map(println)
    Thread.sleep(5000)
    JobInfoManager.getAllJobs().map(println)

  }

}