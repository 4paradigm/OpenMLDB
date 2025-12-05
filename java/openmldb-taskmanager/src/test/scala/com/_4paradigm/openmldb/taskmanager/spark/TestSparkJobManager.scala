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
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}
import org.scalatest.{FunSuite, Ignore}

// TODO: need to run with SPARK_HOME
@Ignore
class TestSparkJobManager extends FunSuite {

  test("Test createSparkLauncher") {
    val launcher = SparkJobManager.createSparkLauncher("")
    assert(launcher!= null)
  }

  test("Test submitSparkJob") {
    val mainClass = classOf[DummySparkApp].getName
    //val mainClass = "com._4paradigm.openmldb.batchjob.SparkVersionApp"

    val jobType = "DummySparkApp"
    val sparkConf = Map(SparkLauncher.DRIVER_EXTRA_CLASSPATH -> System.getProperty("java.class.path"))

    SparkJobManager.submitSparkJob(jobType, mainClass, List[String](), "", "", sparkConf)

    JobInfoManager.getAllJobs().map(println)
    Thread.sleep(5000)
    JobInfoManager.getAllJobs().map(println)

  }

  test("Test submit Spark application") {
    val mainClass = classOf[DummySparkApp].getName

    val launcher = SparkJobManager.createSparkLauncher(mainClass)
    launcher.setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, System.getProperty("java.class.path"))

    val sparkAppHandle = launcher.startApplication()
    Thread.sleep(3000)

    // Submit successfully and get final state
    assert(!sparkAppHandle.getError.isPresent)
    assert(sparkAppHandle.getState == SparkAppHandle.State.LOST)
  }

}