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

package com._4paradigm.openmldb.taskmanager

import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}
import org.scalatest.FunSuite

class TestSparkLauncherUtil extends FunSuite {

  test("Test createSparkLauncher") {
    val launcher = SparkLauncherUtil.createSparkLauncher("")
    assert(launcher!= null)
  }

  /*
  test("Test submit Spark application") {
    val mainClass = classOf[DummySparkApp].getName

    val launcher = SparkLauncherUtil.createSparkLauncher(mainClass)
    launcher.setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, System.getProperty("java.class.path"))

    val sparkAppHandle = launcher.startApplication()
    Thread.sleep(3000)

    // Submit successfully and get final state
    // TODO: get lost state since of lack of log4j jar, more info in sparkAppHandle.getError
    assert(sparkAppHandle.getState.isFinal)
  }
  */

}