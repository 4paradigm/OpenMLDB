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

class TestK8sYamlUtil extends FunSuite {

  test("Test generateArgumentsString") {
    val data1 = List()
    val expect1 = "[]"
    assert(K8sYamlUtil.generateArgumentsString(data1).equals(expect1))

    val data2 = List("SELECT * from db1.t1")
    val expect2 = """ ["SELECT * from db1.t1"] """.trim
    assert(K8sYamlUtil.generateArgumentsString(data2).equals(expect2))

    val data3 = List("SELECT * from db1.t1", "foo", "bar")
    val expect3 = """ ["SELECT * from db1.t1","foo","bar"] """.trim
    assert(K8sYamlUtil.generateArgumentsString(data3).equals(expect3))
  }

  test("Test generateSparkConfString") {
    val data1: Map[String, String] = Map()
    val expect1 = "{}"
    assert(K8sYamlUtil.generateSparkConfString(data1).equals(expect1))

    val data2 = Map("spark.openmldb.zk.cluster" -> "127.0.0.1:2181", "spark.openmldb.zk.root.path" -> "/openmldb")
    val expect2 = """ {spark.openmldb.zk.cluster: "127.0.0.1:2181",spark.openmldb.zk.root.path: "/openmldb"} """.trim
    assert(K8sYamlUtil.generateSparkConfString(data2).equals(expect2))

    val data3 = Map("spark.openmldb.zk.cluster" -> "127.0.0.1:2181")
    val expect3 = """ {spark.openmldb.zk.cluster: "127.0.0.1:2181"} """.trim
    assert(K8sYamlUtil.generateSparkConfString(data3).equals(expect3))
  }
}