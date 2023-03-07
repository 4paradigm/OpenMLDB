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

package com._4paradigm.openmldb.taskmanager.server.impl

import com._4paradigm.openmldb.proto.TaskManager
import com._4paradigm.openmldb.sdk.SdkOption
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor
import com._4paradigm.openmldb.taskmanager.config.TaskManagerConfig
import com._4paradigm.openmldb.taskmanager.{JobInfoManager, LogManager}
import com._4paradigm.openmldb.taskmanager.server.StatusCode
import org.scalatest.FunSuite
import com._4paradigm.openmldb.proto.NS
import org.apache.spark.sql.SparkSession


class TestTaskManagerImpl extends FunSuite {

  test("Test ShowJobs and check status") {
    val impl = new TaskManagerImpl()

    val request = TaskManager.ShowJobsRequest.newBuilder.build
    val response = impl.ShowJobs(request)

    assert(response.getCode == StatusCode.SUCCESS)
  }

  test("Test GetVersion") {
    val impl = new TaskManagerImpl()

    val request = TaskManager.EmptyMessage.newBuilder().build()
    val response = impl.GetVersion(request)

    assert(!response.getBatchVersion.equals("unknown"))
    // TODO(tobe): Notice that we can not get TaskManager version in unit test
    //assert(response.getTaskmanagerVersion.equals("unknown"))
  }

  test("Test ShowJob with non-existent id") {
    val impl = new TaskManagerImpl()

    val jobId = -1
    val request = TaskManager.ShowJobRequest.newBuilder.setId(jobId).build()
    val response = impl.ShowJob(request)

    assert(response.getCode() == StatusCode.FAILED)
  }

  test("Test ShowBatchVersion") {
    val impl = new TaskManagerImpl()

    val request = TaskManager.ShowBatchVersionRequest.newBuilder.setSyncJob(true).build()
    val response = impl.ShowBatchVersion(request)

    val jobId = response.getJob().getId()
    val finalJobInfo = JobInfoManager.getJob(jobId).get
    val jobLog = LogManager.getJobLog(jobId)

    assert(response.getCode == StatusCode.SUCCESS)
    assert(finalJobInfo.isSuccess)
    assert(!jobLog.equals(""))
  }

  // TODO(tobe): RunBatchSql requires TaskManager server to get job log and do not test now

  test("Test RunBatchAndShow") {
    val impl = new TaskManagerImpl()

    val sql = "SELECT 100"
    val request = TaskManager.RunBatchAndShowRequest.newBuilder.setSyncJob(true).setSql(sql).build()
    val response = impl.RunBatchAndShow(request)

    val jobId = response.getJob().getId()
    val finalJobInfo = JobInfoManager.getJob(jobId).get
    val jobLog = LogManager.getJobLog(jobId)

    assert(response.getCode == StatusCode.SUCCESS)
    assert(finalJobInfo.isSuccess)

    val expectedJobLog =
      """
        |+---+
        ||100|
        |+---+
        |+---+
        |""".stripMargin.trim
    assert(jobLog.trim.equals(expectedJobLog))
  }

  test("Test ImportOnlineData") {
    val impl = new TaskManagerImpl()

    // Create test table
    val testDb = "db1"
    val testTable = "t1"
    val option = new SdkOption
    option.setZkCluster(TaskManagerConfig.ZK_CLUSTER)
    option.setZkPath(TaskManagerConfig.ZK_ROOT_PATH)
    val executor = new SqlClusterExecutor(option)
    executor.createDB(testDb)
    executor.executeDDL(testDb, s"drop table $testTable")
    executor.executeDDL(testDb, s"create table $testTable(c1 int)")

    // Run SQL
    val path = getClass.getResource("/c1_table.csv").getPath
    val sql = s"LOAD DATA INFILE 'file://$path' INTO TABLE $testTable OPTIONS (header=true, mode='append')"

    val request = TaskManager.ImportOnlineDataRequest.newBuilder
      .setSyncJob(true)
      .setSql(sql)
      .setDefaultDb(testDb)
      .build()
    val response = impl.ImportOnlineData(request)

    val jobId = response.getJob().getId()
    val finalJobInfo = JobInfoManager.getJob(jobId).get

    // Check status
    assert(response.getCode == StatusCode.SUCCESS)
    assert(finalJobInfo.isSuccess)

    // Check table result
    executor.executeSQL(testDb, "SET @@execute_mode='online'")
    val rs = executor.executeSQL(testDb, s"select count(*) from $testTable")
    rs.next()
    val count = rs.getLong(1)
    assert(count == 5)

    executor.executeDDL(testDb, s"drop table $testTable")
  }


  test("Test ImportOfflineData") {
    val impl = new TaskManagerImpl()

    // Create test table
    val testDb = "db1"
    val testTable = "t1"
    val option = new SdkOption
    option.setZkCluster(TaskManagerConfig.ZK_CLUSTER)
    option.setZkPath(TaskManagerConfig.ZK_ROOT_PATH)
    val executor = new SqlClusterExecutor(option)

    executor.createDB(testDb)
    executor.executeDDL(testDb, s"drop table $testTable")
    executor.executeDDL(testDb, s"create table $testTable(c1 int)")

    // Run SQL
    val path = getClass.getResource("/c1_table.csv").getPath
    val sql = s"LOAD DATA INFILE 'file://$path' INTO TABLE $testTable OPTIONS (header=true, mode='append')"

    val request = TaskManager.ImportOfflineDataRequest.newBuilder
      .setSyncJob(true)
      .setSql(sql)
      .setDefaultDb(testDb)
      .build()
    val response = impl.ImportOfflineData(request)

    val jobId = response.getJob().getId()
    val finalJobInfo = JobInfoManager.getJob(jobId).get

    // Check status
    assert(response.getCode == StatusCode.SUCCESS)
    assert(finalJobInfo.isSuccess)

    // Check offline table result
    val tableInfo: NS.TableInfo = executor.getTableInfo(testDb, testTable)
    val offlinePath: String = tableInfo.getOfflineTableInfo().getPath()
    assert(offlinePath != null && !offlinePath.equals(""))

    val spark = SparkSession.builder().master("local").getOrCreate()
    val df = spark.read.parquet(offlinePath)
    assert(df.count() == 5)

    executor.executeDDL(testDb, s"drop table $testTable")
  }

}
