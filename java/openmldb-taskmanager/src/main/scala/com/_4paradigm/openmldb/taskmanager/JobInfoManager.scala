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

import com._4paradigm.openmldb.sdk.SdkOption
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor
import com._4paradigm.openmldb.taskmanager.config.TaskManagerConfig
import com._4paradigm.openmldb.taskmanager.dao.JobInfo

import java.sql.{PreparedStatement, ResultSet, SQLException, Timestamp}
import java.util.Calendar
import scala.collection.mutable

object JobInfoManager {
  // TODO: Check if internal table has been created
  val dbName = "__INTERNAL_DB"
  val tableName = "JOB_INFO"

  val option = new SdkOption
  option.setZkCluster(TaskManagerConfig.ZK_CLUSTER)
  option.setZkPath(TaskManagerConfig.ZK_ROOTPATH)
  val sqlExecutor = new SqlClusterExecutor(option)

  def createJobSystemTable(): Unit = {
    // TODO: Check db
    println(sqlExecutor.createDB(dbName))

    val createTableSql = s"CREATE TABLE $tableName (id int,\n" +
      "                   job_type string,\n" +
      "                   state string,\n" +
      "                   start_time timestamp,\n" +
      "                   end_time timestamp,\n" +
      "                   parameter string,\n" +
      "                   cluster string,\n" +
      "                   application_id string,\n" +
      "                   error string,\n" +
      "                   index(name=index1, key=state, ttl=1, ttl_type=latest),\n" +
      "                   index(name=index2, key=id, ttl=1, ttl_type=latest))"

    // TODO: Check table
    println(sqlExecutor.executeDDL(dbName, createTableSql))
  }

  def createJobInfo(jobType: String): JobInfo = {
    // TODO: Generate unique job id
    val jobId = 1
    val startTime = new java.sql.Timestamp(Calendar.getInstance.getTime().getTime())

    val jobInfo = new JobInfo(jobId, jobType, "SUBMITTED", startTime, new Timestamp(0l), "", "Yarn", "", "")
    jobInfo.sync()
    jobInfo
  }

  def getAllJobs(): List[JobInfo] = {
    val sql = s"SELECT * FROM $tableName"
    val rs = sqlExecutor.executeSQL(dbName, sql)
    resultSetToJobs(rs)
  }

  def getUnfinishedJobs(): List[JobInfo] = {
    // TODO: Require to support multiple indexes when creating table, https://github.com/4paradigm/OpenMLDB/issues/763
    val sql = s"SELECT * FROM $tableName WHERE state NOT IN (${JobInfo.FINAL_STATE.mkString(",")}) "
    val rs = sqlExecutor.executeSQL(dbName, sql)
    resultSetToJobs(rs)
  }

  def getJob(jobId: Int): JobInfo = {
    // TODO: Require to get only one row, https://github.com/4paradigm/OpenMLDB/issues/704
    val sql = s"SELECT * FROM $tableName WHERE id = $jobId"
    val rs = sqlExecutor.executeSQL(dbName, sql)
    resultSetToJob(rs)
  }

  def syncJob(job: JobInfo): Unit = {
    val insertSql =
      s"""
         | INSERT INTO $tableName VALUES
         | (${job.getId}, "${job.getJobType}", "${job.getState}", ${job.getStartTime.getTime}, ${job.getEndTime.getTime}, "${job.getParameter}", "${job.getCluster}", "${job.getApplicationId}", "${job.getError}")
         |""".stripMargin

    var pstmt: PreparedStatement = null
    try {
      pstmt = sqlExecutor.getInsertPreparedStmt(dbName, insertSql)
      println("Insert: " + pstmt.execute)
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    } finally if (pstmt != null) try {
      pstmt.close()
    }
    catch {
      case throwables: SQLException =>
        throwables.printStackTrace()
    }
  }

  def resultSetToJob(rs: ResultSet): JobInfo = {
    if(rs.getFetchSize == 1) {
      if (rs.next()) {
       return new JobInfo(rs.getInt(1), rs.getString(2), rs.getString(3),
          rs.getTimestamp(4), rs.getTimestamp(5), rs.getString(6),
          rs.getString(7), rs.getString(8), rs.getString(9))
      }
    }
    throw new Exception(s"Job num is ${rs.getFetchSize}")
  }

  def resultSetToJobs(rs: ResultSet): List[JobInfo] = {
    val jobs = mutable.ArrayBuffer[JobInfo]()
    while(rs.next()) {
      val jobInfo = new JobInfo(rs.getInt(1), rs.getString(2), rs.getString(3),
        rs.getTimestamp(4), rs.getTimestamp(5), rs.getString(6),
        rs.getString(7), rs.getString(8), rs.getString(9))
      jobs.append(jobInfo)
    }

    jobs.toList
  }

}
