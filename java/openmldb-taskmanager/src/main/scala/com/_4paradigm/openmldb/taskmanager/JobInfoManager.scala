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
import com._4paradigm.openmldb.taskmanager.dao.{JobIdGenerator, JobInfo}
import com._4paradigm.openmldb.taskmanager.yarn.YarnClientUtil
import org.slf4j.LoggerFactory
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path}

import java.net.URI
import java.sql.{PreparedStatement, ResultSet, SQLException}
import java.util.Calendar
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.mutable
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration

import java.io.File

object JobInfoManager {
  private val logger = LoggerFactory.getLogger(this.getClass)

  // TODO: Check if internal table has been created
  val dbName = "__INTERNAL_DB"
  val tableName = "JOB_INFO"

  val option = new SdkOption
  option.setZkCluster(TaskManagerConfig.ZK_CLUSTER)
  option.setZkPath(TaskManagerConfig.ZK_ROOT_PATH)
  val sqlExecutor = new SqlClusterExecutor(option)

  def createJobSystemTable(): Unit = {
    // TODO: Check db
    sqlExecutor.createDB(dbName)

    val createTableSql = s"CREATE TABLE $tableName ( \n" +
      "                   id int,\n" +
      "                   job_type string,\n" +
      "                   state string,\n" +
      "                   start_time timestamp,\n" +
      "                   end_time timestamp,\n" +
      "                   parameter string,\n" +
      "                   cluster string,\n" +
      "                   application_id string,\n" +
      "                   error string,\n" +
      "                   index(key=id, ttl=1, ttl_type=latest)\n" +
      "                   )"
    // TODO: Check table
    sqlExecutor.executeDDL(dbName, createTableSql)
  }

  def createJobInfo(jobType: String, args: List[String] = List(), sparkConf: Map[String, String] = Map()): JobInfo = {
    val jobId = JobIdGenerator.getUniqueId
    val startTime = new java.sql.Timestamp(Calendar.getInstance.getTime().getTime())
    val initialState = "Submitted"
    val parameter = if (args != null && args.length>0) args.mkString(",") else ""
    val cluster = sparkConf.getOrElse("spark.master", TaskManagerConfig.SPARK_MASTER)
    // TODO: Require endTime is not null for insert sql
    val defaultEndTime = startTime

    // TODO: Parse if run in yarn or local
    val jobInfo = new JobInfo(jobId, jobType, initialState, startTime, defaultEndTime, parameter, cluster, "", "")
    jobInfo.sync()
    jobInfo
  }

  def getJobs(onlyUnfinished: Boolean): java.util.List[JobInfo] = {
    val jobs = if (onlyUnfinished) JobInfoManager.getUnfinishedJobs else JobInfoManager.getAllJobs
    jobs.asJava
  }

  def getAllJobs(): List[JobInfo] = {
    val sql = s"SELECT * FROM $tableName"
    val rs = sqlExecutor.executeSQL(dbName, sql)
    resultSetToJobs(rs)
  }

  def getUnfinishedJobs(): List[JobInfo] = {
    // TODO: Now we can not add index for `state` and run sql with
    //  s"SELECT * FROM $tableName WHERE state NOT IN (${JobInfo.FINAL_STATE.mkString(",")})"
    val sql = s"SELECT * FROM $tableName"
    val rs = sqlExecutor.executeSQL(dbName, sql)

    val jobs = mutable.ArrayBuffer[JobInfo]()
    while(rs.next()) {
      if (!JobInfo.FINAL_STATE.contains(rs.getString(3).toLowerCase)) { // Check if state is finished
        val jobInfo = new JobInfo(rs.getInt(1), rs.getString(2), rs.getString(3),
          rs.getTimestamp(4), rs.getTimestamp(5), rs.getString(6),
          rs.getString(7), rs.getString(8), rs.getString(9))
        jobs.append(jobInfo)
      }
    }

    jobs.toList
  }

  def stopJob(jobId: Int): JobInfo = {
    val sql = s"SELECT * FROM $tableName WHERE id = $jobId"
    val rs = sqlExecutor.executeSQL(dbName, sql)

    val jobInfo = if (rs.getFetchSize == 0) {
      throw new Exception("Job does not exist for id: " + jobId)
    } else if (rs.getFetchSize == 1) {
      resultSetToJob(rs)
    } else {
      throw new Exception("Job num is more than 1, get " + rs.getFetchSize)
    }

    if (jobInfo.isYarnJob && jobInfo.getApplicationId != null) {
      YarnClientUtil.killYarnJob(jobInfo.getApplicationId)
      // TODO: Maybe start new thread to track the state
      jobInfo.setState(YarnClientUtil.getYarnJobState(jobInfo.getApplicationId).toString)
      jobInfo.sync()
    } else {
      // TODO: Set stopped state for other jobs
      jobInfo.setState("STOPPED")
      jobInfo.sync()
    }

    jobInfo
  }

  def deleteJob(jobId: Int): Unit = {
    // TODO: Can not support deleting single row row
  }

  def getJob(jobId: Int): Option[JobInfo] = {
    // TODO: Require to get only one row, https://github.com/4paradigm/OpenMLDB/issues/704
    val sql = s"SELECT * FROM $tableName WHERE id = $jobId"
    val rs = sqlExecutor.executeSQL(dbName, sql)

    if (rs.getFetchSize == 0) {
      None
    } else if (rs.getFetchSize == 1) {
      Some(resultSetToJob(rs))
    } else {
      throw new Exception("Job num is more than 1, get " + rs.getFetchSize)
    }
  }

  def syncJob(job: JobInfo): Unit = {
    // Escape double quote for generated SQL string
    val escapeErrorString = job.getError.replaceAll("\"", "\\\\\\\"")
    // TODO: Could not handle select 10 config (a="aa", b="bb");
    val insertSql =
      s"""
         | INSERT INTO $tableName VALUES
         | (${job.getId}, "${job.getJobType}", "${job.getState}", ${job.getStartTime.getTime}, ${job.getEndTime.getTime}, "${job.getParameter}", "${job.getCluster}", "${job.getApplicationId}", "${escapeErrorString}")
         |""".stripMargin

    var pstmt: PreparedStatement = null
    try {
      logger.info(s"Run insert SQL: $insertSql")
      pstmt = sqlExecutor.getInsertPreparedStmt(dbName, insertSql)
      pstmt.execute()
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
    if (rs.getFetchSize == 1) {
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
    while (rs.next()) {
      val jobInfo = new JobInfo(rs.getInt(1), rs.getString(2), rs.getString(3),
        rs.getTimestamp(4), rs.getTimestamp(5), rs.getString(6),
        rs.getString(7), rs.getString(8), rs.getString(9))
      jobs.append(jobInfo)
    }

    jobs.toList
  }

  def dropOfflineTable(db: String, table: String): Unit = {
    val tableInfo = sqlExecutor.getTableInfo(db, table)

    if (tableInfo.hasOfflineTableInfo) {
      val offlineTableInfo = tableInfo.getOfflineTableInfo

      val filePath = offlineTableInfo.getPath
      if(offlineTableInfo.getDeepCopy) {

        if (filePath.startsWith("file://")) {
          val dir = new File(filePath.substring(7))
          logger.info(s"Try to delete the path ${filePath.substring(7)}")
          FileUtils.deleteDirectory(dir)

        } else if (filePath.startsWith("hdfs://")) {
          val conf = new Configuration();
          // TODO: Get namenode uri from config file
          val namenodeUri = TaskManagerConfig.NAMENODE_URI
          val hdfs = FileSystem.get(URI.create(s"hdfs://$namenodeUri"), conf)
          hdfs.delete(new Path(filePath.substring(7)), true)

        } else {
          throw new Exception(s"Get unsupported file path: $filePath")
        }
      } else {
        logger.info(s"Do not delete file $filePath for non deep copy data")
      }
    }

  }

}
