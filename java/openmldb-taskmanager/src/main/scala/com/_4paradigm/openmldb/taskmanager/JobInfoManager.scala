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
import com._4paradigm.openmldb.taskmanager.util.HdfsUtil
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
  private val INTERNAL_DB_NAME = "__INTERNAL_DB"
  private val JOB_INFO_TABLE_NAME = "JOB_INFO"

  private val option = new SdkOption
  option.setZkCluster(TaskManagerConfig.getZkCluster)
  option.setZkPath(TaskManagerConfig.getZkRootPath)
  option.setUser(TaskManagerConfig.getUser)
  option.setPassword(TaskManagerConfig.getPassword)

  if (!TaskManagerConfig.getPassword.isEmpty) {
    option.setPassword(TaskManagerConfig.getPassword)
  }
  val sqlExecutor = new SqlClusterExecutor(option)
  sqlExecutor.executeSQL("", "set @@execute_mode='online';")

  def createJobInfo(jobType: String, args: List[String] = List(), sparkConf: Map[String, String] = Map()): JobInfo = {
    val jobId = JobIdGenerator.getUniqueId
    val startTime = new java.sql.Timestamp(Calendar.getInstance.getTime().getTime())
    val initialState = "Submitted"
    val parameter = if (args != null && args.length>0) args.mkString(",") else ""
    val cluster = sparkConf.getOrElse("spark.master", TaskManagerConfig.getSparkMaster)

    // TODO: Parse if run in yarn or local
    val jobInfo = new JobInfo(jobId, jobType, initialState, startTime, null, parameter, cluster, "", "")
    jobInfo.sync()
    jobInfo
  }

  def getJobs(onlyUnfinished: Boolean): java.util.List[JobInfo] = {
    val jobs = if (onlyUnfinished) JobInfoManager.getUnfinishedJobs else JobInfoManager.getAllJobs
    jobs.asJava
  }

  def getAllJobs(): List[JobInfo] = {
    val sql = s"SELECT * FROM $JOB_INFO_TABLE_NAME CONFIG (execute_mode = 'online')"
    val rs = sqlExecutor.executeSQL(INTERNAL_DB_NAME, sql)
    // TODO: Reorder in output, use orderby desc if SQL supported
    resultSetToJobs(rs).sortWith(_.getId > _.getId)
  }

  def getUnfinishedJobs(): List[JobInfo] = {
    // TODO: Now we can not add index for `state` and run sql with
    //  s"SELECT * FROM $tableName WHERE state NOT IN (${JobInfo.FINAL_STATE.mkString(",")})"
    val sql = s"SELECT * FROM $JOB_INFO_TABLE_NAME CONFIG (execute_mode = 'online')"
    val rs = sqlExecutor.executeSQL(INTERNAL_DB_NAME, sql)

    val jobs = mutable.ArrayBuffer[JobInfo]()
    while(rs.next()) {
      if (!JobInfo.FINAL_STATE.contains(rs.getString(3).toLowerCase)) { // Check if state is finished
        val jobInfo = new JobInfo(rs.getInt(1), rs.getString(2), rs.getString(3),
          rs.getTimestamp(4), rs.getTimestamp(5), rs.getString(6),
          rs.getString(7), rs.getString(8), rs.getString(9))
        jobs.append(jobInfo)
      }
    }

    jobs.toList.sortWith(_.getId > _.getId)
  }

  def stopJob(jobId: Int): JobInfo = {
    val sql = s"SELECT * FROM $JOB_INFO_TABLE_NAME WHERE id = $jobId CONFIG (execute_mode = 'online')"
    val rs = sqlExecutor.executeSQL(INTERNAL_DB_NAME, sql)

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
    throw new Exception("Delete job is not supported yet")
  }

  def getJob(jobId: Int): Option[JobInfo] = {
    // TODO: Require to get only one row, https://github.com/4paradigm/OpenMLDB/issues/704
    val sql = s"SELECT * FROM $JOB_INFO_TABLE_NAME WHERE id = $jobId CONFIG (execute_mode = 'online')"
    val rs = sqlExecutor.executeSQL(INTERNAL_DB_NAME, sql)

    if (rs.getFetchSize == 0) {
      None
    } else if (rs.getFetchSize == 1) {
      Some(resultSetToJob(rs))
    } else {
      throw new Exception("Job num is more than 1, get " + rs.getFetchSize)
    }
  }

  def syncJob(job: JobInfo): Unit = {
    val insertSql = s"INSERT INTO $JOB_INFO_TABLE_NAME VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    val statement = sqlExecutor.getInsertPreparedStmt(INTERNAL_DB_NAME, insertSql)
    statement.setInt(1, job.getId)
    statement.setString(2, job.getJobType)
    statement.setString(3, job.getState)
    statement.setTimestamp(4, job.getStartTime)
    statement.setTimestamp(5, job.getEndTime)
    statement.setString(6, job.getParameter)
    statement.setString(7, job.getCluster)
    statement.setString(8, job.getApplicationId)
    statement.setString(9, job.getError)

    try {
      logger.info(s"Run insert SQL with job info: $job")
      val ok = statement.execute()
      if (!ok) {
        logger.error("Fail to execute insert SQL")
      }
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    } finally {
      if (statement != null) {
        try {
          statement.close()
        } catch {
          case throwables: SQLException =>
            throwables.printStackTrace()
        }
      }
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
    // Refresh catalog to get the latest table info
    sqlExecutor.refreshCatalog()
    val tableInfo = sqlExecutor.getTableInfo(db, table)

    if (tableInfo.hasOfflineTableInfo) {
      val offlineTableInfo = tableInfo.getOfflineTableInfo

      val filePath = offlineTableInfo.getPath
      if(filePath.nonEmpty) {
        if (filePath.startsWith("file://")) {
          val dir = new File(filePath.substring(7))
          logger.info(s"Try to delete the path ${filePath.substring(7)}")
          FileUtils.deleteDirectory(dir)

        } else if (filePath.startsWith("hdfs://")) {
          logger.info(s"Try to delete the HDFS path ${filePath}")
          HdfsUtil.deleteHdfsDir(filePath)
        } else {
          throw new Exception(s"Get unsupported file path: $filePath")
        }
      } else {
        logger.info(s"Do not delete for empty hard path")
      }
    }

  }

}
