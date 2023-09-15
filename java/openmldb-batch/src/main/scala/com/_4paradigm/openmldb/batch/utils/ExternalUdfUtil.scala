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

package com._4paradigm.openmldb.batch.utils

import com._4paradigm.hybridse.vm.Engine
import com._4paradigm.std.VectorDataType
import org.slf4j.LoggerFactory
import scala.reflect.io.File

object ExternalUdfUtil {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /***
   * Get the actual path for driver.
   *
   * @param absoluteFilePath
   * @return
   */
  def getDriverFilePath(isYarn: Boolean, isCluster: Boolean, absoluteFilePath: String): String = {
    if (isYarn && isCluster) {
      absoluteFilePath.split("/").last
    } else {
      absoluteFilePath
    }
  }

  /**
   * Get the actual path for executor.
   *
   * @param absoluteFilePath
   * @return
   */
  def getExecutorFilePath(isYarn: Boolean, absoluteFilePath: String): String = {
    if (isYarn) {
      absoluteFilePath.split("/").last
    } else {
      absoluteFilePath
    }
  }

  def executorRegisterExternalUdf(externalFunMap: Map[String, com._4paradigm.openmldb.proto.Common.ExternalFun],
                                 taskmanagerExternalFunctionDir: String,
                                 isYarnMode: Boolean): Unit = {

    if (isYarnMode) {
      // No need to register udf again for local model because driver has registered
      // Should load udf so after openmldb so library has been loaded and SQL jit module is not initialized
      for ((functionName, functionProto) <- externalFunMap){
        val argsDataType = new VectorDataType()
        functionProto.getArgTypeList.forEach(dataType => {
          argsDataType.add(DataTypeUtil.protoTypeToOpenmldbType(dataType))
        })
        val returnDataType = DataTypeUtil.protoTypeToOpenmldbType(functionProto.getReturnType)

        // Get the correct file name which is submitted by spark-submit
        val soFileName = functionProto.getFile.split("/").last
        val absoluteSoFilePath = if (taskmanagerExternalFunctionDir.endsWith("/")) {
          taskmanagerExternalFunctionDir + soFileName
        } else {
          taskmanagerExternalFunctionDir + "/" + soFileName
        }

        val executorSoFilePath = ExternalUdfUtil.getExecutorFilePath(isYarnMode, absoluteSoFilePath)
        logger.warn("Get executor so file path: " + executorSoFilePath)

        if (File(executorSoFilePath).exists) {
            val ret = Engine.RegisterExternalFunction(functionName, returnDataType, functionProto.getReturnNullable,
            argsDataType, functionProto.getArgNullable, functionProto.getIsAggregate,
            executorSoFilePath)
            if (!ret.isOK) {
                logger.warn("Register external function failed: " + functionName + ", " + ret.str)
            }
        } else {
          logger.error("The dynamic library file does not exit in " + executorSoFilePath)
        }
      }
    }
  }

}
