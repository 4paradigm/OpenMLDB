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

import java.nio.ByteBuffer

import com._4paradigm.hybridse.codec.Row
import com._4paradigm.hybridse.vm.CoreAPI
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter

object UnsafeRowUtil {

  val HybridseRowHeaderSize = 6

  /** Convert Spark InternalRow to HybridSE row byte arrays.
   *
   * @param internalRow the input row object.
   */
  def internalRowToHybridseRowBytes(internalRow: InternalRow): Array[Byte] = {
    val unsafeRow = internalRow.asInstanceOf[UnsafeRow]

    // Get input UnsafeRow bytes
    val inputRowBytes = unsafeRow.getBytes
    val inputRowSize = inputRowBytes.size

    // Add the header and memcpy bytes for input row, no need to set version and size in header
    val hybridseRowHeaderBytes = ByteBuffer.allocate(HybridseRowHeaderSize)
    ByteBuffer.allocate(HybridseRowHeaderSize + inputRowSize).put(hybridseRowHeaderBytes).put(inputRowBytes).array()
  }


  /** Convert HybridSE row to Spark InternalRow.
   *
   * The HybridSE row is compatible with UnsafeRow bytes but has 6 bytes as header.
   */
  def hybridseRowToInternalRow(hybridseRow: Row, columnNum: Int): InternalRow = {
    val hybridseRowWithoutHeaderSize = hybridseRow.size - UnsafeRowUtil.HybridseRowHeaderSize
    val unsafeRowWriter = new UnsafeRowWriter(columnNum, hybridseRowWithoutHeaderSize)
    unsafeRowWriter.reset()
    unsafeRowWriter.zeroOutNullBytes()

    // Copy and remove header for output row
    CoreAPI.CopyRowToUnsafeRowBytes(hybridseRow, unsafeRowWriter.getBuffer, hybridseRowWithoutHeaderSize)

    // Convert to InternalRow
    val unsafeRow = unsafeRowWriter.getRow
    unsafeRow.asInstanceOf[InternalRow]
  }

}
