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

package com._4paradigm.openmldb.batch

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConverters.seqAsJavaListConverter

class TestLoadDataPlan extends SparkTestSuite {

  test("Test LoadData") {
    val sess = getSparkSession

    // empty table
    val t1 = sess.emptyDataFrame

    val planner = new SparkPlanner(sess)
    var res = planner.plan("load data infile 'openmldb-batch/src/test/resources/test.csv' into table t1 options(format='csv', foo='bar', " +
      "header=false);", Map("t1" -> t1))

    // after loaded
    val output = res.getDf()
    output.show()

    // abnormal delimiter
    res = planner.plan("load data infile 'openmldb-batch/src/test/resources/test.csv' into table t1 options(format='csv', foo='bar', " +
      "header=false, delimiter=\"++\");", Map("t1" -> t1))
    res.getDf().show()
  }

}
