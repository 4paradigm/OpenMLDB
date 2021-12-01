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

import com._4paradigm.hybridse.sdk.HybridSeException

class TestLoadDataPlan extends SparkTestSuite {

  test("Test LoadData") {
    val sess = getSparkSession

    // empty table
    val t1 = sess.emptyDataFrame

    val planner = new SparkPlanner(sess)
    // use overwrite mode to avoid the error that "/tmp/test_dir" already exists,
    // should be removed after load data plan fully functioned.
    var res = planner.plan("load data infile 'openmldb-batch/src/test/resources/test.csv' into table t1 " +
      "options(format='csv', foo='bar', " +
      "header=false, mode='overwrite');", Map("t1" -> t1))
    val output = res.getDf()
    output.show()

    // strange delimiter
    res = planner.plan("load data infile 'openmldb-batch/src/test/resources/test.csv' into table t1 " +
      "options(format='csv', foo='bar', " +
      "header=false, delimiter='++', mode='overwrite');", Map("t1" -> t1))
    res.getDf().show()

    // invalid format option
    try {
      planner.plan("load data infile 'openmldb-batch/src/test/resources/test.csv' into table t1 " +
        "options(format='txt', mode='overwrite');", Map("t1" -> t1))
      fail("unreachable")
    } catch {
      case e: HybridSeException => assert(e.getMessage == "file format unsupported")
      case _: Throwable => fail("should throw parse exception")
    }
  }

}
