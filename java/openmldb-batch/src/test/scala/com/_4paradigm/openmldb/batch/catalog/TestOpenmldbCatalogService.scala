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

package com._4paradigm.openmldb.batch.catalog

import org.scalatest.FunSuite


class TestOpenmldbCatalogService extends FunSuite {

  test("TestOpenmldbCatalogService") {
    val zkCluster = "127.0.0.1:2181"
    val zkPath = "/openmldb_cluster"

    val openmldbCatalogService = new OpenmldbCatalogService(zkCluster, zkPath)
    assert(openmldbCatalogService != null)
  }

}
