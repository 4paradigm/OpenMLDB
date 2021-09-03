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

import org.scalatest.FunSuite

class TestYarnClientUtil extends FunSuite {

  test("Test parseAppIdStr") {
    val clusterTimestamp = 1629883521940L
    val id = 48549

    val appIdStr = s"application_${clusterTimestamp}_$id"

    val appId = YarnClientUtil.parseAppIdStr(appIdStr)

    assert(appId.getClusterTimestamp == clusterTimestamp)
    assert(appId.getId == id)
  }

}