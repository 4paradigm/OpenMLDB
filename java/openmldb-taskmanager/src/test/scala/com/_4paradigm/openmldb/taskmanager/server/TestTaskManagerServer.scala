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

package com._4paradigm.openmldb.taskmanager.server

import org.scalatest.FunSuite

/**
 * Notice that you have to setup OpenMLDB and download Spark package to run these cases.
 */
class TestTaskManagerServer extends FunSuite {

  test("Test start TaskManager") {
    val server = new TaskManagerServer()
    server.start(false)
    server.close()

    assert(true)
  }

  test("Test start rpc server") {
    val server = new TaskManagerServer()
    server.startRpcServer(false)
    server.close()

    assert(true)
  }

}
