/*
 * Copyright 2022 paxos.cn.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.paxos.mysql.engine;

import cn.paxos.mysql.ResultSetWriter;
import java.io.IOException;

/** An interface to callback events received from the MySQL server. */
public interface SqlEngine {

  /**
   * Authenticating the user and password.
   *
   * @param database Database name
   * @param userName User name
   * @param scramble411 Encoded password
   * @param authSeed Encoding seed
   * @throws IOException Thrown with IllegalAccessException as the inner cause if the authentication
   *     is failed
   */
  void authenticate(
      int connectionId, String database, String userName, byte[] scramble411, byte[] authSeed)
      throws IOException;

  /**
   * Querying the SQL.
   *
   * @param resultSetWriter Response writer
   * @param database Database name
   * @param userName User name
   * @param scramble411 Encoded password
   * @param authSeed Encoding seed
   * @param sql SQL text
   * @throws IOException Thrown with IllegalAccessException as the inner cause if the
   *     authentication/authorization is failed, or IllegalArgumentException if SQL is invalid
   */
  void query(
      int connectionId,
      ResultSetWriter resultSetWriter,
      String database,
      String userName,
      byte[] scramble411,
      byte[] authSeed,
      String sql)
      throws IOException;

  void close(int connectionId) throws IOException;
}
