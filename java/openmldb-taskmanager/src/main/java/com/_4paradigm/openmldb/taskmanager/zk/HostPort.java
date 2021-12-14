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

package com._4paradigm.openmldb.taskmanager.zk;

/**
 * The basic structure of server host and port. Used to convert host and port into string.
 */
public class HostPort {

  private String host;
  private int port;

  public HostPort(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  /**
   * Get the host_port string from this object.
   *
   * @return the host_port string
   */
  public String getHostPort() {
    return host + ":" + port;
  }

  /**
   * Parse a host_port string to construct the HostPost object.
   *
   * @param string the host_port string
   * @return the HostPort object
   */
  public static HostPort parseHostPort(String string) {
    String[] strings = string.split(":");
    return new HostPort(strings[0], Integer.parseInt(strings[1]));
  }

}
