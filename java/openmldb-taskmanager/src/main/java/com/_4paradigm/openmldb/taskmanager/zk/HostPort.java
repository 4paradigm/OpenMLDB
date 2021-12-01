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
