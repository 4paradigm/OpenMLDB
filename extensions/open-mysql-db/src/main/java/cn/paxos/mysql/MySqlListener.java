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

package cn.paxos.mysql;

import cn.paxos.mysql.codec.*;
import cn.paxos.mysql.engine.QueryResultColumn;
import cn.paxos.mysql.engine.SqlEngine;
import cn.paxos.mysql.util.Utils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioChannelOption;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import jdk.net.ExtendedSocketOptions;

/** A MySQL protocol implementation. */
public class MySqlListener implements AutoCloseable {

  public static final String VERSION = "8.0.29";
  public static final String VERSION_COMMENT = "";
  private static final Pattern SETTINGS_PATTERN =
      Pattern.compile("@@([\\w.]+)(?:\\sAS\\s)?(\\w+)?");
  private final SqlEngine sqlEngine;
  private final int port;
  private final Channel channel;
  private final io.netty.channel.EventLoopGroup parentGroup;
  private final EventLoopGroup childGroup;
  private final EventExecutorGroup eventExecutorGroup;
  private int connectionId;

  public MySqlListener(int port, int executorGroupSize, SqlEngine sqlEngine) {
    this.port = port;
    this.sqlEngine = sqlEngine;

    parentGroup = new NioEventLoopGroup();
    childGroup = new NioEventLoopGroup();
    eventExecutorGroup = new DefaultEventExecutorGroup(executorGroupSize);
    /*
    With default settings of Aliyun ECS, a connection living
    for a long time (more than about 15 minutes) would be
    forcibly closed by the firewall, which is unacceptable
    for slow SQL queries. So we have to enable TCP keep-alive
    to allow the OS to send heartbeats and reduce the interval
    to less than the time valve which is 910 seconds.
    Reference: https://zhuanlan.zhihu.com/p/52622856

    Steps for the OS:
    sysctl -a | grep keepalive
    vi /etc/sysctl.conf
    net.ipv4.tcp_keepalive_time=300
    sysctl -p
    sysctl -a | grep keepalive

    Steps for Netty:
    .option(ChannelOption.SO_KEEPALIVE, true)
    .childOption(ChannelOption.SO_KEEPALIVE, true)
    */
    final ChannelFuture channelFuture =
        new ServerBootstrap()
            .group(parentGroup, childGroup)
            .channel(NioServerSocketChannel.class)
            .option(NioChannelOption.of(ExtendedSocketOptions.TCP_KEEPIDLE), 300)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childHandler(
                new ChannelInitializer<NioSocketChannel>() {
                  @Override
                  protected void initChannel(NioSocketChannel ch) throws Exception {
                    System.out.println("[mysql-protocol] Initializing child channel");
                    final ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast(new MysqlServerPacketEncoder());
                    pipeline.addLast(new MysqlClientConnectionPacketDecoder());
                    pipeline.addLast(eventExecutorGroup, new ServerHandler());
                  }
                })
            .bind(port);
    channel = channelFuture.channel();
    channelFuture.awaitUninterruptibly();
    if (!channel.isActive()) {
      throw new RuntimeException("MySQL listening on port " + port + " failed");
    }
    System.out.println("[mysql-protocol] MySQL server listening on port " + port + " started");
  }

  @Override
  public void close() {
    channel.close();
    eventExecutorGroup.shutdownGracefully().awaitUninterruptibly();
    childGroup.shutdownGracefully().awaitUninterruptibly();
    parentGroup.shutdownGracefully().awaitUninterruptibly();
  }

  private void handleHandshakeResponse(
      ChannelHandlerContext ctx, HandshakeResponse response, byte[] salt, String remoteAddr) {
    System.out.println("[mysql-protocol] Received handshake response " + remoteAddr);

    System.out.println("[mysql-protocol] attrs = " + response.getAttributes());
    System.out.println("[mysql-protocol] flags = " + response.getCapabilityFlags());

    int readableBytes = response.getAuthPluginData().readableBytes();
    String authPluginName = response.getAuthPluginName();
    System.out.println(
        "[mysql-protocol] Auth plugin name: " + authPluginName + ", " + readableBytes);

    // Processing AuthSwitchRequest while authPluginName or auth data length wrong
    String requestAuthPluginName = Constants.DEFAULT_AUTH_PLUGIN_NAME;
    if (!requestAuthPluginName.equals(authPluginName)) {
      System.out.println("[mysql-protocol] Send AuthSwitchRequest " + requestAuthPluginName);
      ctx.writeAndFlush(
          new AuthSwitchRequest(response.getSequenceId() + 1, requestAuthPluginName, salt));
      response.setAuthPluginName(requestAuthPluginName);
      MysqlClientConnectionPacketDecoder connPacketDecoder =
          ctx.pipeline().get(MysqlClientConnectionPacketDecoder.class);
      connPacketDecoder.setAuthSwitchStatus(1);
      return;
    }
    byte[] scramble411 = new byte[readableBytes];
    response.getAuthPluginData().readBytes(scramble411);

    System.out.println(
        "[mysql-protocol] scramble411: " + Base64.getEncoder().encodeToString(scramble411));

    ctx.pipeline()
        .replace(
            MysqlClientPacketDecoder.class,
            "CommandPacketDecoder",
            new MysqlClientCommandPacketDecoder(
                response.getDatabase(), response.getUsername(), scramble411));

    try {
      sqlEngine.authenticate(
          connectionId, response.getDatabase(), response.getUsername(), scramble411, salt);
    } catch (IOException e) {
      System.out.println(
          "[mysql-protocol] Sql query exception: " + response.getUsername() + ", " + remoteAddr);
      e.printStackTrace();

      Throwable cause = e.getCause();
      int errorCode;
      byte[] sqlState;
      String errMsg =
          Utils.getLocalDateTimeNow()
              + " "
              + Objects.requireNonNullElse(cause.getMessage(), e.getMessage());
      if (cause instanceof IllegalAccessException) {
        errorCode = 1045;
        sqlState = "#28000".getBytes(StandardCharsets.US_ASCII);
      } else {
        errorCode = 1105;
        sqlState = "#HY000".getBytes(StandardCharsets.US_ASCII);
      }
      ctx.writeAndFlush(
          new ErrorResponse(response.getSequenceId() + 1, errorCode, sqlState, errMsg));
      return;
    }

    ctx.writeAndFlush(OkResponse.builder().sequenceId(response.getSequenceId() + 1).build());
  }

  private void handleQuery(
      ChannelHandlerContext ctx, QueryCommand query, byte[] salt, String remoteAddr) {
    final String queryString = query.getQuery();
    final String database = query.getDatabase();
    final String userName = query.getUserName();
    final byte[] scramble411 = query.getScramble411();
    System.out.println(
        "[mysql-protocol] Received query: "
            + ((queryString.startsWith("insert") && queryString.length() > 200)
                ? queryString.substring(0, 200)
                : queryString)
            + ", database: "
            + database
            + ", userName: "
            + userName
            + ", scramble411: "
            + scramble411.length);

    if (isServerSettingsQuery(queryString)) {
      sendSettingsResponse(ctx, query, remoteAddr);
    } else if (queryString.replaceAll("/\\*.*\\*/", "").toLowerCase().trim().startsWith("set ")
        && !queryString
            .replaceAll("/\\*.*\\*/", "")
            .toLowerCase()
            .trim()
            .startsWith("set @@execute_mode=")) {
      // ignore SET command
      ctx.writeAndFlush(OkResponse.builder().sequenceId(query.getSequenceId() + 1).build());
    } else if (queryString.equalsIgnoreCase("rollback")) {
      ctx.writeAndFlush(OkResponse.builder().sequenceId(query.getSequenceId() + 1).build());
    } else {
      // Generic response
      int[] sequenceId = new int[] {query.getSequenceId()};

      boolean[] columnsWritten = new boolean[1];

      ResultSetWriter resultSetWriter =
          new ResultSetWriter() {

            @Override
            public void writeColumns(List<QueryResultColumn> columns) {
              ctx.write(new ColumnCount(++sequenceId[0], columns.size()));
              for (QueryResultColumn column : columns) {
                ColumnType columnType;
                switch (column.getType()) {
                  case "TINYINT(1)":
                  case "TINYINT":
                    columnType = ColumnType.MYSQL_TYPE_TINY;
                    break;
                  case "INT":
                    columnType = ColumnType.MYSQL_TYPE_LONG;
                    break;
                  case "BIGINT":
                    columnType = ColumnType.MYSQL_TYPE_LONGLONG;
                    break;
                  case "FLOAT":
                    columnType = ColumnType.MYSQL_TYPE_FLOAT;
                    break;
                  case "DOUBLE":
                    columnType = ColumnType.MYSQL_TYPE_DOUBLE;
                    break;
                  case "DATE":
                    columnType = ColumnType.MYSQL_TYPE_DATE;
                    break;
                  case "TIMESTAMP":
                    columnType = ColumnType.MYSQL_TYPE_TIMESTAMP;
                    break;
                  default:
                    columnType = ColumnType.MYSQL_TYPE_VAR_STRING;
                    break;
                }
                ctx.write(
                    ColumnDefinition.builder()
                        .sequenceId(++sequenceId[0])
                        .catalog("catalog")
                        .schema("schema")
                        .table("table")
                        .orgTable("org_table")
                        .name(column.getName())
                        .orgName(column.getName())
                        .columnLength(10)
                        .type(columnType)
                        .addFlags(ColumnFlag.NUM)
                        .decimals(5)
                        .build());
              }
              ctx.write(new EofResponse(++sequenceId[0], 0));

              System.out.println("[mysql-protocol] Columns done");

              columnsWritten[0] = !columns.isEmpty();
            }

            @Override
            public void writeRow(List<String> row) {
              if (++sequenceId[0] % 100 == 0) {
                ctx.writeAndFlush(new ResultsetRow(sequenceId[0], row.toArray(new String[0])));
              } else {
                ctx.write(new ResultsetRow(sequenceId[0], row.toArray(new String[0])));
              }
            }

            @Override
            public void finish() {
              ctx.writeAndFlush(new EofResponse(++sequenceId[0], 0));

              System.out.println("[mysql-protocol] All done");
            }
          };
      try {
        sqlEngine.query(
            connectionId, resultSetWriter, database, userName, scramble411, salt, queryString);
      } catch (IOException e) {
        System.out.println("[mysql-protocol] Sql query exception: " + userName + ", " + remoteAddr);
        e.printStackTrace();

        Throwable cause = e.getCause();
        int errorCode;
        byte[] sqlState;
        String errMsg =
            Utils.getLocalDateTimeNow()
                + " "
                + Objects.requireNonNullElse(cause.getMessage(), e.getMessage());
        if (cause instanceof IllegalAccessException) {
          errorCode = 1045;
          sqlState = "#28000".getBytes(StandardCharsets.US_ASCII);
        } else if (cause instanceof IllegalArgumentException) {
          errorCode = 1064;
          sqlState = "#42000".getBytes(StandardCharsets.US_ASCII);
        } else if (e.getMessage()
            .equalsIgnoreCase(
                "java.sql.SQLException: executeSQL fail: [2000] please enter database first")) {
          errorCode = 1046;
          sqlState = "#3D000".getBytes(StandardCharsets.US_ASCII);
        } else {
          errorCode = 1105;
          sqlState = "#HY000".getBytes(StandardCharsets.US_ASCII);
        }
        ctx.writeAndFlush(new ErrorResponse(++sequenceId[0], errorCode, sqlState, errMsg));
        return;
      }

      if (!columnsWritten[0]) {
        ctx.write(new ColumnCount(++sequenceId[0], 1));
        ctx.write(
            ColumnDefinition.builder()
                .sequenceId(++sequenceId[0])
                .catalog("catalog")
                .schema("schema")
                .table("table")
                .orgTable("org_table")
                .name("error")
                .orgName("org_name")
                .columnLength(10)
                .type(ColumnType.MYSQL_TYPE_VAR_STRING)
                .addFlags(ColumnFlag.NUM)
                .decimals(5)
                .build());
        ctx.write(new EofResponse(++sequenceId[0], 0));
        ctx.writeAndFlush(new EofResponse(++sequenceId[0], 0));
      }

      System.out.println("[mysql-protocol] Query done");
    }
  }

  private boolean isServerSettingsQuery(String query) {
    query = query.toLowerCase();
    return query.contains("select")
        && !query.contains("from")
        && (query.contains("@@")
            || query.contains("database()")
            || query.contains("user()")
            || query.contains("version()")
            || query.contains("connection_id()"));
  }

  private void sendSettingsResponse(
      ChannelHandlerContext ctx, QueryCommand query, String remoteAddr) {

    // Fix 'select @@version_comment limit 1'
    // Convert 'select DATABASE(), USER() limit 1' to 'select @@database, @@user limit 1
    String setCommand =
        query
            .getQuery()
            .replace("limit 1", "")
            .replaceAll("(?i)database\\(\\)", "@@database")
            .replaceAll("(?i)user\\(\\)", "@@user")
            .replaceAll("(?i)version\\(\\)", "@@version")
            .replaceAll("(?i)connection_id\\(\\)", "@@connection_id");

    final Matcher matcher = SETTINGS_PATTERN.matcher(setCommand);

    // Add column count row before column definitions to prevent 'UPDATE not result set'.
    final List<ColumnDefinition> columnDefinitions = new ArrayList<>();

    final List<String> values = new ArrayList<>();
    int sequenceId = query.getSequenceId();

    // sequenceId++ to ++sequenceId.
    int columnCountSequenceId = ++sequenceId;

    while (matcher.find()) {
      String systemVariable = matcher.group(1);
      String fieldName = Objects.requireNonNullElse(matcher.group(2), systemVariable);
      switch (systemVariable) {
          // DATABASE() function
        case "database":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_VAR_STRING, 63));
          values.add(query.getDatabase());
          break;
          // USER() function
        case "user":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_VAR_STRING, 63));
          values.add(query.getUserName() + "@" + remoteAddr);
          break;
          // VERSION() function
        case "version":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_VAR_STRING, 63));
          values.add(VERSION);
          break;
          // connection_id() function
        case "connection_id":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_VAR_STRING, 63));
          values.add(Integer.toString(connectionId));
          break;
        case "character_set_client":
        case "character_set_connection":
        case "character_set_results":
        case "character_set_server":
        case "GLOBAL.character_set_server":
        case "character_set_database":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_VAR_STRING, 12));
          values.add("utf8mb4");
          break;
        case "collation_server":
        case "GLOBAL.collation_server":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_VAR_STRING, 63));
          values.add("utf8mb4_0900_ai_ci");
          break;
        case "init_connect":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_VAR_STRING, 0));
          values.add("");
          break;
        case "interactive_timeout":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_VAR_STRING, 21));
          values.add("28800");
          break;
        case "language":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_VAR_STRING, 0));
          values.add("");
          break;
        case "license":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_VAR_STRING, 21));
          values.add("ASLv2");
          break;
        case "lower_case_table_names":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 63));
          values.add("2");
          break;
        case "max_allowed_packet":
        case "global.max_allowed_packet":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 63));
          values.add("4194304");
          break;
        case "net_buffer_length":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 63));
          values.add("16384");
          break;
        case "net_write_timeout":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 63));
          values.add("28800");
          break;
        case "have_query_cache":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 6));
          values.add("NO");
          break;
        case "sql_mode":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 6));
          values.add("0");
          break;
        case "system_time_zone":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_VAR_STRING, 9));
          values.add("UTC");
          break;
        case "time_zone":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_VAR_STRING, 18));
          values.add("SYSTEM");
          break;
        case "tx_isolation":
        case "session.tx_isolation":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_VAR_STRING, 63));
          values.add("REPEATABLE-READ");
          break;
        case "wait_timeout":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 12));
          values.add("28800");
          break;
        case "query_cache_type":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 6));
          values.add("0");
          break;
        case "version_comment":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_VAR_STRING, 0));
          values.add(VERSION_COMMENT);
          break;
        case "collation_connection":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_VAR_STRING, 63));
          values.add("utf8mb4_0900_ai_ci");
          break;
        case "query_cache_size":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 6));
          values.add("0");
          break;
        case "performance_schema":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 6));
          values.add("0");
          break;
        case "session.auto_increment_increment":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 12));
          values.add("1");
          break;
        case "auto_increment_increment":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_LONGLONG, 12));
          values.add("1");
          break;
        case "transaction_isolation":
          columnDefinitions.add(
              newColumnDefinition(
                  ++sequenceId, fieldName, systemVariable, ColumnType.MYSQL_TYPE_VAR_STRING, 63));
          values.add("REPEATABLE-READ");
          //          values.add("READ-UNCOMMITTED");
          break;
        default:
          System.err.println("[mysql-protocol] Unknown system variable: " + systemVariable);
          throw new Error("Unknown system variable " + systemVariable);
      }
    }
    ctx.write(new ColumnCount(columnCountSequenceId, values.size()));
    for (ColumnDefinition columnDefinition : columnDefinitions) {
      ctx.write(columnDefinition);
    }
    ctx.write(new EofResponse(++sequenceId, 0));
    ctx.write(new ResultsetRow(++sequenceId, values.toArray(new String[values.size()])));
    ctx.writeAndFlush(new EofResponse(++sequenceId, 0));
  }

  private ColumnDefinition newColumnDefinition(
      int packetSequence, String name, String orgName, ColumnType columnType, int length) {
    return ColumnDefinition.builder()
        .sequenceId(packetSequence)

        // Added to prevent out of bound.
        .catalog("catalog")
        .schema("schema")
        .table("table")
        .orgTable("org_table")
        .name(name)
        .orgName(orgName)
        .type(columnType)
        .columnLength(length)
        .build();
  }

  private class ServerHandler extends ChannelInboundHandlerAdapter {
    private final byte[] salt;
    private String remoteAddr;

    public ServerHandler() {
      // 20 random bytes with ASCII characters
      salt = Utils.generateRandomAsciiBytes(20);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      // todo may java.lang.NullPointerException
      this.remoteAddr =
          ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress().getHostAddress();
      connectionId = Integer.parseUnsignedInt(ctx.channel().id().asShortText(), 16);
      System.out.println("[mysql-protocol] Server channel active");
      final EnumSet<CapabilityFlags> capabilities = CapabilityFlags.getImplicitCapabilities();
      CapabilityFlags.setCapabilitiesAttr(ctx.channel(), capabilities);
      ctx.writeAndFlush(
          Handshake.builder()
              .serverVersion(VERSION)
              .connectionId(connectionId)
              .addAuthData(salt)
              .characterSet(MysqlCharacterSet.UTF8MB4_0900_AI_CI)
              .addCapabilities(capabilities)
              .build());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      System.out.println("[mysql-protocol] Server channel inactive: " + new Date());
      sqlEngine.close(connectionId);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof HandshakeResponse) {
        handleHandshakeResponse(ctx, (HandshakeResponse) msg, salt, remoteAddr);
      } else if (msg instanceof QueryCommand) {
        handleQuery(ctx, (QueryCommand) msg, salt, remoteAddr);
      } else {
        System.out.println("[mysql-protocol] Received message: " + msg);

        // Prevent hanging on client connection.
        if (msg instanceof CommandPacket) {
          CommandPacket commandPacket = (CommandPacket) msg;
          Command command = commandPacket.getCommand();
          int sequenceId = commandPacket.getSequenceId();
          System.out.println("[mysql-protocol] Received command: " + command);
          if (command.equals(Command.COM_QUIT)) {
            ctx.flush();
            ctx.close();
          } else if (command.equals(Command.COM_INIT_DB) || command.equals(Command.COM_PING)) {
            ctx.writeAndFlush(OkResponse.builder().sequenceId(sequenceId + 1).build());
          } else if (command.equals(Command.COM_FIELD_LIST)) {
            ctx.writeAndFlush(new EofResponse(sequenceId + 1, 0));
          } else if (command.equals(Command.COM_STATISTICS)) {
            String statString =
                "Uptime: "
                    + Utils.getJVMUptime()
                    + "  "
                    + "Hack Code: ..oo.o....oo....o.ooo..o.oo.....o.o..o.ooo..oooo...o...o..oo.o....oo....o.ooo..o.oo.....o.o..o.ooo..oooo...o...o";
            ctx.writeAndFlush(new StatisticsResponse(sequenceId + 1, statString));
          }
        }
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      cause.printStackTrace();
      ctx.close();
      sqlEngine.close(connectionId);
    }
  }
}
