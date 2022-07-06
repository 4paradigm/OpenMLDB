/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.source;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.ConnectionProvider;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({JdbcSourceTask.class})
@PowerMockIgnore("javax.management.*")
public class TableMonitorThreadTest {
  private static final long STARTUP_LIMIT = 50;
  private static final long POLL_INTERVAL = 100;

  private final static TableId FOO = new TableId(null, null, "foo");
  private final static TableId BAR = new TableId(null, null, "bar");
  private final static TableId BAZ = new TableId(null, null, "baz");

  private final static TableId DUP1 = new TableId(null, "dup1", "dup");
  private final static TableId DUP2 = new TableId(null, "dup2", "dup");

  private static final List<TableId> LIST_EMPTY = Collections.emptyList();
  private static final List<TableId> LIST_FOO = Collections.singletonList(FOO);
  private static final List<TableId> LIST_FOO_BAR = Arrays.asList(FOO, BAR);
  private static final List<TableId> LIST_FOO_BAR_BAZ = Arrays.asList(FOO, BAR, BAZ);
  private static final List<TableId> LIST_DUP_ONLY = Arrays.asList(DUP1, DUP2);
  private static final List<TableId> LIST_DUP_WITH_ALL = Arrays.asList(DUP1, FOO, DUP2, BAR, BAZ);

  private static final List<String> FIRST_TOPIC_LIST = Arrays.asList("foo");
  private static final List<String> VIEW_TOPIC_LIST = Arrays.asList("");
  private static final List<String> SECOND_TOPIC_LIST = Arrays.asList("foo", "bar");
  private static final List<String> THIRD_TOPIC_LIST = Arrays.asList("foo", "bar", "baz");
  public static final Set<String> DEFAULT_TABLE_TYPES = Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList("TABLE"))
  );
  public static final Set<String> VIEW_TABLE_TYPES = Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList("VIEW"))
  );
  private TableMonitorThread tableMonitorThread;

  @Mock private ConnectionProvider connectionProvider;
  @Mock private Connection connection;
  @Mock private DatabaseDialect dialect;
  @Mock private ConnectorContext context;
  @Mock private Time time;

  @Test
  public void testSingleLookup() throws Exception {
    EasyMock.expect(dialect.expressionBuilder()).andReturn(ExpressionBuilder.create()).anyTimes();
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, null, null, MockTime.SYSTEM);
    expectTableNames(LIST_FOO, shutdownThread());
    EasyMock.replay(connectionProvider, dialect);

    tableMonitorThread.start();
    tableMonitorThread.join();
    checkTableNames("foo").execute();

    EasyMock.verify(connectionProvider, dialect);
  }

  @Test
  public void testTablesBlockingTimeoutOnUpdateThread() throws Exception {
    EasyMock.expect(dialect.expressionBuilder()).andReturn(ExpressionBuilder.create()).anyTimes();
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, 0, null, null, time);

    CountDownLatch connectionRequested = new CountDownLatch(1);
    CountDownLatch connectionCompleted = new CountDownLatch(1);
    EasyMock.expect(dialect.tableIds(EasyMock.eq(connection))).andReturn(Collections.emptyList());
    EasyMock.expect(connectionProvider.getConnection()).andAnswer(() -> {
      connectionRequested.countDown();
      connectionCompleted.await();
      return connection;
    }).anyTimes();

    EasyMock.expect(time.milliseconds()).andReturn(0L).anyTimes();
    time.waitObject(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.eq(STARTUP_LIMIT));
    EasyMock.expectLastCall()
        .andThrow(new TimeoutException())
        .anyTimes();

    EasyMock.replay(connectionProvider, connection, dialect, time);

    // Haven't had a chance to start the first table read; should return null to signify that no
    // attempt to list tables on the database has succeeded yet
    assertNull(
        "Should not have even started any table reads yet",
        tableMonitorThread.tables()
    );
    tableMonitorThread.start();

    assertTrue(
        "Should have attempted to establish database connection by now",
        connectionRequested.await(10, TimeUnit.SECONDS)
    );
    // Have initiated a table read, but haven't been able to connect to the database yet;
    // should still return
    assertNull(
        "Should not have completed any table reads yet",
        tableMonitorThread.tables()
    );

    connectionCompleted.countDown();
    tableMonitorThread.join();
    // Have completed a table read; should return an empty list (instead of null) to signify that
    // we've been able to read the tables from the database, but just can't find any to query
    assertEquals(Collections.emptyList(), tableMonitorThread.tables());

    EasyMock.verify(time);
  }

  @Test
  public void testTablesBlockingWithDeadlineOnUpdateThread() throws Exception {
    EasyMock.expect(dialect.expressionBuilder()).andReturn(ExpressionBuilder.create());
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, null, null, time);

    EasyMock.expect(dialect.tableIds(EasyMock.eq(connection))).andReturn(Collections.emptyList());
    EasyMock.expect(connectionProvider.getConnection()).andReturn(connection);

    long currentTime = System.currentTimeMillis();
    EasyMock.expect(time.milliseconds()).andReturn(currentTime);
    time.waitObject(
        EasyMock.anyObject(),
        EasyMock.anyObject(),
        EasyMock.eq(currentTime + STARTUP_LIMIT));

    EasyMock.replay(connectionProvider, connection, dialect, time);

    tableMonitorThread.start();
    tableMonitorThread.join();

    assertEquals(Collections.emptyList(), tableMonitorThread.tables());

    EasyMock.verify(time);
  }

  @Test
  public void testWhitelist() throws Exception {
    Set<String> whitelist = new HashSet<>(Arrays.asList("foo", "bar"));
    EasyMock.expect(dialect.expressionBuilder()).andReturn(ExpressionBuilder.create()).anyTimes();
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, whitelist, null, MockTime.SYSTEM);
    expectTableNames(LIST_FOO_BAR, shutdownThread());
    EasyMock.replay(connectionProvider, dialect);

    tableMonitorThread.start();
    tableMonitorThread.join();
    checkTableNames("foo", "bar").execute();

    EasyMock.verify(connectionProvider, dialect);
  }

  @Test
  public void testBlacklist() throws Exception {
    Set<String> blacklist = new HashSet<>(Arrays.asList("bar", "baz"));
    EasyMock.expect(dialect.expressionBuilder()).andReturn(ExpressionBuilder.create()).anyTimes();
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, null, blacklist, MockTime.SYSTEM);
    expectTableNames(LIST_FOO_BAR_BAZ, shutdownThread());
    EasyMock.replay(connectionProvider, dialect);

    tableMonitorThread.start();
    tableMonitorThread.join();
    checkTableNames("foo").execute();

    EasyMock.verify(connectionProvider, dialect);
  }

  @Test
  public void testReconfigOnUpdate() throws Exception {
    EasyMock.expect(dialect.expressionBuilder()).andReturn(ExpressionBuilder.create()).anyTimes();
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, null, null, MockTime.SYSTEM);
    expectTableNames(LIST_FOO);
    expectTableNames(LIST_FOO, checkTableNames("foo"));
    context.requestTaskReconfiguration();
    EasyMock.expectLastCall();

    // Change the result to trigger a task reconfiguration
    expectTableNames(LIST_FOO_BAR);
    context.requestTaskReconfiguration();
    EasyMock.expectLastCall();

    // Changing again should result in another task reconfiguration
    expectTableNames(LIST_FOO, checkTableNames("foo", "bar"), shutdownThread());
    context.requestTaskReconfiguration();
    EasyMock.expectLastCall();

    EasyMock.replay(connectionProvider, dialect, context);

    tableMonitorThread.start();
    tableMonitorThread.join();
    checkTableNames("foo").execute();

    EasyMock.verify(connectionProvider, dialect, context);
  }

  @Test
  public void testInvalidConnection() throws Exception {
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, null, null, MockTime.SYSTEM);
    EasyMock.expect(connectionProvider.getConnection()).andThrow(new ConnectException("Simulated error with the db."));

    CountDownLatch errorLatch = new CountDownLatch(1);
    context.raiseError(EasyMock.anyObject());
    EasyMock.expectLastCall().andAnswer(() -> {
      errorLatch.countDown();
      return null;
    });

    EasyMock.replay(connectionProvider, context);

    tableMonitorThread.start();
    assertTrue("Connector should have failed by now", errorLatch.await(10, TimeUnit.SECONDS));
    tableMonitorThread.join();

    EasyMock.verify(connectionProvider, context);
  }

  @Test
  public void testDuplicates() throws Exception {
    EasyMock.expect(dialect.expressionBuilder()).andReturn(ExpressionBuilder.create()).anyTimes();
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, null, null, MockTime.SYSTEM);
    expectTableNames(LIST_DUP_WITH_ALL, shutdownThread());
    context.requestTaskReconfiguration();
    EasyMock.expectLastCall();
    context.raiseError(EasyMock.anyObject());
    EasyMock.expectLastCall();
    EasyMock.replay(connectionProvider, dialect, context);
    tableMonitorThread.start();
    tableMonitorThread.join();
    assertThrows(ConnectException.class, tableMonitorThread::tables);
    EasyMock.verify(connectionProvider, dialect, context);
  }

  @Test
  public void testDuplicateWithUnqualifiedWhitelist() throws Exception {
    Set<String> whitelist = new HashSet<>(Arrays.asList("dup"));
    EasyMock.expect(dialect.expressionBuilder()).andReturn(ExpressionBuilder.create()).anyTimes();
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, whitelist, null, MockTime.SYSTEM);
    expectTableNames(LIST_DUP_ONLY, shutdownThread());
    context.requestTaskReconfiguration();
    EasyMock.expectLastCall();
    context.raiseError(EasyMock.anyObject());
    EasyMock.expectLastCall();
    EasyMock.replay(connectionProvider, dialect, context);

    tableMonitorThread.start();
    tableMonitorThread.join();
    assertThrows(ConnectException.class, tableMonitorThread::tables);
    EasyMock.verify(connectionProvider, dialect, context);
  }

  @Test
  public void testDuplicateWithUnqualifiedBlacklist() throws Exception {
    Set<String> blacklist = new HashSet<>(Arrays.asList("foo"));
    EasyMock.expect(dialect.expressionBuilder()).andReturn(ExpressionBuilder.create()).anyTimes();
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, null, blacklist, MockTime.SYSTEM);
    expectTableNames(LIST_DUP_WITH_ALL, shutdownThread());
    context.requestTaskReconfiguration();
    EasyMock.expectLastCall();
    context.raiseError(EasyMock.anyObject());
    EasyMock.expectLastCall();
    EasyMock.replay(connectionProvider, dialect, context);

    tableMonitorThread.start();
    tableMonitorThread.join();
    assertThrows(ConnectException.class, tableMonitorThread::tables);
    EasyMock.verify(connectionProvider, dialect, context);
  }

  @Test
  public void testDuplicateWithQualifiedWhitelist() throws Exception {
    Set<String> whitelist = new HashSet<>(Arrays.asList("dup1.dup", "foo"));
    EasyMock.expect(dialect.expressionBuilder()).andReturn(ExpressionBuilder.create()).anyTimes();
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, whitelist, null, MockTime.SYSTEM);
    expectTableNames(LIST_DUP_WITH_ALL, shutdownThread());
    EasyMock.replay(connectionProvider, dialect);

    tableMonitorThread.start();
    tableMonitorThread.join();
    checkTableIds(DUP1, FOO);
    EasyMock.verify(connectionProvider, dialect);
  }

  @Test
  public void testDuplicateWithQualifiedBlacklist() throws Exception {
    Set<String> blacklist = new HashSet<>(Arrays.asList("dup1.dup", "foo"));
    EasyMock.expect(dialect.expressionBuilder()).andReturn(ExpressionBuilder.create()).anyTimes();
    tableMonitorThread = new TableMonitorThread(dialect, connectionProvider, context,
        STARTUP_LIMIT, POLL_INTERVAL, null, blacklist, MockTime.SYSTEM);
    expectTableNames(LIST_DUP_WITH_ALL, shutdownThread());
    EasyMock.replay(connectionProvider, dialect);

    tableMonitorThread.start();
    tableMonitorThread.join();
    checkTableIds(DUP2, BAR, BAZ);
    EasyMock.verify(connectionProvider, dialect);
  }

  private interface Op {
    void execute();
  }

  protected Op shutdownThread() {
    return new Op() {
      @Override
      public void execute() {
        tableMonitorThread.shutdown();
      }
    };
  }

  protected Op checkTableNames(final String...expectedTableNames) {
    return new Op() {
      @Override
      public void execute() {
        List<TableId> expectedTableIds = new ArrayList<>();
        for (String expectedTableName: expectedTableNames) {
          TableId id = new TableId(null, null, expectedTableName);
          expectedTableIds.add(id);
        }
        assertEquals(expectedTableIds, tableMonitorThread.tables());
      }
    };
  }

  protected void checkTableIds(final TableId...expectedTables) {
    assertEquals(Arrays.asList(expectedTables), tableMonitorThread.tables());
  }

  protected void expectTableNames(final List<TableId> expectedTableIds, final Op...operations) throws SQLException {
    EasyMock.expect(connectionProvider.getConnection()).andReturn(connection);
    EasyMock.expect(dialect.tableIds(EasyMock.eq(connection))).andAnswer(
        new IAnswer<List<TableId>>() {
          @Override
          public List<TableId> answer() throws Throwable {
            if (operations != null) {
              for (Op op : operations ) {
                op.execute();
              }
            }
            return expectedTableIds;
          }
        });
  }
}
