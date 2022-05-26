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

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.util.Version;

public class JdbcSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(JdbcSinkTask.class);

  ErrantRecordReporter reporter;
  DatabaseDialect dialect;
  JdbcSinkConfig config;
  JdbcDbWriter writer;
  int remainingRetries;

  @Override
  public void start(final Map<String, String> props) {
    log.info("Starting JDBC Sink task");
    config = new JdbcSinkConfig(props);
    initWriter();
    remainingRetries = config.maxRetries;
    try {
      reporter = context.errantRecordReporter();
    } catch (NoSuchMethodError | NoClassDefFoundError e) {
      // Will occur in Connect runtimes earlier than 2.6
      reporter = null;
    }
  }

  void initWriter() {
    if (config.dialectName != null && !config.dialectName.trim().isEmpty()) {
      dialect = DatabaseDialects.create(config.dialectName, config);
    } else {
      dialect = DatabaseDialects.findBestFor(config.connectionUrl, config);
    }
    final DbStructure dbStructure = new DbStructure(dialect);
    log.info("Initializing writer using SQL dialect: {}", dialect.getClass().getSimpleName());
    writer = new JdbcDbWriter(config, dialect, dbStructure);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }
    final SinkRecord first = records.iterator().next();
    final int recordsCount = records.size();
    log.debug(
        "Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the "
        + "database...",
        recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset()
    );
    try {
      writer.write(records);
    } catch (TableAlterOrCreateException tace) {
      if (reporter != null) {
        unrollAndRetry(records);
      } else {
        throw tace;
      }
    } catch (SQLException sqle) {
      log.warn(
          "Write of {} records failed, remainingRetries={}",
          records.size(),
          remainingRetries,
          sqle
      );
      int totalExceptions = 0;
      for (Throwable e :sqle) {
        totalExceptions++;
      }
      SQLException sqlAllMessagesException = getAllMessagesException(sqle);
      if (remainingRetries > 0) {
        writer.closeQuietly();
        initWriter();
        remainingRetries--;
        context.timeout(config.retryBackoffMs);
        throw new RetriableException(sqlAllMessagesException);
      } else {
        if (reporter != null) {
          unrollAndRetry(records);
        } else {
          log.error(
              "Failing task after exhausting retries; "
                  + "encountered {} exceptions on last write attempt. "
                  + "For complete details on each exception, please enable DEBUG logging.",
              totalExceptions);
          int exceptionCount = 1;
          for (Throwable e : sqle) {
            log.debug("Exception {}:", exceptionCount++, e);
          }
          throw new ConnectException(sqlAllMessagesException);
        }
      }
    }
    remainingRetries = config.maxRetries;
  }

  private void unrollAndRetry(Collection<SinkRecord> records) {
    writer.closeQuietly();
    for (SinkRecord record : records) {
      try {
        writer.write(Collections.singletonList(record));
      } catch (TableAlterOrCreateException tace) {
        reporter.report(record, tace);
        writer.closeQuietly();
      } catch (SQLException sqle) {
        SQLException sqlAllMessagesException = getAllMessagesException(sqle);
        reporter.report(record, sqlAllMessagesException);
        writer.closeQuietly();
      }
    }
  }

  private SQLException getAllMessagesException(SQLException sqle) {
    String sqleAllMessages = "Exception chain:" + System.lineSeparator();
    for (Throwable e : sqle) {
      sqleAllMessages += e + System.lineSeparator();
    }
    SQLException sqlAllMessagesException = new SQLException(sqleAllMessages);
    sqlAllMessagesException.setNextException(sqle);
    return sqlAllMessagesException;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    // Not necessary
  }

  public void stop() {
    log.info("Stopping task");
    try {
      writer.closeQuietly();
    } finally {
      try {
        if (dialect != null) {
          dialect.close();
        }
      } catch (Throwable t) {
        log.warn("Error while closing the {} dialect: ", dialect.name(), t);
      } finally {
        dialect = null;
      }
    }
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

}
