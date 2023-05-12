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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDbWriter {
  private static final Logger log = LoggerFactory.getLogger(JdbcDbWriter.class);

  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  final CachedConnectionProvider cachedConnectionProvider;

  JdbcDbWriter(final JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure) {
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;

    this.cachedConnectionProvider = connectionProvider(
        config.connectionAttempts,
        config.connectionBackoffMs
    );
  }

  protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
    return new CachedConnectionProvider(this.dbDialect, maxConnAttempts, retryBackoff) {
      @Override
      protected void onConnect(final Connection connection) throws SQLException {
        log.info("JdbcDbWriter Connected");
        connection.setAutoCommit(false);
      }
    };
  }

  void write(final Collection<SinkRecord> records)
      throws SQLException, TableAlterOrCreateException {
    final Connection connection = cachedConnectionProvider.getConnection();
    try {
      final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();
      for (SinkRecord record : records) {
        final TableId tableId = destinationTable(record.topic());
        BufferedRecords buffer = bufferByTable.get(tableId);
        if (buffer == null) {
          buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
          bufferByTable.put(tableId, buffer);
        }
        buffer.add(record);
      }
      for (Map.Entry<TableId, BufferedRecords> entry : bufferByTable.entrySet()) {
        TableId tableId = entry.getKey();
        BufferedRecords buffer = entry.getValue();
        log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
        buffer.flush();
        buffer.close();
      }
      connection.commit();
    } catch (SQLException | TableAlterOrCreateException e) {
      try {
        connection.rollback();
      } catch (SQLException sqle) {
        e.addSuppressed(sqle);
      } finally {
        throw e;
      }
    }
  }

  /*
  NOtes:
  1. the moment we encounter a begin record, we commit the flushed transaction
  2. Keep flushing till we see commit
  3.
   */
  void writeConsistently(final Collection<SinkRecord> records)
      throws SQLException, TableAlterOrCreateException {
    final Connection connection = cachedConnectionProvider.getConnection();
    try {
      final Map<TableId, BufferedRecords> bufferedRecords = new HashMap<>();
      for (SinkRecord record : records) {
        Struct s = (Struct) record.value();
        boolean isTxnRecord = s.getStruct("payload").schema().fields().stream()
                               .map(f -> f.name()).collect(Collectors.toSet()).contains("status");
        if (isTxnRecord) {
          if (s.getStruct("payload").getString("status").equals("BEGIN")) {
            // Do nothing, indicate a connection start.
            log.info("Encountered a begin record in sink");
          } else {
            for (Map.Entry<TableId, BufferedRecords> entry : bufferedRecords.entrySet()) {
              TableId tableId = entry.getKey();
              BufferedRecords buffer = entry.getValue();
//              buffer.flush();
              buffer.close();
            }
            connection.commit();
          }
        } else {
          final TableId tableId = destinationTable(record.topic());
          BufferedRecords buffer = bufferedRecords.get(tableId);
          if (buffer == null) {
            buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
            bufferedRecords.put(tableId, buffer);
          }

          buffer.add(record);
          buffer.flush();
        }
      }
    } catch (SQLException | TableAlterOrCreateException e) {
      try {
        connection.rollback();
      } catch (SQLException sqle) {
        e.addSuppressed(sqle);
      } finally {
        throw e;
      }
    }
  }

  /**
   * Write the records to the target database only when all the commit records have been seen.
   *
   * TODO: This function requires the the record it is reading should contain the following
   * if source info present then source -> {db,schema,table}
   * if txn record, then payload -> {db,schema,table}
   * @param records
   */
  void writeConsistentlyWithTxnRecords(final Collection<SinkRecord> records) {
    final Connection connection = cachedConnectionProvider.getConnection();
    try {
      final Map<String, Stack<SinkRecord>> txnMetadata = new HashMap<>();
      final Map<TableId, BufferedRecords> bufferedRecords = new HashMap<>();
      for (SinkRecord record : records) {
        // If we receive a begin-commit message then check and add the stack
        Struct s = (Struct) record.value();
        if (s.getStruct("payload").getString("status").equals("BEGIN")) { // For begin
          // Start a transaction.
        } else if (s.getStruct("payload").getString("status").equals("END")) { // For commit
//          Struct payload = s.getStruct("payload");
//          final String fullTableName =
//            String.format("%s.%s.%s", payload.getString("db"), payload.getString("schema"),
//                          payload.getString("table"));
//          Stack<SinkRecord> recordStack = txnMetadata.get(fullTableName);
//          if (recordStack == null) {
//            throw new IllegalStateException("Commit record encountered without a preceding begin");
//          }
//          recordStack.pop();
//          if (recordStack.isEmpty()) {
//            final TableId tableId = new TableId(payload.getString("db"),
//                                                payload.getString("schema"),
//                                                payload.getString("table"));
//            BufferedRecords buffer = bufferedRecords.get(tableId);
//            buffer.flush();
//            buffer.close();
//            connection.commit();
//          }

          // commit the transaction
          connection.commit();
        } else {
          final TableId tableId = destinationTable(record.topic());

          Struct source = s.getStruct("source");
          final String fullTableName =
            String.format("%s.%s.%s", source.getString("db"), source.getString("schema"),
              source.getString("table"));

          BufferedRecords buffer = bufferedRecords.get(tableId);

          if(txnMetadata.get(fullTableName) == null || txnMetadata.get(fullTableName).isEmpty()) {
            // This indicates that there are no begin-commit messages associated yet, the current
            // record is not a part of any transaction.
            // Flush the records.
            buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
            buffer.add(record);
            buffer.flush();
            buffer.close();
            connection.commit();
          } else {
            if (buffer == null) {
              buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
              bufferedRecords.put(tableId, buffer);
            }

            buffer.add(record);
            buffer.flush();
          }
        }
      }
    } catch (SQLException e) {
      try {
        connection.rollback();
      } catch (SQLException sqle) {
        e.addSuppressed(sqle);
      } finally {
        throw e;
      }
    }
  }

  void closeQuietly() {
    cachedConnectionProvider.close();
  }

  TableId destinationTable(String topic) {
    final String tableName = config.tableNameFormat.replace("${topic}", topic);
    if (tableName.isEmpty()) {
      throw new ConnectException(String.format(
          "Destination table name for topic '%s' is empty using the format string '%s'",
          topic,
          config.tableNameFormat
      ));
    }
    return dbDialect.parseTableIdentifier(tableName);
  }
}
